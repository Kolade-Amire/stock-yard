from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, ValidationError

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.providers.llm.base import LLMMessage, LLMModelResponse, LLMProvider, ToolCall, ToolSpec
from app.schemas.chat import ChatRequest, ChatResponse, ChatTurn
from app.schemas.ticker import PriceBar
from app.services.yfinance_service import YFinanceService
from app.utils.symbols import is_valid_symbol, normalize_symbol

DEFAULT_HISTORY_PERIOD = "6mo"
DEFAULT_HISTORY_INTERVAL = "1d"
DEFAULT_MAX_TOOL_CALL_ROUNDS = 2
DEFAULT_HISTORY_RECENT_BARS_LIMIT = 12
DEFAULT_NEWS_TOOL_LIMIT = 3
DEFAULT_CHAT_SESSION_TTL_SECONDS = 1800
DEFAULT_CHAT_SESSION_MAX_TOOL_ENTRIES = 16
MAX_NEWS_TOOL_LIMIT = 10
MAX_CHAT_SESSION_ID_LENGTH = 128
TOOL_GATING_MODE_BALANCED = "balanced"

ALLOWED_HISTORY_PERIODS = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
ALLOWED_HISTORY_INTERVALS = (
    "1m",
    "2m",
    "5m",
    "15m",
    "30m",
    "60m",
    "90m",
    "1h",
    "1d",
    "5d",
    "1wk",
    "1mo",
    "3mo",
)

OUTLOOK_FALLBACK_TOOL_NAMES = (
    "get_stock_snapshot",
    "get_price_history",
    "get_news_context",
)
OUTLOOK_FALLBACK_TOOL_NAME_SET = frozenset(OUTLOOK_FALLBACK_TOOL_NAMES)
COMPACT_STOCK_SNAPSHOT_FIELDS = (
    "display_name",
    "quote_type",
    "exchange",
    "currency",
    "sector",
    "industry",
    "current_price",
    "previous_close",
    "day_low",
    "day_high",
    "fifty_two_week_low",
    "fifty_two_week_high",
    "market_cap",
    "trailing_pe",
    "forward_pe",
    "dividend_yield",
    "beta",
    "analyst_target_mean",
    "earnings_date",
    "is_etf",
)
SPECIFIC_INTENT_NAMES = ("price", "news", "financials", "earnings", "analyst", "ownership")
CONTEXT_INTENT_PRIORITY = ("earnings", "analyst", "financials", "ownership", "news", "price")
NORMALIZE_PUNCTUATION_RE = re.compile(r"[^a-z0-9/\-\s]+")
NORMALIZE_WHITESPACE_RE = re.compile(r"\s+")
MAX_FINANCIAL_TREND_ANNUAL_POINTS = 4
MAX_FINANCIAL_TREND_QUARTERLY_POINTS = 6
MAX_EARNINGS_DEEP_EVENTS = 4
MAX_EARNINGS_DEEP_ESTIMATE_POINTS = 4
MAX_ANALYST_DEEP_ACTIONS = 5
MAX_ANALYST_DEEP_HISTORY_POINTS = 4
MAX_OWNERSHIP_CONTEXT_ROWS = 5
DEFAULT_MEMO_TTL_BY_TOOL: dict[str, int] = {
    "get_stock_snapshot": 300,
    "get_price_history": 300,
    "get_news_context": 900,
    "get_financial_summary": 3600,
    "get_financial_trends_context": 3600,
    "get_ownership_context": 3600,
    "get_earnings_deep_context": 3600,
    "get_analyst_deep_context": 3600,
}


@dataclass(frozen=True)
class IntentDefinition:
    strong_phrases: tuple[str, ...]
    strong_terms: tuple[str, ...]
    weak_terms: tuple[str, ...]
    tools: tuple[str, ...]


@dataclass(frozen=True)
class MatchInput:
    normalized_text: str
    phrase_text: str
    tokens: frozenset[str]


@dataclass(frozen=True)
class MemoizedToolEntry:
    tool_name: str
    tool_key: str
    payload: dict[str, Any]
    limitations: list[str]
    summary: str
    cached_at: float
    expires_at: float


@dataclass(frozen=True)
class ToolExecutionResult:
    payload: dict[str, Any]
    limitations: list[str]
    memo_hit: bool


@dataclass
class ChatSessionState:
    symbol: str
    expires_at: float
    tool_entries: dict[str, MemoizedToolEntry]


class ChatMemoMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._requests_total = 0
        self._requests_with_cached_context = 0
        self._requests_cached_context_satisfied = 0
        self._memo_hits_total = 0
        self._cold_misses_total = 0
        self._per_tool: dict[str, dict[str, int]] = {}

    def record_request(
        self,
        *,
        cached_context_tool_names: list[str],
        cached_context_satisfied: bool,
        memo_hit_tool_names: list[str],
        cold_miss_tool_names: list[str],
    ) -> dict[str, Any]:
        with self._lock:
            self._requests_total += 1
            if cached_context_tool_names:
                self._requests_with_cached_context += 1
            if cached_context_satisfied:
                self._requests_cached_context_satisfied += 1

            for tool_name in cached_context_tool_names:
                counts = self._per_tool_counts_locked(tool_name)
                counts["cachedContextAvailable"] += 1
                if cached_context_satisfied:
                    counts["cachedContextSatisfied"] += 1

            for tool_name in memo_hit_tool_names:
                self._memo_hits_total += 1
                counts = self._per_tool_counts_locked(tool_name)
                counts["memoHits"] += 1

            for tool_name in cold_miss_tool_names:
                self._cold_misses_total += 1
                counts = self._per_tool_counts_locked(tool_name)
                counts["coldMisses"] += 1

            return {
                "requests": self._requests_total,
                "cachedContextAvailableRequests": self._requests_with_cached_context,
                "cachedContextSatisfiedRequests": self._requests_cached_context_satisfied,
                "memoHits": self._memo_hits_total,
                "coldMisses": self._cold_misses_total,
                "perTool": {
                    tool_name: dict(counts)
                    for tool_name, counts in sorted(self._per_tool.items())
                },
            }

    def _per_tool_counts_locked(self, tool_name: str) -> dict[str, int]:
        counts = self._per_tool.get(tool_name)
        if counts is None:
            counts = {
                "cachedContextAvailable": 0,
                "cachedContextSatisfied": 0,
                "memoHits": 0,
                "coldMisses": 0,
            }
            self._per_tool[tool_name] = counts
        return counts


class ChatSessionStore:
    def __init__(self, *, session_ttl_seconds: int, max_tool_entries: int) -> None:
        self._session_ttl_seconds = max(1, session_ttl_seconds)
        self._max_tool_entries = max(1, max_tool_entries)
        self._sessions: dict[str, ChatSessionState] = {}
        self._lock = Lock()

    def resolve_session(self, requested_session_id: str | None, symbol: str) -> str:
        now = time.monotonic()
        with self._lock:
            self._evict_expired_sessions_locked(now)
            if requested_session_id:
                existing = self._sessions.get(requested_session_id)
                if existing is not None and existing.symbol == symbol:
                    existing.expires_at = now + self._session_ttl_seconds
                    return requested_session_id

            session_id = uuid4().hex
            self._sessions[session_id] = ChatSessionState(
                symbol=symbol,
                expires_at=now + self._session_ttl_seconds,
                tool_entries={},
            )
            return session_id

    def get_entry(
        self,
        *,
        session_id: str,
        symbol: str,
        tool_key: str,
    ) -> MemoizedToolEntry | None:
        now = time.monotonic()
        with self._lock:
            state = self._get_active_session_locked(session_id=session_id, symbol=symbol, now=now)
            if state is None:
                return None
            entry = state.tool_entries.get(tool_key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                state.tool_entries.pop(tool_key, None)
                return None
            state.expires_at = now + self._session_ttl_seconds
            return entry

    def set_entry(
        self,
        *,
        session_id: str,
        symbol: str,
        entry: MemoizedToolEntry,
    ) -> None:
        now = time.monotonic()
        with self._lock:
            state = self._get_active_session_locked(session_id=session_id, symbol=symbol, now=now)
            if state is None:
                state = ChatSessionState(
                    symbol=symbol,
                    expires_at=now + self._session_ttl_seconds,
                    tool_entries={},
                )
                self._sessions[session_id] = state

            state.expires_at = now + self._session_ttl_seconds
            state.tool_entries.pop(entry.tool_key, None)
            state.tool_entries[entry.tool_key] = entry

            while len(state.tool_entries) > self._max_tool_entries:
                oldest_key = next(iter(state.tool_entries))
                state.tool_entries.pop(oldest_key, None)

    def get_context_entries(
        self,
        *,
        session_id: str,
        symbol: str,
        tool_names: list[str],
    ) -> list[MemoizedToolEntry]:
        now = time.monotonic()
        with self._lock:
            state = self._get_active_session_locked(session_id=session_id, symbol=symbol, now=now)
            if state is None:
                return []

            freshest_by_tool: dict[str, MemoizedToolEntry] = {}
            allowed_tool_names = set(tool_names)
            stale_keys: list[str] = []
            for tool_key, entry in state.tool_entries.items():
                if entry.expires_at <= now:
                    stale_keys.append(tool_key)
                    continue
                if entry.tool_name not in allowed_tool_names:
                    continue
                current = freshest_by_tool.get(entry.tool_name)
                if current is None or entry.cached_at > current.cached_at:
                    freshest_by_tool[entry.tool_name] = entry

            for stale_key in stale_keys:
                state.tool_entries.pop(stale_key, None)

            state.expires_at = now + self._session_ttl_seconds
            ordered_entries: list[MemoizedToolEntry] = []
            for tool_name in tool_names:
                entry = freshest_by_tool.get(tool_name)
                if entry is not None:
                    ordered_entries.append(entry)
            return ordered_entries

    def _get_active_session_locked(
        self,
        *,
        session_id: str,
        symbol: str,
        now: float,
    ) -> ChatSessionState | None:
        self._evict_expired_sessions_locked(now)
        state = self._sessions.get(session_id)
        if state is None:
            return None
        if state.symbol != symbol:
            return None
        if state.expires_at <= now:
            self._sessions.pop(session_id, None)
            return None
        return state

    def _evict_expired_sessions_locked(self, now: float) -> None:
        expired_session_ids = [
            session_id
            for session_id, state in self._sessions.items()
            if state.expires_at <= now
        ]
        for session_id in expired_session_ids:
            self._sessions.pop(session_id, None)


INTENT_CATALOG: dict[str, IntentDefinition] = {
    "price": IntentDefinition(
        strong_phrases=(
            "price action",
            "how has it moved",
            "stock move",
            "recent move",
            "trading range",
            "price trend",
            "52 week",
        ),
        strong_terms=(
            "price",
            "chart",
            "history",
            "trend",
            "performance",
            "volatility",
            "drawdown",
            "bounce",
            "selloff",
            "rally",
        ),
        weak_terms=(
            "moved",
            "move",
            "moving",
            "up",
            "down",
            "range",
            "support",
            "resistance",
            "momentum",
        ),
        tools=("get_price_history", "get_stock_snapshot"),
    ),
    "news": IntentDefinition(
        strong_phrases=(
            "what happened",
            "what moved the stock",
            "latest developments",
            "news flow",
            "latest update",
            "recent news",
            "why is it up",
            "why is it down",
        ),
        strong_terms=(
            "news",
            "headline",
            "headlines",
            "article",
            "articles",
            "catalyst",
            "catalysts",
            "headwind",
            "tailwind",
            "event",
        ),
        weak_terms=("today", "recent", "development", "update", "story"),
        tools=("get_news_context",),
    ),
    "financials": IntentDefinition(
        strong_phrases=(
            "balance sheet",
            "income statement",
            "cash flow",
            "free cash flow",
        ),
        strong_terms=(
            "financial",
            "fundamental",
            "fundamentals",
            "profitability",
            "leverage",
            "liquidity",
            "multiples",
            "valuation",
            "revenue",
            "debt",
            "cash",
            "cashflow",
            "margin",
            "margins",
        ),
        weak_terms=("profit", "profits", "cashflow", "multiple", "p/e", "pe"),
        tools=("get_financial_summary", "get_financial_trends_context", "get_stock_snapshot"),
    ),
    "earnings": IntentDefinition(
        strong_phrases=(
            "next report",
            "earnings date",
            "eps expectations",
            "quarterly results",
            "earnings call",
        ),
        strong_terms=(
            "earnings",
            "guidance",
            "beat",
            "miss",
            "eps",
            "quarter",
            "report",
        ),
        weak_terms=("estimate", "estimates", "reported", "results"),
        tools=("get_earnings_deep_context",),
    ),
    "analyst": IntentDefinition(
        strong_phrases=(
            "wall street",
            "street view",
            "street sentiment",
            "price targets",
            "price target",
        ),
        strong_terms=(
            "analyst",
            "analysts",
            "consensus",
            "coverage",
            "revisions",
            "upgrades",
            "downgrades",
        ),
        weak_terms=("target", "targets", "rating", "ratings", "upgrade", "downgrade"),
        tools=("get_analyst_deep_context",),
    ),
    "ownership": IntentDefinition(
        strong_phrases=(
            "major holders",
            "mutual fund holders",
            "institutional holders",
            "insider roster",
        ),
        strong_terms=(
            "ownership",
            "holders",
            "holder",
            "institutional",
            "institutions",
            "owns",
            "owned",
            "exposure",
            "insider",
            "insiders",
        ),
        weak_terms=("mutual", "fund", "owned", "ownership", "holding", "holdings"),
        tools=("get_ownership_context",),
    ),
    "general_outlook": IntentDefinition(
        strong_phrases=(
            "bull case",
            "bear case",
            "top risks",
            "top risk",
            "what to watch",
            "quick take",
            "near term",
        ),
        strong_terms=("outlook", "summary", "summarize", "bullish", "bearish"),
        weak_terms=("risk", "risks", "watch", "view", "take", "outlook", "near-term"),
        tools=OUTLOOK_FALLBACK_TOOL_NAMES,
    ),
}

CHAT_SYSTEM_INSTRUCTION = """
You are a stock analysis assistant for one active ticker symbol.
You must use tool results as the source of truth and remain grounded to the active symbol only.
Never invent numbers, events, news, or metrics.
If data is unavailable or uncertain, state that clearly.
Use prior conversation turns to stay conversational and context-aware.
Do not provide personalized investment advice or direct buy/sell instructions.
Return only valid JSON that matches the required response schema.
""".strip()

CHAT_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "highlights": {
            "type": "array",
            "items": {"type": "string"},
        },
        "limitations": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": ["answer", "highlights", "limitations"],
    "additionalProperties": False,
}


class StructuredChatAnswer(BaseModel):
    answer: str = Field(min_length=1)
    highlights: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


class ChatService:
    def __init__(
        self,
        *,
        yfinance_service: YFinanceService,
        llm_provider: LLMProvider,
        max_turns: int = 6,
        max_tool_call_rounds: int = DEFAULT_MAX_TOOL_CALL_ROUNDS,
        history_recent_bars_limit: int = DEFAULT_HISTORY_RECENT_BARS_LIMIT,
        news_tool_default_limit: int = DEFAULT_NEWS_TOOL_LIMIT,
        session_ttl_seconds: int = DEFAULT_CHAT_SESSION_TTL_SECONDS,
        session_max_tool_entries: int = DEFAULT_CHAT_SESSION_MAX_TOOL_ENTRIES,
        memo_ttl_overview_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_stock_snapshot"],
        memo_ttl_history_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_price_history"],
        memo_ttl_news_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_news_context"],
        memo_ttl_financials_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_financial_summary"],
        memo_ttl_earnings_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_earnings_deep_context"],
        memo_ttl_analyst_seconds: int = DEFAULT_MEMO_TTL_BY_TOOL["get_analyst_deep_context"],
        tool_gating_mode: str = TOOL_GATING_MODE_BALANCED,
    ) -> None:
        self._yfinance_service = yfinance_service
        self._llm_provider = llm_provider
        self._max_turns = max(1, max_turns)
        self._max_tool_call_rounds = max(1, max_tool_call_rounds)
        self._history_recent_bars_limit = max(1, history_recent_bars_limit)
        self._news_tool_default_limit = max(1, min(news_tool_default_limit, MAX_NEWS_TOOL_LIMIT))
        self._tool_gating_mode = tool_gating_mode.strip().lower()
        self._logger = get_logger(__name__)
        self._session_store = ChatSessionStore(
            session_ttl_seconds=session_ttl_seconds,
            max_tool_entries=session_max_tool_entries,
        )
        self._memo_metrics = ChatMemoMetrics()
        self._memo_ttl_by_tool = {
            "get_stock_snapshot": max(1, memo_ttl_overview_seconds),
            "get_price_history": max(1, memo_ttl_history_seconds),
            "get_news_context": max(1, memo_ttl_news_seconds),
            "get_financial_summary": max(1, memo_ttl_financials_seconds),
            "get_financial_trends_context": max(1, memo_ttl_financials_seconds),
            "get_ownership_context": max(1, memo_ttl_financials_seconds),
            "get_earnings_deep_context": max(1, memo_ttl_earnings_seconds),
            "get_analyst_deep_context": max(1, memo_ttl_analyst_seconds),
        }

        if self._tool_gating_mode != TOOL_GATING_MODE_BALANCED:
            raise ValueError("Only the 'balanced' chat tool gating mode is supported.")

    async def chat(self, payload: ChatRequest) -> ChatResponse:
        symbol = self._normalize_and_validate_symbol(payload.symbol)
        requested_session_id = self._normalize_session_id(payload.sessionId)
        clipped_turns = self._clip_conversation(payload.conversation)
        messages = self._build_messages(
            clipped_turns=clipped_turns,
            current_message=payload.message,
        )
        selected_tools, matched_intents, context_extra_intent = self._select_tool_specs(
            current_message=payload.message,
            clipped_turns=clipped_turns,
        )
        session_id = self._session_store.resolve_session(requested_session_id, symbol)
        collected_limitations: list[str] = []
        used_tools: list[str] = []
        selected_tool_names = [tool.name for tool in selected_tools]
        cached_context_entries = self._session_store.get_context_entries(
            session_id=session_id,
            symbol=symbol,
            tool_names=selected_tool_names,
        )
        cached_context = self._build_cached_context(
            symbol=symbol,
            entries=cached_context_entries,
        )
        cached_context_tool_names = [entry.tool_name for entry in cached_context_entries]
        system_instruction = self._build_system_instruction(
            symbol=symbol,
            cached_context=cached_context,
        )
        tool_call_count = 0
        memo_hit_count = 0
        memo_hit_tool_names: list[str] = []
        cold_miss_tool_names: list[str] = []

        self._logger.info(
            "Chat tool selection symbol=%s sessionId=%s turns=%d intents=%s contextExtra=%s "
            "tools=%s cachedContextEntries=%d cachedContextTools=%s",
            symbol,
            session_id,
            len(clipped_turns),
            matched_intents,
            context_extra_intent,
            selected_tool_names,
            len(cached_context_entries),
            cached_context_tool_names,
        )

        for tool_round in range(self._max_tool_call_rounds + 1):
            model_response = await self._llm_provider.generate(
                system_instruction=system_instruction,
                messages=messages,
                tools=selected_tools,
                response_schema=CHAT_RESPONSE_SCHEMA,
            )

            if model_response.tool_calls:
                if tool_round >= self._max_tool_call_rounds:
                    raise ApiError(
                        code="LLM_ERROR",
                        message="Model exceeded tool-call limit.",
                        status_code=502,
                    )

                tool_call_count += len(model_response.tool_calls)
                messages.append(
                    LLMMessage(role="assistant", content="", tool_calls=model_response.tool_calls)
                )
                for tool_call in model_response.tool_calls:
                    used_tools.append(tool_call.name)
                    tool_result = await self._execute_tool(
                        active_symbol=symbol,
                        session_id=session_id,
                        tool_call=tool_call,
                    )
                    if tool_result.memo_hit:
                        memo_hit_count += 1
                        memo_hit_tool_names.append(tool_call.name)
                    else:
                        cold_miss_tool_names.append(tool_call.name)
                    collected_limitations.extend(tool_result.limitations)
                    messages.append(
                        LLMMessage(
                            role="tool",
                            content=json.dumps(tool_result.payload, default=str),
                            name=tool_call.name,
                            tool_call_id=tool_call.id,
                        )
                    )
                continue

            first_round_completed_without_tools = tool_round == 0
            structured_answer = self._parse_structured_answer(model_response)
            merged_limitations = self._dedupe_preserve_order(
                [*structured_answer.limitations, *collected_limitations]
            )
            deduped_used_tools = self._dedupe_preserve_order(used_tools)
            memo_metrics_snapshot = self._memo_metrics.record_request(
                cached_context_tool_names=cached_context_tool_names,
                cached_context_satisfied=bool(cached_context_entries)
                and first_round_completed_without_tools,
                memo_hit_tool_names=memo_hit_tool_names,
                cold_miss_tool_names=cold_miss_tool_names,
            )
            self._log_chat_request_summary(
                symbol=symbol,
                session_id=session_id,
                selected_tool_names=selected_tool_names,
                cached_context_tool_names=cached_context_tool_names,
                tool_call_count=tool_call_count,
                memo_hit_count=memo_hit_count,
                used_tools=deduped_used_tools,
                limitation_count=len(merged_limitations),
                cached_context_satisfied=bool(cached_context_entries)
                and first_round_completed_without_tools,
                memo_metrics_snapshot=memo_metrics_snapshot,
            )
            return ChatResponse(
                symbol=symbol,
                sessionId=session_id,
                answer=structured_answer.answer,
                highlights=structured_answer.highlights,
                usedTools=deduped_used_tools,
                limitations=merged_limitations,
            )

        raise ApiError(
            code="LLM_ERROR",
            message="Chat orchestration failed to produce a final response.",
            status_code=502,
        )

    def _clip_conversation(self, conversation: list[ChatTurn]) -> list[ChatTurn]:
        return conversation[-self._max_turns :]

    @staticmethod
    def _build_messages(
        *,
        clipped_turns: list[ChatTurn],
        current_message: str,
    ) -> list[LLMMessage]:
        messages: list[LLMMessage] = []
        for turn in clipped_turns:
            stripped_content = turn.content.strip()
            if not stripped_content:
                continue
            messages.append(LLMMessage(role=turn.role, content=stripped_content))

        stripped_current_message = current_message.strip()
        if not stripped_current_message:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Chat message cannot be empty.",
                status_code=400,
            )

        messages.append(LLMMessage(role="user", content=stripped_current_message))
        return messages

    @staticmethod
    def _build_system_instruction(symbol: str, cached_context: str | None = None) -> str:
        instruction = f"{CHAT_SYSTEM_INSTRUCTION}\nActive ticker symbol: {symbol}"
        if cached_context:
            instruction = f"{instruction}\n\n{cached_context}"
        return instruction

    def _build_cached_context(
        self,
        *,
        symbol: str,
        entries: list[MemoizedToolEntry],
    ) -> str | None:
        if not entries:
            return None

        lines = [
            "Previously grounded cached context is available for this same chat session.",
            "Reuse it when it is sufficient instead of calling tools again.",
            f"Cached ticker: {symbol}",
        ]
        for entry in entries:
            line = f"- {entry.tool_name}: {entry.summary}"
            compact_limitations = self._compact_limitations(entry.limitations)
            if compact_limitations:
                serialized_limitations = json.dumps(compact_limitations, separators=(",", ":"))
                line = f"{line}; limitations={serialized_limitations}"
            lines.append(line)
        return "\n".join(lines)

    def _log_chat_request_summary(
        self,
        *,
        symbol: str,
        session_id: str,
        selected_tool_names: list[str],
        cached_context_tool_names: list[str],
        tool_call_count: int,
        memo_hit_count: int,
        used_tools: list[str],
        limitation_count: int,
        cached_context_satisfied: bool,
        memo_metrics_snapshot: dict[str, Any],
    ) -> None:
        self._logger.info(
            "Chat request summary symbol=%s sessionId=%s selectedTools=%s "
            "cachedContextTools=%s toolCalls=%d memoHits=%d memoMisses=%d "
            "cachedContextSatisfied=%s usedTools=%s limitations=%d",
            symbol,
            session_id,
            selected_tool_names,
            cached_context_tool_names,
            tool_call_count,
            memo_hit_count,
            max(0, tool_call_count - memo_hit_count),
            cached_context_satisfied,
            used_tools,
            limitation_count,
        )
        self._logger.info(
            "Chat memo metrics requests=%d cachedContextAvailableRequests=%d "
            "cachedContextSatisfiedRequests=%d memoHits=%d coldMisses=%d perTool=%s",
            memo_metrics_snapshot["requests"],
            memo_metrics_snapshot["cachedContextAvailableRequests"],
            memo_metrics_snapshot["cachedContextSatisfiedRequests"],
            memo_metrics_snapshot["memoHits"],
            memo_metrics_snapshot["coldMisses"],
            json.dumps(memo_metrics_snapshot["perTool"], sort_keys=True, separators=(",", ":")),
        )

    @staticmethod
    def _normalize_session_id(session_id: str | None) -> str | None:
        if session_id is None:
            return None
        normalized_session_id = session_id.strip()
        if not normalized_session_id:
            return None
        if len(normalized_session_id) > MAX_CHAT_SESSION_ID_LENGTH:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Session ID is too long.",
                status_code=400,
                details={"maxLength": MAX_CHAT_SESSION_ID_LENGTH},
            )
        return normalized_session_id

    def _normalize_tool_arguments_for_cache(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> dict[str, Any]:
        if tool_name == "get_price_history":
            period = self._coerce_str(arguments.get("period")) or DEFAULT_HISTORY_PERIOD
            interval = self._coerce_str(arguments.get("interval")) or DEFAULT_HISTORY_INTERVAL
            return {
                "period": period,
                "interval": interval,
            }
        if tool_name == "get_news_context":
            raw_limit = self._coerce_int(arguments.get("limit"))
            limit = (
                self._news_tool_default_limit
                if raw_limit is None
                else max(1, min(raw_limit, MAX_NEWS_TOOL_LIMIT))
            )
            return {"limit": limit}
        return {}

    @staticmethod
    def _build_tool_cache_key(tool_name: str, normalized_arguments: dict[str, Any]) -> str:
        serialized_arguments = json.dumps(normalized_arguments, sort_keys=True, separators=(",", ":"))
        return f"{tool_name}:{serialized_arguments}"

    def _summarize_tool_payload(
        self,
        *,
        tool_name: str,
        payload: dict[str, Any],
        limitations: list[str],
        normalized_arguments: dict[str, Any],
    ) -> str:
        summary_data = self._summary_data_for_tool(
            tool_name=tool_name,
            payload=payload,
            normalized_arguments=normalized_arguments,
        )
        if limitations:
            summary_data["limitationCount"] = len(limitations)
        return json.dumps(summary_data, separators=(",", ":"), default=str)

    def _summary_data_for_tool(
        self,
        *,
        tool_name: str,
        payload: dict[str, Any],
        normalized_arguments: dict[str, Any],
    ) -> dict[str, Any]:
        if tool_name == "get_stock_snapshot":
            overview = payload.get("overview", {})
            return self._drop_empty_summary_values(
                {
                    "displayName": overview.get("display_name"),
                    "currentPrice": overview.get("current_price"),
                    "marketCap": overview.get("market_cap"),
                    "sector": overview.get("sector"),
                    "industry": overview.get("industry"),
                    "earningsDate": overview.get("earnings_date"),
                }
            )
        if tool_name == "get_price_history":
            summary = payload.get("summary", {})
            return self._drop_empty_summary_values(
                {
                    "period": normalized_arguments.get("period"),
                    "interval": normalized_arguments.get("interval"),
                    "lastClose": summary.get("lastClose"),
                    "percentChange": summary.get("percentChange"),
                    "periodHigh": summary.get("periodHigh"),
                    "periodLow": summary.get("periodLow"),
                    "barCount": summary.get("barCount"),
                }
            )
        if tool_name == "get_news_context":
            top_headlines = payload.get("topHeadlines") or []
            return self._drop_empty_summary_values(
                {
                    "limit": normalized_arguments.get("limit"),
                    "itemCount": payload.get("itemCount"),
                    "topHeadlines": top_headlines[:2],
                }
            )
        if tool_name == "get_financial_summary":
            financial_summary = payload.get("financialSummary", {})
            return self._drop_empty_summary_values(
                {
                    "revenueTTM": financial_summary.get("revenue_ttm"),
                    "netIncomeTTM": financial_summary.get("net_income_ttm"),
                    "freeCashFlow": financial_summary.get("free_cash_flow"),
                    "grossMargins": financial_summary.get("gross_margins"),
                    "operatingMargins": financial_summary.get("operating_margins"),
                    "debtToEquity": financial_summary.get("debt_to_equity"),
                }
            )
        if tool_name == "get_financial_trends_context":
            annual_summary = payload.get("annualSummary", {})
            quarterly_summary = payload.get("quarterlySummary", {})
            return self._drop_empty_summary_values(
                {
                    "annualLatestPeriodEnd": annual_summary.get("latestPeriodEnd"),
                    "annualRevenueDelta": annual_summary.get("revenueDelta"),
                    "annualNetIncomeDelta": annual_summary.get("netIncomeDelta"),
                    "annualFreeCashFlowDelta": annual_summary.get("freeCashFlowDelta"),
                    "quarterlyLatestPeriodEnd": quarterly_summary.get("latestPeriodEnd"),
                    "quarterlyRevenueDelta": quarterly_summary.get("revenueDelta"),
                    "quarterlyNetIncomeDelta": quarterly_summary.get("netIncomeDelta"),
                    "quarterlyFreeCashFlowDelta": quarterly_summary.get("freeCashFlowDelta"),
                }
            )
        if tool_name == "get_earnings_deep_context":
            recent_surprises = payload.get("recentSurprises") or []
            eps_estimates = payload.get("epsEstimates") or []
            return self._drop_empty_summary_values(
                {
                    "nextEarningsDate": payload.get("nextEarningsDate"),
                    "recentSurprises": [
                        self._drop_empty_summary_values(
                            {
                                "quarter": event.get("quarter"),
                                "surprisePercent": event.get("surprisePercent"),
                            }
                        )
                        for event in recent_surprises[:2]
                    ],
                    "epsEstimatePeriods": [
                        self._drop_empty_summary_values(
                            {
                                "period": point.get("period"),
                                "avg": point.get("avg"),
                                "growth": point.get("growth"),
                            }
                        )
                        for point in eps_estimates[:2]
                    ],
                }
            )
        if tool_name == "get_analyst_deep_context":
            recommendation_history = payload.get("recommendationHistory") or []
            action_timeline = payload.get("actionTimeline") or []
            return self._drop_empty_summary_values(
                {
                    "targetMean": payload.get("currentTargets", {}).get("targetMean"),
                    "targetLow": payload.get("currentTargets", {}).get("targetLow"),
                    "targetHigh": payload.get("currentTargets", {}).get("targetHigh"),
                    "recentActionCount": payload.get("recentActionCount"),
                    "recentActionWindowDays": payload.get("recentActionWindowDays"),
                    "latestRecommendation": recommendation_history[-1] if recommendation_history else None,
                    "latestAction": action_timeline[0] if action_timeline else None,
                }
            )
        if tool_name == "get_ownership_context":
            major_holders = payload.get("majorHolders") or []
            institutional_holders = payload.get("institutionalHolders") or []
            mutual_fund_holders = payload.get("mutualFundHolders") or []
            insider_roster = payload.get("insiderRoster") or []
            return self._drop_empty_summary_values(
                {
                    "majorHolders": [
                        self._drop_empty_summary_values(
                            {
                                "label": item.get("label"),
                                "value": item.get("value"),
                            }
                        )
                        for item in major_holders[:2]
                    ],
                    "topInstitutionalHolders": [
                        holder.get("holder")
                        for holder in institutional_holders[:2]
                        if holder.get("holder")
                    ],
                    "topMutualFundHolders": [
                        holder.get("holder")
                        for holder in mutual_fund_holders[:2]
                        if holder.get("holder")
                    ],
                    "topInsiders": [
                        insider.get("name")
                        for insider in insider_roster[:2]
                        if insider.get("name")
                    ],
                }
            )
        return self._drop_empty_summary_values({"tool": tool_name})

    @staticmethod
    def _drop_empty_summary_values(value: Any) -> Any:
        if isinstance(value, dict):
            cleaned_dict = {
                key: ChatService._drop_empty_summary_values(item)
                for key, item in value.items()
                if item is not None
            }
            return {
                key: item
                for key, item in cleaned_dict.items()
                if item not in ({}, [], "", None)
            }
        if isinstance(value, list):
            cleaned_list = [
                ChatService._drop_empty_summary_values(item)
                for item in value
                if item is not None
            ]
            return [item for item in cleaned_list if item not in ({}, [], "", None)]
        return value

    @staticmethod
    def _compact_limitations(limitations: list[str]) -> list[str]:
        return [limitation for limitation in limitations[:2] if limitation]

    def _select_tool_specs(
        self,
        *,
        current_message: str,
        clipped_turns: list[ChatTurn],
    ) -> tuple[list[ToolSpec], list[str], str | None]:
        current_match = self._normalize_match_input(current_message)
        current_intents = self._match_intents(current_match, include_weak_terms=True)
        current_specific_intents = self._specific_intents(current_intents)

        selected_intents = current_specific_intents
        context_extra_intent: str | None = None

        if not selected_intents:
            selected_intents = ["general_outlook"]

            previous_user_text = self._last_turn_content(clipped_turns, role="user")
            previous_user_intents = self._specific_intents(
                self._match_intents(
                    self._normalize_match_input(previous_user_text),
                    include_weak_terms=True,
                )
            )
            context_extra_intent = self._highest_priority_intent(previous_user_intents)

            if context_extra_intent is None:
                previous_assistant_text = self._last_turn_content(clipped_turns, role="assistant")
                previous_assistant_intents = self._specific_intents(
                    self._match_intents(
                        self._normalize_match_input(previous_assistant_text),
                        include_weak_terms=False,
                    )
                )
                context_extra_intent = self._highest_priority_intent(previous_assistant_intents)

            if context_extra_intent is not None:
                selected_intents.append(context_extra_intent)

        tool_specs_by_name = {tool.name: tool for tool in self._all_tool_specs()}
        selected_tool_names = self._tool_names_for_intents(selected_intents)
        selected_tools = [
            tool_specs_by_name[name]
            for name in selected_tool_names
            if name in tool_specs_by_name
        ]
        return selected_tools, selected_intents, context_extra_intent

    @staticmethod
    def _normalize_match_input(text: str) -> MatchInput:
        normalized_text = NORMALIZE_PUNCTUATION_RE.sub(" ", text.lower())
        normalized_text = NORMALIZE_WHITESPACE_RE.sub(" ", normalized_text).strip()
        phrase_text = normalized_text.replace("-", " ")
        token_values = set(normalized_text.split())
        token_values.update(phrase_text.split())
        return MatchInput(
            normalized_text=normalized_text,
            phrase_text=phrase_text,
            tokens=frozenset(token_values),
        )

    @staticmethod
    def _last_turn_content(clipped_turns: list[ChatTurn], *, role: str) -> str:
        for turn in reversed(clipped_turns):
            if turn.role != role:
                continue
            stripped_content = turn.content.strip()
            if stripped_content:
                return stripped_content
        return ""

    @staticmethod
    def _match_intents(match_input: MatchInput, *, include_weak_terms: bool) -> list[str]:
        matched_intents: list[str] = []
        if not match_input.normalized_text:
            return matched_intents

        for intent_name, intent_definition in INTENT_CATALOG.items():
            if ChatService._matches_intent(
                match_input=match_input,
                intent_definition=intent_definition,
                include_weak_terms=include_weak_terms,
            ):
                matched_intents.append(intent_name)
        return matched_intents

    @staticmethod
    def _matches_intent(
        *,
        match_input: MatchInput,
        intent_definition: IntentDefinition,
        include_weak_terms: bool,
    ) -> bool:
        if ChatService._contains_any_phrase(
            match_input=match_input,
            phrases=intent_definition.strong_phrases,
        ):
            return True
        if ChatService._contains_any_term(
            tokens=match_input.tokens,
            terms=intent_definition.strong_terms,
        ):
            return True
        if not include_weak_terms:
            return False
        return ChatService._contains_two_distinct_terms(
            tokens=match_input.tokens,
            terms=intent_definition.weak_terms,
        )

    @staticmethod
    def _contains_any_phrase(*, match_input: MatchInput, phrases: tuple[str, ...]) -> bool:
        padded_normalized_text = f" {match_input.normalized_text} "
        padded_phrase_text = f" {match_input.phrase_text} "
        for phrase in phrases:
            normalized_phrase = ChatService._normalize_match_input(phrase)
            phrase_candidates = (
                f" {normalized_phrase.normalized_text} ",
                f" {normalized_phrase.phrase_text} ",
            )
            if any(
                candidate in padded_normalized_text or candidate in padded_phrase_text
                for candidate in phrase_candidates
            ):
                return True
        return False

    @staticmethod
    def _contains_any_term(*, tokens: frozenset[str], terms: tuple[str, ...]) -> bool:
        for term in terms:
            normalized_term = ChatService._normalize_match_input(term)
            if normalized_term.normalized_text in tokens or normalized_term.phrase_text in tokens:
                return True
        return False

    @staticmethod
    def _contains_two_distinct_terms(*, tokens: frozenset[str], terms: tuple[str, ...]) -> bool:
        matched_terms: set[str] = set()
        for term in terms:
            normalized_term = ChatService._normalize_match_input(term)
            if normalized_term.normalized_text in tokens or normalized_term.phrase_text in tokens:
                matched_terms.add(term)
            if len(matched_terms) >= 2:
                return True
        return False

    @staticmethod
    def _specific_intents(intent_names: list[str]) -> list[str]:
        return [intent_name for intent_name in intent_names if intent_name in SPECIFIC_INTENT_NAMES]

    @staticmethod
    def _highest_priority_intent(intent_names: list[str]) -> str | None:
        intent_name_set = set(intent_names)
        for intent_name in CONTEXT_INTENT_PRIORITY:
            if intent_name in intent_name_set:
                return intent_name
        return None

    @staticmethod
    def _tool_names_for_intents(intent_names: list[str]) -> list[str]:
        selected_tool_names: list[str] = []
        for intent_name in intent_names:
            intent_definition = INTENT_CATALOG.get(intent_name)
            if intent_definition is None:
                continue
            selected_tool_names.extend(intent_definition.tools)
        return ChatService._dedupe_preserve_order(selected_tool_names)

    async def _execute_tool(
        self,
        *,
        active_symbol: str,
        session_id: str,
        tool_call: ToolCall,
    ) -> ToolExecutionResult:
        tool_name = tool_call.name
        arguments = tool_call.arguments
        normalized_arguments = self._normalize_tool_arguments_for_cache(tool_name, arguments)
        tool_key = self._build_tool_cache_key(tool_name, normalized_arguments)
        cached_entry = self._session_store.get_entry(
            session_id=session_id,
            symbol=active_symbol,
            tool_key=tool_key,
        )
        if cached_entry is not None:
            self._logger.info("Chat tool memo hit symbol=%s sessionId=%s tool=%s", active_symbol, session_id, tool_name)
            return ToolExecutionResult(
                payload=cached_entry.payload,
                limitations=cached_entry.limitations,
                memo_hit=True,
            )

        try:
            if tool_name == "get_stock_snapshot":
                tool_result = await self._tool_get_stock_snapshot(active_symbol)
            elif tool_name == "get_price_history":
                tool_result = await self._tool_get_price_history(active_symbol, arguments)
            elif tool_name == "get_news_context":
                tool_result = await self._tool_get_news_context(active_symbol, arguments)
            elif tool_name == "get_financial_summary":
                tool_result = await self._tool_get_financial_summary(active_symbol)
            elif tool_name == "get_financial_trends_context":
                tool_result = await self._tool_get_financial_trends_context(active_symbol)
            elif tool_name == "get_earnings_deep_context":
                tool_result = await self._tool_get_earnings_deep_context(active_symbol)
            elif tool_name == "get_analyst_deep_context":
                tool_result = await self._tool_get_analyst_deep_context(active_symbol)
            elif tool_name == "get_ownership_context":
                tool_result = await self._tool_get_ownership_context(active_symbol)
            else:
                return ToolExecutionResult(
                    payload={
                        "error": {
                            "code": "TOOL_NOT_FOUND",
                            "message": f"Unknown tool '{tool_name}'.",
                        }
                    },
                    limitations=[f"Tool '{tool_name}' is unavailable."],
                    memo_hit=False,
                )
        except ApiError as exc:
            self._logger.warning("Tool %s failed with %s", tool_name, exc.code)
            return ToolExecutionResult(
                payload={
                    "error": {
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details,
                    }
                },
                limitations=[f"{tool_name}: {exc.message}"],
                memo_hit=False,
            )

        payload, limitations = tool_result
        cached_at = time.monotonic()
        self._session_store.set_entry(
            session_id=session_id,
            symbol=active_symbol,
            entry=MemoizedToolEntry(
                tool_name=tool_name,
                tool_key=tool_key,
                payload=payload,
                limitations=list(limitations),
                summary=self._summarize_tool_payload(
                    tool_name=tool_name,
                    payload=payload,
                    limitations=limitations,
                    normalized_arguments=normalized_arguments,
                ),
                cached_at=cached_at,
                expires_at=cached_at
                + self._memo_ttl_by_tool.get(tool_name, DEFAULT_CHAT_SESSION_TTL_SECONDS),
            ),
        )
        return ToolExecutionResult(
            payload=payload,
            limitations=limitations,
            memo_hit=False,
        )

    async def _tool_get_stock_snapshot(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        overview = await self._yfinance_service.get_ticker_overview(symbol)
        compact_overview = {
            field_name: getattr(overview.overview, field_name)
            for field_name in COMPACT_STOCK_SNAPSHOT_FIELDS
        }
        payload = {
            "symbol": overview.symbol,
            "overview": compact_overview,
            "dataLimitations": overview.dataLimitations,
        }
        return payload, overview.dataLimitations

    async def _tool_get_price_history(
        self,
        symbol: str,
        arguments: dict[str, Any],
    ) -> tuple[dict[str, Any], list[str]]:
        period = self._coerce_str(arguments.get("period")) or DEFAULT_HISTORY_PERIOD
        interval = self._coerce_str(arguments.get("interval")) or DEFAULT_HISTORY_INTERVAL

        history = await self._yfinance_service.get_ticker_history(
            symbol=symbol,
            period=period,
            interval=interval,
        )
        compact_recent_bars = self._compact_history_bars(history.bars)
        summary = self._summarize_history(history.bars)
        payload = {
            "symbol": history.symbol,
            "period": history.period,
            "interval": history.interval,
            "summary": summary,
            "recentBars": compact_recent_bars,
        }
        self._logger.info(
            "Tool get_price_history symbol=%s period=%s interval=%s compactBars=%d",
            history.symbol,
            history.period,
            history.interval,
            len(compact_recent_bars),
        )
        limitations: list[str] = []
        if len(history.bars) < 2:
            limitations.append("Price history includes fewer than two bars.")
        return payload, limitations

    async def _tool_get_news_context(
        self,
        symbol: str,
        arguments: dict[str, Any],
    ) -> tuple[dict[str, Any], list[str]]:
        raw_limit = self._coerce_int(arguments.get("limit"))
        limit = (
            self._news_tool_default_limit
            if raw_limit is None
            else max(1, min(raw_limit, MAX_NEWS_TOOL_LIMIT))
        )

        news = await self._yfinance_service.get_ticker_news(symbol=symbol, limit=limit)
        compact_news = self._compact_news_items(news.news)
        top_headlines = [item["title"] for item in compact_news if item["title"]][:5]
        payload = {
            "symbol": news.symbol,
            "itemCount": len(news.news),
            "topHeadlines": top_headlines,
            "news": compact_news,
            "dataLimitations": news.dataLimitations,
        }
        self._logger.info(
            "Tool get_news_context symbol=%s compactItems=%d",
            news.symbol,
            len(compact_news),
        )
        return payload, news.dataLimitations

    async def _tool_get_financial_summary(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        summary = await self._yfinance_service.get_financial_summary(symbol)
        payload = {
            "symbol": summary.symbol,
            "financialSummary": summary.financialSummary.model_dump(),
            "dataLimitations": summary.dataLimitations,
        }
        return payload, summary.dataLimitations

    async def _tool_get_financial_trends_context(
        self,
        symbol: str,
    ) -> tuple[dict[str, Any], list[str]]:
        trends = await self._yfinance_service.get_financial_trends(symbol)
        annual_points = self._compact_financial_trend_points(
            trends.annual[-MAX_FINANCIAL_TREND_ANNUAL_POINTS :]
        )
        quarterly_points = self._compact_financial_trend_points(
            trends.quarterly[-MAX_FINANCIAL_TREND_QUARTERLY_POINTS :]
        )
        payload = {
            "symbol": trends.symbol,
            "annual": annual_points,
            "quarterly": quarterly_points,
            "annualSummary": self._summarize_financial_trends(annual_points),
            "quarterlySummary": self._summarize_financial_trends(quarterly_points),
            "dataLimitations": trends.dataLimitations,
        }
        return payload, trends.dataLimitations

    async def _tool_get_earnings_deep_context(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        earnings_context = await self._yfinance_service.get_earnings_context(symbol)
        earnings_history = await self._yfinance_service.get_earnings_history(symbol)
        earnings_estimates = await self._yfinance_service.get_earnings_estimates(symbol)
        recent_events = self._compact_earnings_history_events(
            earnings_history.events[-MAX_EARNINGS_DEEP_EVENTS :]
        )
        payload = {
            "symbol": symbol,
            "nextEarningsDate": earnings_context.earningsContext.next_earnings_date,
            "earningsDateCandidates": earnings_context.earningsContext.earnings_date_candidates,
            "recentSurprises": recent_events,
            "epsEstimates": self._compact_earnings_estimate_points(
                earnings_estimates.epsEstimates[:MAX_EARNINGS_DEEP_ESTIMATE_POINTS]
            ),
            "revenueEstimates": self._compact_revenue_estimate_points(
                earnings_estimates.revenueEstimates[:MAX_EARNINGS_DEEP_ESTIMATE_POINTS]
            ),
            "growthEstimates": self._compact_growth_estimate_points(
                earnings_estimates.growthEstimates[:MAX_EARNINGS_DEEP_ESTIMATE_POINTS]
            ),
            "dataLimitations": self._dedupe_preserve_order(
                [
                    *earnings_context.dataLimitations,
                    *earnings_history.dataLimitations,
                    *earnings_estimates.dataLimitations,
                ]
            ),
        }
        return payload, payload["dataLimitations"]

    async def _tool_get_analyst_deep_context(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        analyst_context = await self._yfinance_service.get_analyst_context(symbol)
        analyst_summary = await self._yfinance_service.get_analyst_summary(symbol)
        analyst_history = await self._yfinance_service.get_analyst_history(symbol)
        payload = {
            "symbol": symbol,
            "currentTargets": {
                "currentPriceTarget": analyst_summary.analystSummary.currentPriceTarget,
                "targetLow": analyst_summary.analystSummary.targetLow,
                "targetHigh": analyst_summary.analystSummary.targetHigh,
                "targetMean": analyst_summary.analystSummary.targetMean,
                "targetMedian": analyst_summary.analystSummary.targetMedian,
            },
            "recommendationSummary": analyst_summary.analystSummary.recommendationSummary.model_dump(),
            "recentActionCount": analyst_summary.analystSummary.recentActionCount,
            "recentActionWindowDays": analyst_summary.analystSummary.recentActionWindowDays,
            "recentActions": self._compact_analyst_actions(
                analyst_context.analystContext.recent_actions[:MAX_ANALYST_DEEP_ACTIONS]
            ),
            "recommendationHistory": self._compact_recommendation_history(
                analyst_history.recommendationHistory[-MAX_ANALYST_DEEP_HISTORY_POINTS :]
            ),
            "actionTimeline": self._compact_analyst_timeline_actions(
                analyst_history.actions[:MAX_ANALYST_DEEP_ACTIONS]
            ),
            "dataLimitations": self._dedupe_preserve_order(
                [
                    *analyst_context.dataLimitations,
                    *analyst_summary.dataLimitations,
                    *analyst_history.dataLimitations,
                ]
            ),
        }
        return payload, payload["dataLimitations"]

    async def _tool_get_ownership_context(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        ownership = await self._yfinance_service.get_ticker_ownership(
            symbol=symbol,
            section="all",
            limit=MAX_OWNERSHIP_CONTEXT_ROWS,
            offset=0,
        )
        payload = {
            "symbol": ownership.symbol,
            "majorHolders": [metric.model_dump() for metric in ownership.majorHolders],
            "institutionalHolders": [
                holder.model_dump() for holder in ownership.institutionalHolders[:MAX_OWNERSHIP_CONTEXT_ROWS]
            ],
            "mutualFundHolders": [
                holder.model_dump() for holder in ownership.mutualFundHolders[:MAX_OWNERSHIP_CONTEXT_ROWS]
            ],
            "insiderRoster": [
                insider.model_dump() for insider in ownership.insiderRoster[:MAX_OWNERSHIP_CONTEXT_ROWS]
            ],
            "dataLimitations": ownership.dataLimitations,
        }
        return payload, ownership.dataLimitations

    def _parse_structured_answer(self, response: LLMModelResponse) -> StructuredChatAnswer:
        raw_payload = response.parsed
        if raw_payload is None:
            raw_payload = self._parse_json_text(response.text)

        if raw_payload is None:
            raise ApiError(
                code="LLM_ERROR",
                message="Model returned a non-JSON chat response.",
                status_code=502,
            )

        try:
            return StructuredChatAnswer.model_validate(raw_payload)
        except ValidationError as exc:
            raise ApiError(
                code="LLM_ERROR",
                message="Model returned an invalid structured chat response.",
                status_code=502,
                details={"errors": exc.errors()},
            ) from exc

    @staticmethod
    def _normalize_and_validate_symbol(symbol: str) -> str:
        normalized_symbol = normalize_symbol(symbol)
        if not is_valid_symbol(normalized_symbol):
            raise ApiError(
                code="INVALID_SYMBOL",
                message="Ticker symbol format is invalid.",
                status_code=400,
                details={"symbol": symbol},
            )
        return normalized_symbol

    @staticmethod
    def _all_tool_specs() -> list[ToolSpec]:
        return [
            ToolSpec(
                name="get_stock_snapshot",
                description="Fetches normalized ticker overview metrics and limitations.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_price_history",
                description="Fetches normalized ticker history and summary for curated periods and intervals.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                        "period": {"type": "string", "enum": list(ALLOWED_HISTORY_PERIODS)},
                        "interval": {
                            "type": "string",
                            "enum": list(ALLOWED_HISTORY_INTERVALS),
                        },
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_news_context",
                description="Fetches normalized ticker news items and limitations.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                        "limit": {"type": "integer", "minimum": 1, "maximum": MAX_NEWS_TOOL_LIMIT},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_financial_summary",
                description="Fetches normalized financial summary metrics and limitations.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_financial_trends_context",
                description="Fetches compact annual and quarterly financial trend context with derived summaries.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_earnings_deep_context",
                description="Fetches compact earnings date, surprise history, and estimate trend context.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_analyst_deep_context",
                description="Fetches compact analyst target, recommendation, and recent action history context.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_ownership_context",
                description="Fetches compact ownership context including major holders, institutions, funds, and insiders.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
        ]

    def _compact_history_bars(self, bars: list[PriceBar]) -> list[dict[str, Any]]:
        return [
            {
                "timestamp": bar.timestamp,
                "close": bar.close,
                "high": bar.high,
                "low": bar.low,
                "volume": bar.volume,
            }
            for bar in bars[-self._history_recent_bars_limit :]
        ]

    @staticmethod
    def _compact_news_items(news_items: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "title": item.title,
                "publisher": item.publisher,
                "published_at": item.published_at,
                "link": item.link,
            }
            for item in news_items
        ]

    @staticmethod
    def _compact_financial_trend_points(points: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "periodEnd": point.periodEnd,
                "revenue": point.revenue,
                "netIncome": point.netIncome,
                "operatingCashFlow": point.operatingCashFlow,
                "capitalExpenditure": point.capitalExpenditure,
                "freeCashFlow": point.freeCashFlow,
            }
            for point in points
        ]

    @staticmethod
    def _summarize_financial_trends(points: list[dict[str, Any]]) -> dict[str, Any]:
        if len(points) < 2:
            return {
                "pointCount": len(points),
                "latestPeriodEnd": points[-1]["periodEnd"] if points else None,
                "revenueDelta": None,
                "netIncomeDelta": None,
                "freeCashFlowDelta": None,
            }

        latest = points[-1]
        previous = points[-2]
        return {
            "pointCount": len(points),
            "latestPeriodEnd": latest["periodEnd"],
            "revenueDelta": ChatService._delta_or_none(previous["revenue"], latest["revenue"]),
            "netIncomeDelta": ChatService._delta_or_none(previous["netIncome"], latest["netIncome"]),
            "freeCashFlowDelta": ChatService._delta_or_none(
                previous["freeCashFlow"],
                latest["freeCashFlow"],
            ),
        }

    @staticmethod
    def _compact_earnings_history_events(events: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "reportDate": event.reportDate,
                "quarter": event.quarter,
                "epsEstimate": event.epsEstimate,
                "epsActual": event.epsActual,
                "surprisePercent": event.surprisePercent,
            }
            for event in events
        ]

    @staticmethod
    def _compact_earnings_estimate_points(points: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "period": point.period,
                "avg": point.avg,
                "low": point.low,
                "high": point.high,
                "yearAgoEps": point.yearAgoEps,
                "numberOfAnalysts": point.numberOfAnalysts,
                "growth": point.growth,
            }
            for point in points
        ]

    @staticmethod
    def _compact_revenue_estimate_points(points: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "period": point.period,
                "avg": point.avg,
                "low": point.low,
                "high": point.high,
                "numberOfAnalysts": point.numberOfAnalysts,
                "yearAgoRevenue": point.yearAgoRevenue,
                "growth": point.growth,
            }
            for point in points
        ]

    @staticmethod
    def _compact_growth_estimate_points(points: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "period": point.period,
                "stockTrend": point.stockTrend,
                "indexTrend": point.indexTrend,
            }
            for point in points
        ]

    @staticmethod
    def _compact_analyst_actions(actions: list[Any]) -> list[dict[str, Any]]:
        return [
            {
                "gradedAt": action.graded_at,
                "firm": action.firm,
                "toGrade": action.to_grade,
                "fromGrade": action.from_grade,
                "action": action.action,
                "priceTargetAction": action.price_target_action,
                "currentPriceTarget": action.current_price_target,
                "priorPriceTarget": action.prior_price_target,
            }
            for action in actions
        ]

    @staticmethod
    def _compact_recommendation_history(history: list[Any]) -> list[dict[str, Any]]:
        return [item.model_dump() for item in history]

    @staticmethod
    def _compact_analyst_timeline_actions(actions: list[Any]) -> list[dict[str, Any]]:
        return [item.model_dump() for item in actions]

    @staticmethod
    def _delta_or_none(previous: float | None, current: float | None) -> float | None:
        if previous is None or current is None:
            return None
        return current - previous

    @staticmethod
    def _summarize_history(bars: list[PriceBar]) -> dict[str, Any]:
        if not bars:
            return {
                "barCount": 0,
                "start": None,
                "end": None,
                "firstClose": None,
                "lastClose": None,
                "absoluteChange": None,
                "percentChange": None,
                "periodHigh": None,
                "periodLow": None,
                "averageVolume": None,
            }

        first_close = bars[0].close
        last_close = bars[-1].close
        absolute_change = last_close - first_close
        percent_change: float | None = None
        if first_close != 0:
            percent_change = (absolute_change / first_close) * 100.0

        highs = [bar.high for bar in bars]
        lows = [bar.low for bar in bars]
        volumes = [bar.volume for bar in bars if bar.volume is not None]
        average_volume: float | None = None
        if volumes:
            average_volume = sum(volumes) / len(volumes)

        return {
            "barCount": len(bars),
            "start": bars[0].timestamp,
            "end": bars[-1].timestamp,
            "firstClose": first_close,
            "lastClose": last_close,
            "absoluteChange": absolute_change,
            "percentChange": percent_change,
            "periodHigh": max(highs),
            "periodLow": min(lows),
            "averageVolume": average_volume,
        }

    @staticmethod
    def _parse_json_text(text: str | None) -> dict[str, Any] | None:
        if text is None:
            return None
        stripped = text.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _coerce_str(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _dedupe_preserve_order(values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            deduped.append(value)
        return deduped
