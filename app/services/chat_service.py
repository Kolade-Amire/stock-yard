from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

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
MAX_NEWS_TOOL_LIMIT = 10
TOOL_GATING_MODE_BALANCED = "balanced"

ALLOWED_HISTORY_PERIODS = ("1d", "5d", "1mo", "3mo", "6mo", "1y", "5y", "max")
ALLOWED_HISTORY_INTERVALS = ("1m", "5m", "15m", "1h", "1d", "1wk", "1mo")

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
SPECIFIC_INTENT_NAMES = ("price", "news", "financials", "earnings", "analyst")
CONTEXT_INTENT_PRIORITY = ("earnings", "analyst", "financials", "news", "price")
NORMALIZE_PUNCTUATION_RE = re.compile(r"[^a-z0-9/\-\s]+")
NORMALIZE_WHITESPACE_RE = re.compile(r"\s+")


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
        tools=("get_financial_summary", "get_stock_snapshot"),
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
        tools=("get_earnings_context",),
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
        tools=("get_analyst_context",),
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

        if self._tool_gating_mode != TOOL_GATING_MODE_BALANCED:
            raise ValueError("Only the 'balanced' chat tool gating mode is supported.")

    async def chat(self, payload: ChatRequest) -> ChatResponse:
        symbol = self._normalize_and_validate_symbol(payload.symbol)
        clipped_turns = self._clip_conversation(payload.conversation)
        messages = self._build_messages(
            clipped_turns=clipped_turns,
            current_message=payload.message,
        )
        selected_tools, matched_intents, context_extra_intent = self._select_tool_specs(
            current_message=payload.message,
            clipped_turns=clipped_turns,
        )
        collected_limitations: list[str] = []
        used_tools: list[str] = []

        self._logger.info(
            "Chat tool selection symbol=%s turns=%d intents=%s contextExtra=%s tools=%s",
            symbol,
            len(clipped_turns),
            matched_intents,
            context_extra_intent,
            [tool.name for tool in selected_tools],
        )

        for tool_round in range(self._max_tool_call_rounds + 1):
            model_response = await self._llm_provider.generate(
                system_instruction=self._build_system_instruction(symbol),
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

                messages.append(
                    LLMMessage(role="assistant", content="", tool_calls=model_response.tool_calls)
                )
                for tool_call in model_response.tool_calls:
                    used_tools.append(tool_call.name)
                    tool_result, tool_limitations = await self._execute_tool(
                        active_symbol=symbol,
                        tool_call=tool_call,
                    )
                    collected_limitations.extend(tool_limitations)
                    messages.append(
                        LLMMessage(
                            role="tool",
                            content=json.dumps(tool_result, default=str),
                            name=tool_call.name,
                            tool_call_id=tool_call.id,
                        )
                    )
                continue

            structured_answer = self._parse_structured_answer(model_response)
            merged_limitations = self._dedupe_preserve_order(
                [*structured_answer.limitations, *collected_limitations]
            )
            return ChatResponse(
                symbol=symbol,
                answer=structured_answer.answer,
                highlights=structured_answer.highlights,
                usedTools=self._dedupe_preserve_order(used_tools),
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
    def _build_system_instruction(symbol: str) -> str:
        return f"{CHAT_SYSTEM_INSTRUCTION}\nActive ticker symbol: {symbol}"

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
        tool_call: ToolCall,
    ) -> tuple[dict[str, Any], list[str]]:
        tool_name = tool_call.name
        arguments = tool_call.arguments

        try:
            if tool_name == "get_stock_snapshot":
                return await self._tool_get_stock_snapshot(active_symbol)
            if tool_name == "get_price_history":
                return await self._tool_get_price_history(active_symbol, arguments)
            if tool_name == "get_news_context":
                return await self._tool_get_news_context(active_symbol, arguments)
            if tool_name == "get_financial_summary":
                return await self._tool_get_financial_summary(active_symbol)
            if tool_name == "get_earnings_context":
                return await self._tool_get_earnings_context(active_symbol)
            if tool_name == "get_analyst_context":
                return await self._tool_get_analyst_context(active_symbol)
        except ApiError as exc:
            self._logger.warning("Tool %s failed with %s", tool_name, exc.code)
            return (
                {
                    "error": {
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details,
                    }
                },
                [f"{tool_name}: {exc.message}"],
            )

        return (
            {"error": {"code": "TOOL_NOT_FOUND", "message": f"Unknown tool '{tool_name}'."}},
            [f"Tool '{tool_name}' is unavailable."],
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

    async def _tool_get_earnings_context(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        earnings_context = await self._yfinance_service.get_earnings_context(symbol)
        payload = {
            "symbol": earnings_context.symbol,
            "earningsContext": earnings_context.earningsContext.model_dump(),
            "dataLimitations": earnings_context.dataLimitations,
        }
        return payload, earnings_context.dataLimitations

    async def _tool_get_analyst_context(self, symbol: str) -> tuple[dict[str, Any], list[str]]:
        analyst_context = await self._yfinance_service.get_analyst_context(symbol)
        payload = {
            "symbol": analyst_context.symbol,
            "analystContext": analyst_context.analystContext.model_dump(),
            "dataLimitations": analyst_context.dataLimitations,
        }
        return payload, analyst_context.dataLimitations

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
                name="get_earnings_context",
                description="Fetches normalized earnings-date and estimate context with limitations.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                    },
                    "additionalProperties": False,
                },
            ),
            ToolSpec(
                name="get_analyst_context",
                description="Fetches analyst target, recommendation, and recent action context.",
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
