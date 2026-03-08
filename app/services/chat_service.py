from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, Field, ValidationError

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.providers.llm.base import LLMMessage, LLMModelResponse, LLMProvider, ToolCall, ToolSpec
from app.schemas.chat import ChatRequest, ChatResponse
from app.schemas.ticker import PriceBar
from app.services.yfinance_service import YFinanceService
from app.utils.symbols import is_valid_symbol, normalize_symbol

DEFAULT_HISTORY_PERIOD = "6mo"
DEFAULT_HISTORY_INTERVAL = "1d"
HISTORY_RECENT_BARS_LIMIT = 30
MAX_TOOL_CALL_ROUNDS = 3
MAX_NEWS_TOOL_LIMIT = 10

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
        max_turns: int = 12,
    ) -> None:
        self._yfinance_service = yfinance_service
        self._llm_provider = llm_provider
        self._max_turns = max(1, max_turns)
        self._logger = get_logger(__name__)

    async def chat(self, payload: ChatRequest) -> ChatResponse:
        symbol = self._normalize_and_validate_symbol(payload.symbol)
        messages = self._build_messages(payload)
        collected_limitations: list[str] = []
        used_tools: list[str] = []

        for tool_round in range(MAX_TOOL_CALL_ROUNDS + 1):
            model_response = await self._llm_provider.generate(
                system_instruction=self._build_system_instruction(symbol),
                messages=messages,
                tools=self._tool_specs(),
                response_schema=CHAT_RESPONSE_SCHEMA,
            )

            if model_response.tool_calls:
                if tool_round >= MAX_TOOL_CALL_ROUNDS:
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

    def _build_messages(self, payload: ChatRequest) -> list[LLMMessage]:
        clipped_turns = payload.conversation[-self._max_turns :]
        messages: list[LLMMessage] = []
        for turn in clipped_turns:
            stripped_content = turn.content.strip()
            if not stripped_content:
                continue
            messages.append(LLMMessage(role=turn.role, content=stripped_content))

        current_message = payload.message.strip()
        if not current_message:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Chat message cannot be empty.",
                status_code=400,
            )

        messages.append(LLMMessage(role="user", content=current_message))
        return messages

    @staticmethod
    def _build_system_instruction(symbol: str) -> str:
        return f"{CHAT_SYSTEM_INSTRUCTION}\nActive ticker symbol: {symbol}"

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
        payload = {
            "symbol": overview.symbol,
            "overview": overview.overview.model_dump(),
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
        summary = self._summarize_history(history.bars)
        payload = {
            "symbol": history.symbol,
            "period": history.period,
            "interval": history.interval,
            "summary": summary,
            "recentBars": [bar.model_dump() for bar in history.bars[-HISTORY_RECENT_BARS_LIMIT:]],
        }
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
        limit = 5 if raw_limit is None else max(1, min(raw_limit, MAX_NEWS_TOOL_LIMIT))

        news = await self._yfinance_service.get_ticker_news(symbol=symbol, limit=limit)
        top_headlines = [item.title for item in news.news if item.title][:5]
        payload = {
            "symbol": news.symbol,
            "itemCount": len(news.news),
            "topHeadlines": top_headlines,
            "news": [item.model_dump() for item in news.news],
            "dataLimitations": news.dataLimitations,
        }
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
    def _tool_specs() -> list[ToolSpec]:
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
                description="Fetches normalized ticker history and summary.",
                parameters={
                    "type": "object",
                    "properties": {
                        "symbol": {"type": "string"},
                        "period": {"type": "string"},
                        "interval": {"type": "string"},
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
