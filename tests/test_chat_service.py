import unittest
from typing import Any

from app.providers.llm.base import LLMModelResponse, LLMProvider, ToolCall, ToolSpec, LLMMessage
from app.schemas.chat import ChatRequest, ChatTurn
from app.schemas.ticker import (
    EarningsContext,
    EarningsContextResponse,
    EarningsEstimatePoint,
    EarningsEstimatesResponse,
    EarningsHistoryEvent,
    EarningsHistoryResponse,
    FinancialSummary,
    FinancialSummaryResponse,
    FinancialTrendPoint,
    FinancialTrendsResponse,
    HolderEntry,
    InsiderRosterEntry,
    MajorHolderMetric,
    OwnershipPagination,
    OwnershipResponse,
    PriceBar,
    TickerHistoryResponse,
    TickerNewsItem,
    TickerNewsResponse,
    TickerOverview,
    TickerOverviewResponse,
)
from app.services.chat_service import ChatService


class FakeYFinanceService:
    def __init__(self) -> None:
        self.call_counts = {
            "overview": 0,
            "history": 0,
            "news": 0,
            "financial_summary": 0,
            "financial_trends": 0,
            "earnings_context": 0,
            "earnings_history": 0,
            "earnings_estimates": 0,
            "ownership": 0,
        }

    async def get_ticker_overview(self, symbol: str) -> TickerOverviewResponse:
        self.call_counts["overview"] += 1
        return TickerOverviewResponse(
            symbol=symbol,
            overview=TickerOverview(
                display_name=f"{symbol} Inc.",
                current_price=190.0,
                market_cap=1_000_000_000.0,
                sector="Technology",
                industry="Software",
            ),
            dataLimitations=[],
        )

    async def get_ticker_history(self, symbol: str, period: str, interval: str) -> TickerHistoryResponse:
        self.call_counts["history"] += 1
        return TickerHistoryResponse(
            symbol=symbol,
            period=period,
            interval=interval,
            bars=[
                PriceBar(
                    timestamp="2026-03-10T00:00:00Z",
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=1000,
                ),
                PriceBar(
                    timestamp="2026-03-11T00:00:00Z",
                    open=101.0,
                    high=102.0,
                    low=100.0,
                    close=101.0,
                    volume=1000,
                ),
            ],
        )

    async def get_ticker_news(self, symbol: str, limit: int = 10) -> TickerNewsResponse:
        self.call_counts["news"] += 1
        return TickerNewsResponse(
            symbol=symbol,
            news=[
                TickerNewsItem(title="Headline 1"),
                TickerNewsItem(title="Headline 2"),
            ],
            dataLimitations=[],
        )

    async def get_financial_summary(self, symbol: str) -> FinancialSummaryResponse:
        self.call_counts["financial_summary"] += 1
        return FinancialSummaryResponse(
            symbol=symbol,
            financialSummary=FinancialSummary(
                revenue_ttm=10.0,
                net_income_ttm=2.0,
                free_cash_flow=3.0,
            ),
            dataLimitations=[],
        )

    async def get_financial_trends(self, symbol: str) -> FinancialTrendsResponse:
        self.call_counts["financial_trends"] += 1
        return FinancialTrendsResponse(
            symbol=symbol,
            annual=[
                FinancialTrendPoint(periodEnd="2025-12-31", revenue=10.0, netIncome=2.0, freeCashFlow=3.0),
                FinancialTrendPoint(periodEnd="2026-12-31", revenue=12.0, netIncome=2.5, freeCashFlow=4.0),
            ],
            quarterly=[
                FinancialTrendPoint(periodEnd="2026-09-30", revenue=2.5, netIncome=0.5, freeCashFlow=0.8),
                FinancialTrendPoint(periodEnd="2026-12-31", revenue=3.0, netIncome=0.7, freeCashFlow=1.0),
            ],
            dataLimitations=[],
        )

    async def get_earnings_context(self, symbol: str) -> EarningsContextResponse:
        self.call_counts["earnings_context"] += 1
        return EarningsContextResponse(
            symbol=symbol,
            earningsContext=EarningsContext(
                next_earnings_date="2026-04-30T20:00:00Z",
                earnings_date_candidates=["2026-04-30T20:00:00Z"],
            ),
            dataLimitations=[],
        )

    async def get_earnings_history(self, symbol: str) -> EarningsHistoryResponse:
        self.call_counts["earnings_history"] += 1
        return EarningsHistoryResponse(
            symbol=symbol,
            events=[
                EarningsHistoryEvent(
                    reportDate="2025-12-31T00:00:00Z",
                    quarter="Q1",
                    epsEstimate=1.0,
                    epsActual=1.1,
                    surprisePercent=10.0,
                )
            ],
            dataLimitations=[],
        )

    async def get_earnings_estimates(self, symbol: str) -> EarningsEstimatesResponse:
        self.call_counts["earnings_estimates"] += 1
        return EarningsEstimatesResponse(
            symbol=symbol,
            epsEstimates=[
                EarningsEstimatePoint(period="Q2", avg=1.2, growth=0.05),
            ],
            revenueEstimates=[],
            growthEstimates=[],
            dataLimitations=[],
        )

    async def get_ticker_ownership(
        self,
        symbol: str,
        section: str,
        limit: int,
        offset: int,
    ) -> OwnershipResponse:
        self.call_counts["ownership"] += 1
        return OwnershipResponse(
            symbol=symbol,
            requestedSection=section,
            limit=limit,
            offset=offset,
            majorHolders=[MajorHolderMetric(key="institutions", label="Institutions", value=0.7)],
            institutionalHolders=[HolderEntry(holder="Vanguard")],
            mutualFundHolders=[HolderEntry(holder="Fidelity")],
            insiderRoster=[InsiderRosterEntry(name="CEO")],
            institutionalPagination=OwnershipPagination(
                offset=offset,
                limit=limit,
                returnedCount=1,
                totalAvailable=1,
                hasMore=False,
            ),
            mutualFundPagination=OwnershipPagination(
                offset=offset,
                limit=limit,
                returnedCount=1,
                totalAvailable=1,
                hasMore=False,
            ),
            insiderRosterPagination=OwnershipPagination(
                offset=offset,
                limit=limit,
                returnedCount=1,
                totalAvailable=1,
                hasMore=False,
            ),
            dataLimitations=[],
        )


class FakeLLMProvider(LLMProvider):
    def __init__(self) -> None:
        self.force_cached_context_answer = False

    async def generate(
        self,
        *,
        system_instruction: str,
        messages: list[LLMMessage],
        tools: list[ToolSpec],
        response_schema: dict[str, Any],
    ) -> LLMModelResponse:
        latest_user = next((message.content for message in reversed(messages) if message.role == "user"), "")
        tool_messages = [message for message in messages if message.role == "tool"]

        if tool_messages:
            return LLMModelResponse(
                parsed={
                    "answer": "tool-grounded answer",
                    "highlights": ["ok"],
                    "limitations": [],
                }
            )

        if self.force_cached_context_answer and "same chat session" in system_instruction:
            return LLMModelResponse(
                parsed={
                    "answer": "answered from cached context",
                    "highlights": ["cached"],
                    "limitations": [],
                }
            )

        tool_names = {tool.name for tool in tools}
        lowered_user = latest_user.lower()
        if "earnings" in lowered_user and "get_earnings_deep_context" in tool_names:
            return LLMModelResponse(
                tool_calls=[ToolCall(id="call_earnings", name="get_earnings_deep_context", arguments={})]
            )
        if "history" in lowered_user and "get_price_history" in tool_names:
            arguments = {}
            if "explicit defaults" in lowered_user:
                arguments = {"period": "6mo", "interval": "1d"}
            return LLMModelResponse(
                tool_calls=[ToolCall(id="call_history", name="get_price_history", arguments=arguments)]
            )
        if "ownership" in lowered_user and "get_ownership_context" in tool_names:
            return LLMModelResponse(
                tool_calls=[ToolCall(id="call_ownership", name="get_ownership_context", arguments={})]
            )

        return LLMModelResponse(
            parsed={
                "answer": "no tools needed",
                "highlights": ["none"],
                "limitations": [],
            }
        )


class ChatServiceMemoizationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.yfinance = FakeYFinanceService()
        self.llm = FakeLLMProvider()
        self.service = ChatService(
            yfinance_service=self.yfinance,
            llm_provider=self.llm,
            max_turns=6,
            max_tool_call_rounds=2,
            history_recent_bars_limit=12,
            news_tool_default_limit=3,
            session_ttl_seconds=1800,
            session_max_tool_entries=16,
            memo_ttl_overview_seconds=300,
            memo_ttl_history_seconds=300,
            memo_ttl_news_seconds=900,
            memo_ttl_financials_seconds=3600,
            memo_ttl_earnings_seconds=3600,
            memo_ttl_analyst_seconds=3600,
            tool_gating_mode="balanced",
        )

    def _metrics_snapshot(self) -> dict[str, Any]:
        return self.service._memo_metrics.record_request(
            cached_context_tool_names=[],
            cached_context_satisfied=False,
            memo_hit_tool_names=[],
            cold_miss_tool_names=[],
        )

    async def test_chat_creates_session_and_records_cold_miss(self) -> None:
        response = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        self.assertTrue(response.sessionId)
        self.assertEqual(response.usedTools, ["get_earnings_deep_context"])
        self.assertEqual(self.yfinance.call_counts["earnings_context"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_history"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_estimates"], 1)

        metrics = self._metrics_snapshot()
        self.assertEqual(metrics["requests"], 2)
        self.assertEqual(metrics["cachedContextAvailableRequests"], 0)
        self.assertEqual(metrics["cachedContextSatisfiedRequests"], 0)
        self.assertEqual(metrics["memoHits"], 0)
        self.assertEqual(metrics["coldMisses"], 1)
        self.assertEqual(metrics["perTool"]["get_earnings_deep_context"]["coldMisses"], 1)

    async def test_chat_reuses_cached_context_without_tool_call(self) -> None:
        first = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )
        self.llm.force_cached_context_answer = True

        second = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                sessionId=first.sessionId,
                message="Given that, what are the top near-term risks?",
                conversation=[
                    ChatTurn(role="user", content="Have they been beating earnings lately and what are estimates now?"),
                    ChatTurn(role="assistant", content=first.answer),
                ],
            )
        )

        self.assertEqual(second.sessionId, first.sessionId)
        self.assertEqual(second.usedTools, [])
        self.assertEqual(self.yfinance.call_counts["earnings_context"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_history"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_estimates"], 1)

        metrics = self._metrics_snapshot()
        self.assertEqual(metrics["cachedContextAvailableRequests"], 1)
        self.assertEqual(metrics["cachedContextSatisfiedRequests"], 1)
        self.assertEqual(
            metrics["perTool"]["get_earnings_deep_context"]["cachedContextAvailable"],
            1,
        )
        self.assertEqual(
            metrics["perTool"]["get_earnings_deep_context"]["cachedContextSatisfied"],
            1,
        )

    async def test_chat_reuses_memoized_tool_payload_on_repeat_tool_call(self) -> None:
        first = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        second = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                sessionId=first.sessionId,
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        self.assertEqual(second.sessionId, first.sessionId)
        self.assertEqual(second.usedTools, ["get_earnings_deep_context"])
        self.assertEqual(self.yfinance.call_counts["earnings_context"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_history"], 1)
        self.assertEqual(self.yfinance.call_counts["earnings_estimates"], 1)

        metrics = self._metrics_snapshot()
        self.assertEqual(metrics["memoHits"], 1)
        self.assertEqual(metrics["coldMisses"], 1)
        self.assertEqual(metrics["perTool"]["get_earnings_deep_context"]["memoHits"], 1)

    async def test_chat_restarts_session_when_symbol_changes(self) -> None:
        first = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        second = await self.service.chat(
            ChatRequest(
                symbol="MSFT",
                sessionId=first.sessionId,
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        self.assertNotEqual(second.sessionId, first.sessionId)
        self.assertEqual(self.yfinance.call_counts["earnings_context"], 2)
        self.assertEqual(self.yfinance.call_counts["earnings_history"], 2)
        self.assertEqual(self.yfinance.call_counts["earnings_estimates"], 2)

    async def test_chat_normalizes_tool_arguments_for_memo_keys(self) -> None:
        first = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Show me the price history",
                conversation=[],
            )
        )

        second = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                sessionId=first.sessionId,
                message="Show me the price history with explicit defaults",
                conversation=[],
            )
        )

        self.assertEqual(second.sessionId, first.sessionId)
        self.assertEqual(second.usedTools, ["get_price_history"])
        self.assertEqual(self.yfinance.call_counts["history"], 1)

        metrics = self._metrics_snapshot()
        self.assertEqual(metrics["memoHits"], 1)
        self.assertEqual(metrics["perTool"]["get_price_history"]["memoHits"], 1)

    async def test_chat_memo_metrics_accumulate_per_tool(self) -> None:
        earnings = await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )
        self.llm.force_cached_context_answer = True
        await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                sessionId=earnings.sessionId,
                message="Given that, what are the top near-term risks?",
                conversation=[
                    ChatTurn(role="user", content="Have they been beating earnings lately and what are estimates now?"),
                    ChatTurn(role="assistant", content=earnings.answer),
                ],
            )
        )
        self.llm.force_cached_context_answer = False
        await self.service.chat(
            ChatRequest(
                symbol="AAPL",
                sessionId=earnings.sessionId,
                message="Have they been beating earnings lately and what are estimates now?",
                conversation=[],
            )
        )

        metrics = self._metrics_snapshot()
        earnings_metrics = metrics["perTool"]["get_earnings_deep_context"]
        self.assertGreaterEqual(metrics["requests"], 4)
        self.assertEqual(metrics["cachedContextAvailableRequests"], 2)
        self.assertEqual(metrics["cachedContextSatisfiedRequests"], 1)
        self.assertEqual(metrics["memoHits"], 1)
        self.assertEqual(metrics["coldMisses"], 1)
        self.assertEqual(earnings_metrics["cachedContextAvailable"], 2)
        self.assertEqual(earnings_metrics["cachedContextSatisfied"], 1)
        self.assertEqual(earnings_metrics["memoHits"], 1)
        self.assertEqual(earnings_metrics["coldMisses"], 1)


if __name__ == "__main__":
    unittest.main()
