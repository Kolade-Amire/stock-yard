from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any

import yfinance as yf
from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.schemas.ticker import (
    AnalystActionEvent,
    AnalystContext,
    AnalystContextResponse,
    AnalystRecommendationSnapshot,
    EarningsContext,
    EarningsContextResponse,
    FinancialSummary,
    FinancialSummaryResponse,
    PriceBar,
    TickerHistoryResponse,
    TickerNewsItem,
    TickerNewsResponse,
    TickerOverview,
    TickerOverviewResponse,
    TickerSearchResponse,
    TickerSearchResult,
)
from app.schemas.market import (
    BenchmarkFund,
    BenchmarkFundsResponse,
    BenchmarkHolding,
    BenchmarkSectorWeight,
    EarningsCalendarEvent,
    EarningsCalendarResponse,
    MarketMover,
    MarketMoversResponse,
    SectorCompanyReference,
    SectorDetailResponse,
    SectorFundReference,
    SectorIndustryReference,
    SectorOverview,
    SectorPulseItem,
    SectorPulseResponse,
)
from app.utils.cache import TTLCache
from app.utils.mappers import (
    coerce_bool,
    coerce_datetime_string,
    coerce_float,
    coerce_int,
    coerce_str,
    first_non_null,
)
from app.utils.symbols import is_valid_symbol, normalize_query, normalize_symbol

ALLOWED_QUOTE_TYPES = {"EQUITY", "ETF"}
MARKET_SCOPE_US = "us"
DEFAULT_MOVERS_LIMIT = 10
MAX_MOVERS_LIMIT = 25
DEFAULT_EARNINGS_CALENDAR_LIMIT = 25
MAX_EARNINGS_CALENDAR_LIMIT = 100
MAX_BENCHMARK_HOLDINGS = 5
MAX_BENCHMARK_SECTOR_WEIGHTS = 5
MAX_SECTOR_PULSE_FUNDS = 3
MAX_SECTOR_PULSE_COMPANIES = 3
ALLOWED_MOVER_SCREENS: dict[str, str] = {
    "gainers": "day_gainers",
    "losers": "day_losers",
    "most_active": "most_actives",
}
CURATED_BENCHMARK_FUNDS: tuple[dict[str, str], ...] = (
    {
        "symbol": "SPY",
        "benchmarkKey": "sp500",
        "benchmarkName": "S&P 500",
        "category": "large_cap_us",
    },
    {
        "symbol": "QQQ",
        "benchmarkKey": "nasdaq100",
        "benchmarkName": "Nasdaq-100",
        "category": "large_cap_growth_us",
    },
    {
        "symbol": "DIA",
        "benchmarkKey": "dow30",
        "benchmarkName": "Dow Jones Industrial Average",
        "category": "large_cap_value_us",
    },
    {
        "symbol": "IWM",
        "benchmarkKey": "russell2000",
        "benchmarkName": "Russell 2000",
        "category": "small_cap_us",
    },
    {
        "symbol": "VTI",
        "benchmarkKey": "total_us_market",
        "benchmarkName": "Total US Stock Market",
        "category": "broad_market_us",
    },
    {
        "symbol": "BND",
        "benchmarkKey": "us_aggregate_bond",
        "benchmarkName": "US Aggregate Bond",
        "category": "bonds_us",
    },
)
CURATED_SECTOR_KEYS = (
    "basic-materials",
    "communication-services",
    "consumer-cyclical",
    "consumer-defensive",
    "energy",
    "financial-services",
    "healthcare",
    "industrials",
    "real-estate",
    "technology",
    "utilities",
)
MAX_NEWS_LIMIT = 50
EARNINGS_DATES_LIMIT = 8
ANALYST_ACTION_WINDOW_DAYS = 90
MAX_ANALYST_ACTION_EVENTS = 5
ALLOWED_HISTORY_PERIODS = frozenset({"1d", "5d", "1mo", "3mo", "6mo", "1y", "5y", "max"})
ALLOWED_HISTORY_INTERVALS = frozenset({"1m", "5m", "15m", "1h", "1d", "1wk", "1mo"})
ALLOWED_HISTORY_PERIODS_BY_INTERVAL = {
    "1m": frozenset({"1d", "5d"}),
    "5m": frozenset({"1d", "5d", "1mo"}),
    "15m": frozenset({"1d", "5d", "1mo"}),
    "1h": frozenset({"1d", "5d", "1mo"}),
    "1d": ALLOWED_HISTORY_PERIODS,
    "1wk": ALLOWED_HISTORY_PERIODS,
    "1mo": ALLOWED_HISTORY_PERIODS,
}


class YFinanceService:
    def __init__(
        self,
        *,
        cache_ttl_overview_seconds: int,
        cache_ttl_history_seconds: int,
        cache_ttl_news_seconds: int,
        cache_ttl_movers_seconds: int,
        cache_ttl_benchmarks_seconds: int,
        cache_ttl_earnings_calendar_seconds: int,
        cache_ttl_sectors_seconds: int,
        cache_ttl_financials_seconds: int,
        cache_ttl_earnings_seconds: int,
        cache_ttl_analyst_seconds: int,
    ) -> None:
        self._logger = get_logger(__name__)
        self._overview_cache = TTLCache[TickerOverviewResponse](cache_ttl_overview_seconds)
        self._history_cache = TTLCache[TickerHistoryResponse](cache_ttl_history_seconds)
        self._news_cache = TTLCache[TickerNewsResponse](cache_ttl_news_seconds)
        self._movers_cache = TTLCache[MarketMoversResponse](cache_ttl_movers_seconds)
        self._benchmarks_cache = TTLCache[BenchmarkFundsResponse](cache_ttl_benchmarks_seconds)
        self._earnings_calendar_cache = TTLCache[EarningsCalendarResponse](
            cache_ttl_earnings_calendar_seconds
        )
        self._sector_pulse_cache = TTLCache[SectorPulseResponse](cache_ttl_sectors_seconds)
        self._sector_detail_cache = TTLCache[SectorDetailResponse](cache_ttl_sectors_seconds)
        self._financial_summary_cache = TTLCache[FinancialSummaryResponse](
            cache_ttl_financials_seconds
        )
        self._earnings_context_cache = TTLCache[EarningsContextResponse](cache_ttl_earnings_seconds)
        self._analyst_context_cache = TTLCache[AnalystContextResponse](cache_ttl_analyst_seconds)

    async def search_tickers(self, query: str, limit: int = 10) -> TickerSearchResponse:
        normalized_query = normalize_query(query)
        if not normalized_query:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Query cannot be empty.",
                status_code=400,
            )
        return await run_in_threadpool(self._search_tickers_sync, normalized_query, limit)

    async def get_ticker_overview(self, symbol: str) -> TickerOverviewResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)

        cached = self._overview_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Overview cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_ticker_overview_sync, normalized_symbol)
        self._overview_cache.set(normalized_symbol, response)
        return response

    async def get_ticker_news(self, symbol: str, limit: int = 10) -> TickerNewsResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        bounded_limit = max(1, min(limit, MAX_NEWS_LIMIT))

        cache_key = f"{normalized_symbol}:{bounded_limit}"
        cached = self._news_cache.get(cache_key)
        if cached is not None:
            self._logger.info("News cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_ticker_news_sync,
            normalized_symbol,
            bounded_limit,
        )
        self._news_cache.set(cache_key, response)
        return response

    async def get_market_movers(self, screen: str, limit: int = DEFAULT_MOVERS_LIMIT) -> MarketMoversResponse:
        normalized_screen = self._normalize_and_validate_mover_screen(screen)
        bounded_limit = self._normalize_and_validate_mover_limit(limit)

        cache_key = f"{normalized_screen}:{bounded_limit}"
        cached = self._movers_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Movers cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_market_movers_sync,
            normalized_screen,
            bounded_limit,
        )
        self._movers_cache.set(cache_key, response)
        return response

    async def get_benchmark_funds(self) -> BenchmarkFundsResponse:
        cache_key = MARKET_SCOPE_US
        cached = self._benchmarks_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Benchmarks cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(self._get_benchmark_funds_sync)
        self._benchmarks_cache.set(cache_key, response)
        return response

    async def get_earnings_calendar(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        limit: int = DEFAULT_EARNINGS_CALENDAR_LIMIT,
        active_only: bool = True,
    ) -> EarningsCalendarResponse:
        normalized_start, normalized_end = self._normalize_earnings_calendar_range(
            start=start,
            end=end,
        )
        bounded_limit = self._normalize_and_validate_earnings_calendar_limit(limit)

        cache_key = (
            f"{normalized_start.isoformat()}:{normalized_end.isoformat()}:"
            f"{bounded_limit}:{int(active_only)}"
        )
        cached = self._earnings_calendar_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Earnings calendar cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_earnings_calendar_sync,
            normalized_start,
            normalized_end,
            bounded_limit,
            active_only,
        )
        self._earnings_calendar_cache.set(cache_key, response)
        return response

    async def get_sector_pulse(self) -> SectorPulseResponse:
        cache_key = MARKET_SCOPE_US
        cached = self._sector_pulse_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Sector pulse cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(self._get_sector_pulse_sync)
        self._sector_pulse_cache.set(cache_key, response)
        return response

    async def get_sector_detail(self, *, sector_key: str) -> SectorDetailResponse:
        normalized_key = self._normalize_and_validate_sector_key(sector_key)
        cached = self._sector_detail_cache.get(normalized_key)
        if cached is not None:
            self._logger.info("Sector detail cache hit for %s", normalized_key)
            return cached

        response = await run_in_threadpool(self._build_sector_detail_sync, normalized_key)
        self._sector_detail_cache.set(normalized_key, response)
        return response

    async def get_financial_summary(self, symbol: str) -> FinancialSummaryResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)

        cached = self._financial_summary_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Financial summary cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(
            self._get_financial_summary_sync,
            normalized_symbol,
        )
        self._financial_summary_cache.set(normalized_symbol, response)
        return response

    async def get_earnings_context(self, symbol: str) -> EarningsContextResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)

        cached = self._earnings_context_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Earnings context cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(
            self._get_earnings_context_sync,
            normalized_symbol,
        )
        self._earnings_context_cache.set(normalized_symbol, response)
        return response

    async def get_analyst_context(self, symbol: str) -> AnalystContextResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)

        cached = self._analyst_context_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Analyst context cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(
            self._get_analyst_context_sync,
            normalized_symbol,
        )
        self._analyst_context_cache.set(normalized_symbol, response)
        return response

    async def get_ticker_history(
        self,
        symbol: str,
        period: str,
        interval: str,
    ) -> TickerHistoryResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        normalized_period, normalized_interval = self._validate_history_period_interval(
            period=period,
            interval=interval,
        )

        cache_key = f"{normalized_symbol}:{normalized_period}:{normalized_interval}"
        cached = self._history_cache.get(cache_key)
        if cached is not None:
            self._logger.info("History cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_ticker_history_sync,
            normalized_symbol,
            normalized_period,
            normalized_interval,
        )
        self._history_cache.set(cache_key, response)
        return response

    def _normalize_and_validate_symbol(self, symbol: str) -> str:
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
    def _normalize_and_validate_mover_screen(screen: str) -> str:
        normalized_screen = screen.strip().lower()
        if normalized_screen not in ALLOWED_MOVER_SCREENS:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported market movers screen.",
                status_code=400,
                details={
                    "screen": screen,
                    "allowedScreens": sorted(ALLOWED_MOVER_SCREENS.keys()),
                },
            )
        return normalized_screen

    @staticmethod
    def _normalize_and_validate_mover_limit(limit: int) -> int:
        if limit < 1 or limit > MAX_MOVERS_LIMIT:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Market movers limit is outside the supported range.",
                status_code=400,
                details={
                    "limit": limit,
                    "minLimit": 1,
                    "maxLimit": MAX_MOVERS_LIMIT,
                },
            )
        return limit

    @staticmethod
    def _normalize_and_validate_sector_key(sector_key: str) -> str:
        normalized_key = sector_key.strip().lower()
        if normalized_key not in CURATED_SECTOR_KEYS:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported sector key.",
                status_code=400,
                details={
                    "sectorKey": sector_key,
                    "allowedSectorKeys": sorted(CURATED_SECTOR_KEYS),
                },
            )
        return normalized_key

    def _normalize_earnings_calendar_range(
        self,
        *,
        start: str | None,
        end: str | None,
    ) -> tuple[date, date]:
        normalized_start = self._parse_iso_date(value=start, field_name="start") if start else date.today()
        normalized_end = (
            self._parse_iso_date(value=end, field_name="end")
            if end
            else normalized_start + timedelta(days=7)
        )

        if normalized_end < normalized_start:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Earnings calendar end date cannot be earlier than start date.",
                status_code=400,
                details={
                    "start": normalized_start.isoformat(),
                    "end": normalized_end.isoformat(),
                },
            )

        return normalized_start, normalized_end

    @staticmethod
    def _parse_iso_date(*, value: str, field_name: str) -> date:
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Invalid earnings calendar date format.",
                status_code=400,
                details={
                    "field": field_name,
                    "value": value,
                    "expectedFormat": "YYYY-MM-DD",
                },
            ) from exc

    @staticmethod
    def _normalize_and_validate_earnings_calendar_limit(limit: int) -> int:
        if limit < 1 or limit > MAX_EARNINGS_CALENDAR_LIMIT:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Earnings calendar limit is outside the supported range.",
                status_code=400,
                details={
                    "limit": limit,
                    "minLimit": 1,
                    "maxLimit": MAX_EARNINGS_CALENDAR_LIMIT,
                },
            )
        return limit

    def _validate_history_period_interval(
        self,
        *,
        period: str,
        interval: str,
    ) -> tuple[str, str]:
        normalized_period = period.strip().lower()
        normalized_interval = interval.strip().lower()

        if normalized_period not in ALLOWED_HISTORY_PERIODS:
            raise ApiError(
                code="INVALID_PERIOD_INTERVAL",
                message="Invalid period/interval combination for history request.",
                status_code=400,
                details={
                    "period": period,
                    "interval": interval,
                    "allowedPeriods": sorted(ALLOWED_HISTORY_PERIODS),
                    "allowedIntervals": sorted(ALLOWED_HISTORY_INTERVALS),
                },
            )

        if normalized_interval not in ALLOWED_HISTORY_INTERVALS:
            raise ApiError(
                code="INVALID_PERIOD_INTERVAL",
                message="Invalid period/interval combination for history request.",
                status_code=400,
                details={
                    "period": period,
                    "interval": interval,
                    "allowedPeriods": sorted(ALLOWED_HISTORY_PERIODS),
                    "allowedIntervals": sorted(ALLOWED_HISTORY_INTERVALS),
                },
            )

        valid_periods = ALLOWED_HISTORY_PERIODS_BY_INTERVAL[normalized_interval]
        if normalized_period not in valid_periods:
            raise ApiError(
                code="INVALID_PERIOD_INTERVAL",
                message="Invalid period/interval combination for history request.",
                status_code=400,
                details={
                    "period": normalized_period,
                    "interval": normalized_interval,
                    "allowedPeriodsForInterval": sorted(valid_periods),
                },
            )

        return normalized_period, normalized_interval

    def _search_tickers_sync(self, query: str, limit: int) -> TickerSearchResponse:
        raw_quotes = self._fetch_search_quotes(query=query, limit=limit)
        results: list[TickerSearchResult] = []

        for quote in raw_quotes:
            result = self._map_search_result(quote)
            if result is None:
                continue
            results.append(result)
            if len(results) >= limit:
                break

        return TickerSearchResponse(query=query, results=results)

    def _get_market_movers_sync(self, screen: str, limit: int) -> MarketMoversResponse:
        provider_screen = ALLOWED_MOVER_SCREENS[screen]
        try:
            payload = yf.screen(provider_screen, count=limit)
        except Exception as exc:
            self._logger.exception("yfinance movers fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch market movers from market data provider.",
                status_code=502,
                details={"screen": screen},
            ) from exc

        quotes = self._extract_mover_quotes(payload)
        movers: list[MarketMover] = []
        skipped_rows = 0
        for quote in quotes:
            mover = self._map_market_mover(quote)
            if mover is None:
                skipped_rows += 1
                continue
            movers.append(mover)
            if len(movers) >= limit:
                break

        if not movers:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Market movers data is unavailable for the selected screen.",
                status_code=404,
                details={"screen": screen},
            )

        limitations: list[str] = []
        if skipped_rows > 0:
            limitations.append(
                f"{skipped_rows} market mover entries were omitted because provider fields were incomplete."
            )

        return MarketMoversResponse(
            screen=screen,
            marketScope=MARKET_SCOPE_US,
            asOf=datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            results=movers,
            dataLimitations=limitations,
        )

    @staticmethod
    def _extract_mover_quotes(payload: Any) -> list[dict[str, Any]]:
        if not isinstance(payload, Mapping):
            return []
        raw_quotes = payload.get("quotes")
        if not isinstance(raw_quotes, list):
            raw_quotes = payload.get("items")
        if not isinstance(raw_quotes, list):
            return []
        return [item for item in raw_quotes if isinstance(item, dict)]

    def _map_market_mover(self, quote: dict[str, Any]) -> MarketMover | None:
        raw_symbol = coerce_str(quote.get("symbol"))
        if raw_symbol is None:
            return None

        symbol = normalize_symbol(raw_symbol)
        if not is_valid_symbol(symbol):
            return None

        quote_type = first_non_null(
            coerce_str(quote.get("quoteType")),
            coerce_str(quote.get("quote_type")),
        )
        normalized_quote_type = quote_type.upper() if quote_type else None

        current_price = first_non_null(
            coerce_float(quote.get("regularMarketPrice")),
            coerce_float(quote.get("intradayprice")),
            coerce_float(quote.get("price")),
        )
        change = first_non_null(
            coerce_float(quote.get("regularMarketChange")),
            coerce_float(quote.get("change")),
        )
        percent_change = first_non_null(
            coerce_float(quote.get("regularMarketChangePercent")),
            coerce_float(quote.get("percentchange")),
            coerce_float(quote.get("percentChange")),
        )
        volume = first_non_null(
            coerce_int(quote.get("regularMarketVolume")),
            coerce_int(quote.get("dayvolume")),
            coerce_int(quote.get("volume")),
        )
        market_cap = first_non_null(
            coerce_float(quote.get("marketCap")),
            coerce_float(quote.get("intradaymarketcap")),
        )

        if all(
            value is None
            for value in (current_price, change, percent_change, volume, market_cap)
        ):
            return None

        return MarketMover(
            symbol=symbol,
            name=first_non_null(
                coerce_str(quote.get("shortName")),
                coerce_str(quote.get("longName")),
                coerce_str(quote.get("displayName")),
                symbol,
            ),
            exchange=first_non_null(
                coerce_str(quote.get("exchange")),
                coerce_str(quote.get("fullExchangeName")),
                coerce_str(quote.get("exchangeName")),
            ),
            quoteType=normalized_quote_type,
            currentPrice=current_price,
            change=change,
            percentChange=percent_change,
            volume=volume,
            marketCap=market_cap,
        )

    def _get_benchmark_funds_sync(self) -> BenchmarkFundsResponse:
        benchmark_funds: list[BenchmarkFund] = []
        top_level_limitations: list[str] = []

        for benchmark in CURATED_BENCHMARK_FUNDS:
            symbol = benchmark["symbol"]
            try:
                mapped_fund = self._build_benchmark_fund(
                    symbol=symbol,
                    benchmark_key=benchmark["benchmarkKey"],
                    benchmark_name=benchmark["benchmarkName"],
                    category=benchmark["category"],
                )
            except ApiError as exc:
                self._logger.warning("Skipping benchmark fund %s: %s", symbol, exc.message)
                top_level_limitations.append(
                    f"{symbol} benchmark fund data is unavailable from the data provider."
                )
                continue
            except Exception as exc:
                self._logger.warning("Unexpected benchmark fund failure for %s: %s", symbol, exc)
                top_level_limitations.append(
                    f"{symbol} benchmark fund data is unavailable due to a provider parsing error."
                )
                continue

            benchmark_funds.append(mapped_fund)

        if not benchmark_funds:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Benchmark fund data is unavailable.",
                status_code=404,
                details={"marketScope": MARKET_SCOPE_US},
            )

        omitted_count = len(CURATED_BENCHMARK_FUNDS) - len(benchmark_funds)
        if omitted_count > 0:
            top_level_limitations.insert(
                0,
                f"{omitted_count} benchmark funds were omitted because provider data was unavailable.",
            )

        return BenchmarkFundsResponse(
            asOf=datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            funds=benchmark_funds,
            dataLimitations=self._dedupe_preserve_order(top_level_limitations),
        )

    def _build_benchmark_fund(
        self,
        *,
        symbol: str,
        benchmark_key: str,
        benchmark_name: str,
        category: str,
    ) -> BenchmarkFund:
        try:
            ticker = yf.Ticker(symbol)
        except Exception as exc:
            self._logger.exception("yfinance benchmark ticker init failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to initialize benchmark fund from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        limitations: list[str] = []

        info: dict[str, Any] = {}
        fast_info: dict[str, Any] = {}
        funds_data: Any = None

        try:
            info = self._coerce_mapping(getattr(ticker, "info", {}))
        except Exception as exc:
            self._logger.warning("Benchmark info fetch failed for %s: %s", symbol, exc)
            limitations.append("Quote metadata is unavailable from the data provider.")

        try:
            fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
        except Exception as exc:
            self._logger.warning("Benchmark fast_info fetch failed for %s: %s", symbol, exc)
            limitations.append("Fast quote data is unavailable from the data provider.")

        try:
            funds_data = getattr(ticker, "funds_data", None)
        except Exception as exc:
            self._logger.warning("Benchmark funds_data fetch failed for %s: %s", symbol, exc)
            limitations.append("Fund profile details are unavailable from the data provider.")

        fund_overview: dict[str, Any] = {}
        fund_operations: Any = None
        raw_top_holdings: Any = None
        raw_sector_weightings: Any = None

        if funds_data is not None:
            fund_overview = self._safe_get_mapping_attr(
                funds_data,
                "fund_overview",
                symbol=symbol,
                limitations=limitations,
                failure_message="Fund overview details are unavailable from the data provider.",
            )
            fund_operations = self._safe_get_attr(
                funds_data,
                "fund_operations",
                symbol=symbol,
                limitations=limitations,
                failure_message="Fund operations data is unavailable from the data provider.",
            )
            raw_top_holdings = self._safe_get_attr(
                funds_data,
                "top_holdings",
                symbol=symbol,
                limitations=limitations,
                failure_message="Top holdings are unavailable from the data provider.",
            )
            raw_sector_weightings = self._safe_get_attr(
                funds_data,
                "sector_weightings",
                symbol=symbol,
                limitations=limitations,
                failure_message="Sector weights are unavailable from the data provider.",
            )

        current_price = first_non_null(
            coerce_float(fast_info.get("lastPrice")),
            coerce_float(info.get("currentPrice")),
            coerce_float(info.get("regularMarketPrice")),
        )
        previous_close = first_non_null(
            coerce_float(fast_info.get("previousClose")),
            coerce_float(info.get("previousClose")),
            coerce_float(info.get("regularMarketPreviousClose")),
        )
        day_change = first_non_null(
            coerce_float(info.get("regularMarketChange")),
            current_price - previous_close
            if current_price is not None and previous_close is not None
            else None,
        )
        day_change_percent = first_non_null(
            coerce_float(info.get("regularMarketChangePercent")),
            ((day_change / previous_close) * 100)
            if day_change is not None and previous_close not in (None, 0)
            else None,
        )

        top_holdings = self._map_benchmark_holdings(raw_top_holdings)
        sector_weights = self._map_benchmark_sector_weights(raw_sector_weightings)
        expense_ratio = first_non_null(
            self._coerce_positive_float(
                self._extract_fund_operation_value(
                    fund_operations,
                    row_label="Annual Report Expense Ratio",
                    symbol=symbol,
                )
            ),
            self._coerce_positive_float(coerce_float(info.get("annualReportExpenseRatio"))),
            self._coerce_percentage_basis_points(info.get("netExpenseRatio")),
        )
        net_assets = first_non_null(
            self._coerce_positive_float(coerce_float(info.get("totalAssets"))),
            self._coerce_positive_float(coerce_float(info.get("netAssets"))),
            self._coerce_positive_float(
                self._extract_fund_operation_value(
                    fund_operations,
                    row_label="Total Net Assets",
                    symbol=symbol,
                )
            ),
        )

        benchmark_fund = BenchmarkFund(
            symbol=symbol,
            benchmarkKey=benchmark_key,
            benchmarkName=benchmark_name,
            category=category,
            displayName=first_non_null(
                coerce_str(info.get("longName")),
                coerce_str(info.get("shortName")),
                coerce_str(info.get("displayName")),
                symbol,
            ),
            currentPrice=current_price,
            previousClose=previous_close,
            dayChange=day_change,
            dayChangePercent=day_change_percent,
            currency=first_non_null(
                coerce_str(info.get("currency")),
                coerce_str(fast_info.get("currency")),
            ),
            expenseRatio=expense_ratio,
            netAssets=net_assets,
            yield_=first_non_null(
                coerce_float(info.get("yield")),
                coerce_float(info.get("trailingAnnualDividendYield")),
            ),
            fundFamily=first_non_null(
                coerce_str(fund_overview.get("family")),
                coerce_str(info.get("fundFamily")),
            ),
            topHoldings=top_holdings,
            sectorWeights=sector_weights,
            dataLimitations=[],
        )

        if self._benchmark_fund_has_no_material_data(benchmark_fund):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Benchmark fund data is unavailable.",
                status_code=404,
                details={"symbol": symbol},
            )

        if benchmark_fund.currentPrice is None:
            limitations.append("Current price is unavailable from the data provider.")
        if benchmark_fund.expenseRatio is None:
            limitations.append("Expense ratio is unavailable from the data provider.")
        if benchmark_fund.netAssets is None:
            limitations.append("Net assets are unavailable from the data provider.")
        if benchmark_fund.fundFamily is None:
            limitations.append("Fund family is unavailable from the data provider.")
        if not benchmark_fund.topHoldings:
            limitations.append("Top holdings are unavailable from the data provider.")
        if not benchmark_fund.sectorWeights:
            limitations.append("Sector weights are unavailable from the data provider.")

        benchmark_fund.dataLimitations = self._dedupe_preserve_order(limitations)
        return benchmark_fund

    def _get_earnings_calendar_sync(
        self,
        start: date,
        end: date,
        limit: int,
        active_only: bool,
    ) -> EarningsCalendarResponse:
        try:
            calendars = yf.Calendars(start=start, end=end)
            raw_events = calendars.get_earnings_calendar(
                start=start,
                end=end,
                limit=limit,
                filter_most_active=active_only,
            )
        except Exception as exc:
            self._logger.exception("yfinance earnings calendar fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch earnings calendar from market data provider.",
                status_code=502,
                details={
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "limit": limit,
                    "activeOnly": active_only,
                },
            ) from exc

        if raw_events is None or getattr(raw_events, "empty", False):
            return EarningsCalendarResponse(
                start=start.isoformat(),
                end=end.isoformat(),
                limit=limit,
                activeOnly=active_only,
                events=[],
                dataLimitations=[],
            )

        events: list[EarningsCalendarEvent] = []
        skipped_rows = 0
        iterrows = getattr(raw_events, "iterrows", None)
        if not callable(iterrows):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings calendar data is unavailable.",
                status_code=404,
                details={"start": start.isoformat(), "end": end.isoformat()},
            )

        for index, row in iterrows():
            mapped_event = self._map_earnings_calendar_event(index=index, row=row)
            if mapped_event is None:
                skipped_rows += 1
                continue
            events.append(mapped_event)
            if len(events) >= limit:
                break

        if not events and skipped_rows > 0:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings calendar data is unavailable.",
                status_code=404,
                details={"start": start.isoformat(), "end": end.isoformat()},
            )

        limitations: list[str] = []
        if skipped_rows > 0:
            limitations.append(
                f"{skipped_rows} earnings calendar entries were omitted because provider fields were incomplete."
            )

        return EarningsCalendarResponse(
            start=start.isoformat(),
            end=end.isoformat(),
            limit=limit,
            activeOnly=active_only,
            events=events,
            dataLimitations=limitations,
        )

    def _get_sector_pulse_sync(self) -> SectorPulseResponse:
        sectors: list[SectorPulseItem] = []
        top_level_limitations: list[str] = []

        for sector_key in CURATED_SECTOR_KEYS:
            try:
                detail = self._get_or_build_sector_detail_sync(sector_key)
            except ApiError as exc:
                self._logger.warning("Skipping sector %s from pulse: %s", sector_key, exc.message)
                top_level_limitations.append(
                    f"{sector_key} sector data is unavailable from the data provider."
                )
                continue
            except Exception as exc:
                self._logger.warning("Unexpected sector pulse failure for %s: %s", sector_key, exc)
                top_level_limitations.append(
                    f"{sector_key} sector data is unavailable due to a provider parsing error."
                )
                continue

            sectors.append(
                SectorPulseItem(
                    key=detail.key,
                    name=detail.name,
                    symbol=detail.symbol,
                    overview=detail.overview,
                    topEtfs=detail.topEtfs[:MAX_SECTOR_PULSE_FUNDS],
                    topMutualFunds=detail.topMutualFunds[:MAX_SECTOR_PULSE_FUNDS],
                    topCompanies=detail.topCompanies[:MAX_SECTOR_PULSE_COMPANIES],
                    dataLimitations=detail.dataLimitations,
                )
            )

        if not sectors:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Sector pulse data is unavailable.",
                status_code=404,
                details={"marketScope": MARKET_SCOPE_US},
            )

        omitted_count = len(CURATED_SECTOR_KEYS) - len(sectors)
        if omitted_count > 0:
            top_level_limitations.insert(
                0,
                f"{omitted_count} sectors were omitted because provider data was unavailable.",
            )

        return SectorPulseResponse(
            asOf=datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            sectors=sectors,
            dataLimitations=self._dedupe_preserve_order(top_level_limitations),
        )

    def _get_or_build_sector_detail_sync(self, sector_key: str) -> SectorDetailResponse:
        cached = self._sector_detail_cache.get(sector_key)
        if cached is not None:
            return cached

        detail = self._build_sector_detail_sync(sector_key)
        self._sector_detail_cache.set(sector_key, detail)
        return detail

    def _build_sector_detail_sync(self, sector_key: str) -> SectorDetailResponse:
        try:
            sector = yf.Sector(sector_key)
        except Exception as exc:
            self._logger.exception("yfinance sector init failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to initialize sector data from market data provider.",
                status_code=502,
                details={"sectorKey": sector_key},
            ) from exc

        try:
            name = coerce_str(sector.name)
            symbol = coerce_str(sector.symbol)
            overview = self._map_sector_overview(sector.overview)
            top_etfs = self._map_sector_fund_references(sector.top_etfs)
            top_mutual_funds = self._map_sector_fund_references(sector.top_mutual_funds)
            top_companies = self._map_sector_company_references(sector.top_companies)
            industries = self._map_sector_industries(sector.industries)
        except Exception as exc:
            self._logger.exception("yfinance sector fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch sector data from market data provider.",
                status_code=502,
                details={"sectorKey": sector_key},
            ) from exc

        detail = SectorDetailResponse(
            key=sector_key,
            name=name,
            symbol=symbol,
            overview=overview,
            topEtfs=top_etfs,
            topMutualFunds=top_mutual_funds,
            topCompanies=top_companies,
            industries=industries,
            dataLimitations=[],
        )

        if self._sector_detail_has_no_material_data(detail):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Sector data is unavailable.",
                status_code=404,
                details={"sectorKey": sector_key},
            )

        limitations: list[str] = []
        if detail.name is None:
            limitations.append("Sector name is unavailable from the data provider.")
        if detail.symbol is None:
            limitations.append("Sector symbol is unavailable from the data provider.")
        if detail.overview.description is None:
            limitations.append("Sector description is unavailable from the data provider.")
        if not detail.topEtfs:
            limitations.append("Top ETFs are unavailable from the data provider.")
        if not detail.topMutualFunds:
            limitations.append("Top mutual funds are unavailable from the data provider.")
        if not detail.topCompanies:
            limitations.append("Top companies are unavailable from the data provider.")
        if not detail.industries:
            limitations.append("Industry breakdown is unavailable from the data provider.")

        detail.dataLimitations = self._dedupe_preserve_order(limitations)
        return detail

    def _map_benchmark_holdings(self, payload: Any) -> list[BenchmarkHolding]:
        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return []

        holdings: list[BenchmarkHolding] = []
        for index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            symbol = normalize_symbol(
                first_non_null(
                    coerce_str(index),
                    coerce_str(row_mapping.get("Symbol")),
                )
                or ""
            )
            if not symbol:
                continue

            name = coerce_str(row_mapping.get("Name"))
            holding_percent = coerce_float(row_mapping.get("Holding Percent"))
            if name is None and holding_percent is None:
                continue

            holdings.append(
                BenchmarkHolding(
                    symbol=symbol,
                    name=name,
                    holdingPercent=holding_percent,
                )
            )
            if len(holdings) >= MAX_BENCHMARK_HOLDINGS:
                break

        return holdings

    def _map_benchmark_sector_weights(self, payload: Any) -> list[BenchmarkSectorWeight]:
        raw_mapping = self._coerce_mapping(payload)
        if not raw_mapping:
            return []

        sector_weights: list[BenchmarkSectorWeight] = []
        for raw_sector, raw_weight in sorted(
            raw_mapping.items(),
            key=lambda item: coerce_float(item[1]) or float("-inf"),
            reverse=True,
        ):
            sector = self._format_sector_name(coerce_str(raw_sector))
            weight = coerce_float(raw_weight)
            if sector is None or weight is None:
                continue

            sector_weights.append(BenchmarkSectorWeight(sector=sector, weight=weight))
            if len(sector_weights) >= MAX_BENCHMARK_SECTOR_WEIGHTS:
                break

        return sector_weights

    def _map_sector_overview(self, payload: Any) -> SectorOverview:
        overview_mapping = self._coerce_mapping(payload)
        return SectorOverview(
            companiesCount=self._coerce_non_negative_int(
                first_non_null(
                    overview_mapping.get("companies_count"),
                    overview_mapping.get("companiesCount"),
                )
            ),
            marketCap=self._coerce_finite_float(
                first_non_null(
                    overview_mapping.get("market_cap"),
                    overview_mapping.get("marketCap"),
                )
            ),
            messageBoardId=first_non_null(
                coerce_str(overview_mapping.get("message_board_id")),
                coerce_str(overview_mapping.get("messageBoardId")),
            ),
            description=coerce_str(overview_mapping.get("description")),
            industriesCount=self._coerce_non_negative_int(
                first_non_null(
                    overview_mapping.get("industries_count"),
                    overview_mapping.get("industriesCount"),
                )
            ),
            marketWeight=self._coerce_finite_float(
                first_non_null(
                    overview_mapping.get("market_weight"),
                    overview_mapping.get("marketWeight"),
                )
            ),
            employeeCount=self._coerce_non_negative_int(
                first_non_null(
                    overview_mapping.get("employee_count"),
                    overview_mapping.get("employeeCount"),
                )
            ),
        )

    def _map_sector_fund_references(
        self,
        payload: Any,
    ) -> list[SectorFundReference]:
        raw_mapping = self._coerce_mapping(payload)
        if not raw_mapping:
            return []

        funds: list[SectorFundReference] = []
        for raw_symbol, raw_name in raw_mapping.items():
            symbol = normalize_symbol(coerce_str(raw_symbol) or "")
            if not symbol:
                continue
            name = coerce_str(raw_name)
            if name is None:
                continue
            funds.append(
                SectorFundReference(
                    symbol=symbol,
                    name=name,
                )
            )
        return funds

    def _map_sector_company_references(
        self,
        payload: Any,
    ) -> list[SectorCompanyReference]:
        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return []

        companies: list[SectorCompanyReference] = []
        for index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            symbol = normalize_symbol(
                first_non_null(
                    coerce_str(index),
                    coerce_str(row_mapping.get("symbol")),
                )
                or ""
            )
            if not symbol:
                continue

            companies.append(
                SectorCompanyReference(
                    symbol=symbol,
                    name=coerce_str(row_mapping.get("name")),
                    rating=coerce_str(row_mapping.get("rating")),
                    marketWeight=self._coerce_finite_float(
                        first_non_null(
                            row_mapping.get("market weight"),
                            row_mapping.get("marketWeight"),
                        )
                    ),
                )
            )
        return companies

    def _map_sector_industries(
        self,
        payload: Any,
    ) -> list[SectorIndustryReference]:
        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return []

        industries: list[SectorIndustryReference] = []
        for index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            key = coerce_str(index)
            if key is None:
                continue
            industries.append(
                SectorIndustryReference(
                    key=key,
                    name=coerce_str(row_mapping.get("name")),
                    symbol=coerce_str(row_mapping.get("symbol")),
                    marketWeight=self._coerce_finite_float(
                        first_non_null(
                            row_mapping.get("market weight"),
                            row_mapping.get("marketWeight"),
                        )
                    ),
                )
            )
        return industries

    def _map_earnings_calendar_event(self, *, index: Any, row: Any) -> EarningsCalendarEvent | None:
        row_mapping = self._coerce_mapping(row)

        raw_symbol = first_non_null(
            coerce_str(index),
            coerce_str(row_mapping.get("Symbol")),
        )
        if raw_symbol is None:
            return None

        symbol = normalize_symbol(raw_symbol)
        if not is_valid_symbol(symbol):
            return None

        earnings_date = self._coerce_calendar_timestamp(
            first_non_null(
                row_mapping.get("Event Start Date"),
                row_mapping.get("Date"),
                row_mapping.get("Earnings Date"),
                row_mapping.get("startdatetime"),
            )
        )
        if earnings_date is None:
            return None

        return EarningsCalendarEvent(
            symbol=symbol,
            companyName=first_non_null(
                coerce_str(row_mapping.get("Company")),
                coerce_str(row_mapping.get("Company Name")),
            ),
            earningsDate=earnings_date,
            reportTime=first_non_null(
                coerce_str(row_mapping.get("Timing")),
                coerce_str(row_mapping.get("Report Time")),
                coerce_str(row_mapping.get("Event Name")),
            ),
            epsEstimate=first_non_null(
                self._coerce_finite_float(row_mapping.get("EPS Estimate")),
                self._coerce_finite_float(row_mapping.get("epsestimate")),
            ),
            reportedEps=first_non_null(
                self._coerce_finite_float(row_mapping.get("Reported EPS")),
                self._coerce_finite_float(row_mapping.get("epsactual")),
            ),
            surprisePercent=first_non_null(
                self._coerce_finite_float(row_mapping.get("Surprise(%)")),
                self._coerce_finite_float(row_mapping.get("Surprise (%)")),
                self._coerce_finite_float(row_mapping.get("epssurprisepct")),
            ),
            marketCap=first_non_null(
                self._coerce_finite_float(row_mapping.get("Marketcap")),
                self._coerce_finite_float(row_mapping.get("Market Cap (Intraday)")),
                self._coerce_finite_float(row_mapping.get("intradaymarketcap")),
            ),
        )

    def _extract_fund_operation_value(
        self,
        payload: Any,
        *,
        row_label: str,
        symbol: str,
    ) -> float | None:
        loc = getattr(payload, "loc", None)
        if loc is None:
            return None

        try:
            row = loc[row_label]
        except Exception:
            return None

        row_mapping = self._coerce_mapping(row)
        if row_mapping:
            return coerce_float(row_mapping.get(symbol))
        return coerce_float(row)

    @staticmethod
    def _coerce_positive_float(value: Any) -> float | None:
        coerced_value = coerce_float(value)
        if coerced_value is None:
            return None
        if not isfinite(coerced_value):
            return None
        if coerced_value <= 0:
            return None
        return coerced_value

    @staticmethod
    def _coerce_percentage_basis_points(value: Any) -> float | None:
        coerced_value = coerce_float(value)
        if coerced_value is None:
            return None
        if not isfinite(coerced_value):
            return None
        if coerced_value <= 0:
            return None
        return coerced_value / 100

    @staticmethod
    def _coerce_calendar_timestamp(value: Any) -> str | None:
        dt_value: datetime | None = None

        if isinstance(value, datetime):
            dt_value = value
        elif isinstance(value, date):
            dt_value = datetime.combine(value, datetime.min.time())
        elif hasattr(value, "to_pydatetime"):
            try:
                parsed = value.to_pydatetime()
            except Exception:
                parsed = None
            if isinstance(parsed, datetime):
                dt_value = parsed
        elif isinstance(value, str):
            dt_value = YFinanceService._parse_iso_timestamp(value)
            if dt_value is None:
                try:
                    dt_value = datetime.fromisoformat(value)
                except ValueError:
                    return None

        if dt_value is None:
            return None

        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        else:
            dt_value = dt_value.astimezone(timezone.utc)

        return dt_value.isoformat().replace("+00:00", "Z")

    def _safe_get_mapping_attr(
        self,
        payload: Any,
        attribute: str,
        *,
        symbol: str,
        limitations: list[str],
        failure_message: str,
    ) -> dict[str, Any]:
        value = self._safe_get_attr(
            payload,
            attribute,
            symbol=symbol,
            limitations=limitations,
            failure_message=failure_message,
        )
        return self._coerce_mapping(value)

    def _safe_get_attr(
        self,
        payload: Any,
        attribute: str,
        *,
        symbol: str,
        limitations: list[str],
        failure_message: str,
    ) -> Any:
        try:
            return getattr(payload, attribute, None)
        except Exception as exc:
            self._logger.warning("Benchmark %s fetch failed for %s: %s", attribute, symbol, exc)
            limitations.append(failure_message)
            return None

    @staticmethod
    def _format_sector_name(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.replace("_", " ").replace("-", " ").strip()
        if not normalized:
            return None
        return " ".join(part.capitalize() for part in normalized.split())

    @staticmethod
    def _benchmark_fund_has_no_material_data(fund: BenchmarkFund) -> bool:
        return (
            fund.currentPrice is None
            and fund.previousClose is None
            and fund.expenseRatio is None
            and fund.netAssets is None
            and fund.fundFamily is None
            and not fund.topHoldings
            and not fund.sectorWeights
        )

    @staticmethod
    def _sector_overview_has_material_data(overview: SectorOverview) -> bool:
        return any(
            value is not None
            for value in (
                overview.companiesCount,
                overview.marketCap,
                overview.messageBoardId,
                overview.description,
                overview.industriesCount,
                overview.marketWeight,
                overview.employeeCount,
            )
        )

    def _sector_detail_has_no_material_data(self, detail: SectorDetailResponse) -> bool:
        return (
            detail.name is None
            and detail.symbol is None
            and not self._sector_overview_has_material_data(detail.overview)
            and not detail.topEtfs
            and not detail.topMutualFunds
            and not detail.topCompanies
            and not detail.industries
        )

    def _fetch_search_quotes(self, *, query: str, limit: int) -> list[dict[str, Any]]:
        try:
            if hasattr(yf, "Search"):
                search = yf.Search(query, max_results=limit, news_count=0)
                quotes = getattr(search, "quotes", [])
            else:
                payload = yf.search(query, max_results=limit, news_count=0)
                quotes = payload.get("quotes", []) if isinstance(payload, dict) else []
        except Exception as exc:
            self._logger.exception("yfinance search failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to search symbols from market data provider.",
                status_code=502,
            ) from exc

        if not isinstance(quotes, list):
            return []
        return [item for item in quotes if isinstance(item, dict)]

    def _map_search_result(self, quote: dict[str, Any]) -> TickerSearchResult | None:
        raw_symbol = coerce_str(quote.get("symbol"))
        if raw_symbol is None:
            return None

        symbol = normalize_symbol(raw_symbol)
        if not is_valid_symbol(symbol):
            return None

        quote_type = coerce_str(quote.get("quoteType"))
        if quote_type is None:
            return None

        normalized_quote_type = quote_type.upper()
        if normalized_quote_type not in ALLOWED_QUOTE_TYPES:
            return None

        name = first_non_null(
            coerce_str(quote.get("shortname")),
            coerce_str(quote.get("longname")),
            symbol,
        )
        exchange = first_non_null(
            coerce_str(quote.get("exchange")),
            coerce_str(quote.get("exchDisp")),
        )

        return TickerSearchResult(
            symbol=symbol,
            name=name,
            exchange=exchange,
            quoteType=normalized_quote_type,
        )

    def _get_ticker_overview_sync(self, symbol: str) -> TickerOverviewResponse:
        try:
            ticker = yf.Ticker(symbol)
            info = self._coerce_mapping(getattr(ticker, "info", {}))
            fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
        except Exception as exc:
            self._logger.exception("yfinance overview fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch ticker overview from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        if not info and not fast_info:
            raise ApiError(
                code="NOT_FOUND",
                message="Ticker not found.",
                status_code=404,
                details={"symbol": symbol},
            )

        overview = self._build_overview(symbol=symbol, info=info, fast_info=fast_info)
        if self._overview_has_no_material_data(overview):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Ticker data is unavailable.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations = self._build_overview_limitations(overview)
        return TickerOverviewResponse(
            symbol=symbol,
            overview=overview,
            dataLimitations=limitations,
        )

    def _get_ticker_news_sync(self, symbol: str, limit: int) -> TickerNewsResponse:
        try:
            ticker = yf.Ticker(symbol)
            raw_news = ticker.get_news()
        except Exception as exc:
            self._logger.exception("yfinance news fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch ticker news from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        if not isinstance(raw_news, list):
            raw_news = []

        news_items: list[TickerNewsItem] = []
        for item in raw_news:
            if not isinstance(item, dict):
                continue
            mapped = self._map_news_item(item)
            if mapped is None:
                continue
            news_items.append(mapped)
            if len(news_items) >= limit:
                break

        # Distinguish "no news" for a valid ticker from an unusable symbol payload.
        if not news_items:
            try:
                info = self._coerce_mapping(getattr(ticker, "info", {}))
                fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
            except Exception as exc:
                self._logger.exception("yfinance metadata fetch failed", exc_info=exc)
                raise ApiError(
                    code="PROVIDER_ERROR",
                    message="Failed to validate ticker metadata from market data provider.",
                    status_code=502,
                    details={"symbol": symbol},
                ) from exc

            if not info and not fast_info:
                raise ApiError(
                    code="DATA_UNAVAILABLE",
                    message="Ticker data is unavailable.",
                    status_code=404,
                    details={"symbol": symbol},
                )

        limitations = self._build_news_limitations(news_items)
        return TickerNewsResponse(
            symbol=symbol,
            news=news_items,
            dataLimitations=limitations,
        )

    def _get_financial_summary_sync(self, symbol: str) -> FinancialSummaryResponse:
        try:
            ticker = yf.Ticker(symbol)
            info = self._coerce_mapping(getattr(ticker, "info", {}))
            fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
        except Exception as exc:
            self._logger.exception("yfinance financial fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch financial summary from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        summary = self._build_financial_summary(info=info, fast_info=fast_info)
        if self._financial_summary_has_no_material_data(summary):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Financial summary is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations = self._build_financial_limitations(summary)
        return FinancialSummaryResponse(
            symbol=symbol,
            financialSummary=summary,
            dataLimitations=limitations,
        )

    def _get_earnings_context_sync(self, symbol: str) -> EarningsContextResponse:
        try:
            ticker = yf.Ticker(symbol)
            info = self._coerce_mapping(getattr(ticker, "info", {}))
        except Exception as exc:
            self._logger.exception("yfinance earnings fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch earnings context from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        limitations: list[str] = []
        data_sources: list[str] = []
        date_candidates: list[str] = []

        try:
            raw_earnings_dates = ticker.get_earnings_dates(limit=EARNINGS_DATES_LIMIT)
            parsed_dates = self._extract_earnings_dates(raw_earnings_dates)
            if parsed_dates:
                date_candidates.extend(parsed_dates)
                data_sources.append("earnings_dates")
        except ImportError:
            limitations.append(
                "Detailed earnings-date history is unavailable because optional parsers are missing."
            )
        except Exception as exc:
            self._logger.warning(
                "yfinance earnings_dates fetch failed for %s: %s",
                symbol,
                exc,
            )
            limitations.append("Detailed earnings-date history is unavailable from the data provider.")

        calendar_data: dict[str, Any] = {}
        try:
            calendar_data = self._coerce_mapping(ticker.get_calendar())
            if calendar_data:
                data_sources.append("calendar")
        except Exception as exc:
            self._logger.warning("yfinance calendar fetch failed for %s: %s", symbol, exc)
            limitations.append("Earnings calendar details are unavailable from the data provider.")

        if info:
            data_sources.append("info")

        date_candidates.extend(self._extract_calendar_earnings_dates(calendar_data))
        info_earnings_date = coerce_datetime_string(info.get("earningsDate"))
        if info_earnings_date is not None:
            date_candidates.append(info_earnings_date)

        normalized_candidates = self._dedupe_preserve_order(date_candidates)
        next_earnings_date = normalized_candidates[0] if normalized_candidates else None

        earnings_context = EarningsContext(
            next_earnings_date=next_earnings_date,
            earnings_date_candidates=normalized_candidates,
            eps_estimate_low=first_non_null(
                coerce_float(calendar_data.get("Earnings Low")),
                coerce_float(info.get("epsLow")),
            ),
            eps_estimate_avg=coerce_float(calendar_data.get("Earnings Average")),
            eps_estimate_high=first_non_null(
                coerce_float(calendar_data.get("Earnings High")),
                coerce_float(info.get("epsHigh")),
            ),
            revenue_estimate_low=coerce_float(calendar_data.get("Revenue Low")),
            revenue_estimate_avg=coerce_float(calendar_data.get("Revenue Average")),
            revenue_estimate_high=coerce_float(calendar_data.get("Revenue High")),
            data_sources=self._dedupe_preserve_order(data_sources),
        )

        if self._earnings_context_has_no_material_data(earnings_context):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings context is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations.extend(self._build_earnings_limitations(earnings_context))
        return EarningsContextResponse(
            symbol=symbol,
            earningsContext=earnings_context,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_analyst_context_sync(self, symbol: str) -> AnalystContextResponse:
        try:
            ticker = yf.Ticker(symbol)
            info = self._coerce_mapping(getattr(ticker, "info", {}))
        except Exception as exc:
            self._logger.exception("yfinance analyst fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch analyst context from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        limitations: list[str] = []

        price_targets: dict[str, Any] = {}
        try:
            price_targets = self._coerce_mapping(ticker.get_analyst_price_targets())
        except Exception as exc:
            self._logger.warning("yfinance analyst_price_targets fetch failed for %s: %s", symbol, exc)
            limitations.append("Analyst price targets are unavailable from the data provider.")

        recommendation_snapshot = AnalystRecommendationSnapshot()
        recommendation_populated = False
        try:
            raw_recommendations = ticker.get_recommendations_summary()
            recommendation_snapshot = self._extract_recommendation_snapshot(raw_recommendations)
            recommendation_populated = self._recommendation_has_material_data(recommendation_snapshot)
        except Exception as exc:
            self._logger.warning(
                "yfinance recommendations_summary fetch failed for %s: %s",
                symbol,
                exc,
            )
            limitations.append("Analyst recommendation summary is unavailable from the data provider.")

        recent_actions: list[AnalystActionEvent] = []
        try:
            raw_actions = ticker.get_upgrades_downgrades()
            recent_actions = self._extract_recent_analyst_actions(raw_actions)
        except Exception as exc:
            self._logger.warning("yfinance upgrades_downgrades fetch failed for %s: %s", symbol, exc)
            limitations.append("Recent analyst action history is unavailable from the data provider.")

        analyst_context = AnalystContext(
            current_price_target=coerce_float(price_targets.get("current")),
            target_low=first_non_null(
                coerce_float(price_targets.get("low")),
                coerce_float(info.get("targetLowPrice")),
            ),
            target_high=first_non_null(
                coerce_float(price_targets.get("high")),
                coerce_float(info.get("targetHighPrice")),
            ),
            target_mean=first_non_null(
                coerce_float(price_targets.get("mean")),
                coerce_float(info.get("targetMeanPrice")),
            ),
            target_median=first_non_null(
                coerce_float(price_targets.get("median")),
                coerce_float(info.get("targetMedianPrice")),
            ),
            recommendation_summary=recommendation_snapshot,
            recent_actions=recent_actions,
            recent_action_count=len(recent_actions),
            recent_action_window_days=ANALYST_ACTION_WINDOW_DAYS,
        )

        if self._analyst_context_has_no_material_data(analyst_context):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Analyst context is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations.extend(
            self._build_analyst_limitations(
                analyst_context=analyst_context,
                recommendation_has_data=recommendation_populated,
            )
        )
        return AnalystContextResponse(
            symbol=symbol,
            analystContext=analyst_context,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_ticker_history_sync(
        self,
        symbol: str,
        period: str,
        interval: str,
    ) -> TickerHistoryResponse:
        try:
            ticker = yf.Ticker(symbol)
            history = ticker.history(
                period=period,
                interval=interval,
                auto_adjust=False,
            )
        except Exception as exc:
            self._logger.exception("yfinance history fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch ticker history from market data provider.",
                status_code=502,
                details={"symbol": symbol, "period": period, "interval": interval},
            ) from exc

        bars = self._map_history_rows(history)
        if not bars:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Ticker history is unavailable for the selected period and interval.",
                status_code=404,
                details={"symbol": symbol, "period": period, "interval": interval},
            )

        return TickerHistoryResponse(
            symbol=symbol,
            period=period,
            interval=interval,
            bars=bars,
        )

    def _map_history_rows(self, history: Any) -> list[PriceBar]:
        if history is None:
            return []

        if getattr(history, "empty", False):
            return []

        iterrows = getattr(history, "iterrows", None)
        if not callable(iterrows):
            return []

        bars: list[PriceBar] = []
        for index, row in iterrows():
            row_mapping = self._coerce_mapping(row)

            open_price = self._coerce_finite_float(row_mapping.get("Open"))
            high_price = self._coerce_finite_float(row_mapping.get("High"))
            low_price = self._coerce_finite_float(row_mapping.get("Low"))
            close_price = self._coerce_finite_float(row_mapping.get("Close"))
            if (
                open_price is None
                or high_price is None
                or low_price is None
                or close_price is None
            ):
                continue

            timestamp = self._coerce_history_timestamp(index)
            if timestamp is None:
                continue

            bars.append(
                PriceBar(
                    timestamp=timestamp,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    adj_close=first_non_null(
                        self._coerce_finite_float(row_mapping.get("Adj Close")),
                        self._coerce_finite_float(row_mapping.get("AdjClose")),
                    ),
                    volume=self._coerce_non_negative_int(row_mapping.get("Volume")),
                )
            )

        bars.sort(key=lambda bar: bar.timestamp)
        return bars

    @staticmethod
    def _coerce_history_timestamp(value: Any) -> str | None:
        dt_value: datetime | None = None

        if isinstance(value, datetime):
            dt_value = value
        elif hasattr(value, "to_pydatetime"):
            try:
                parsed = value.to_pydatetime()
            except Exception:
                parsed = None
            if isinstance(parsed, datetime):
                dt_value = parsed
        elif isinstance(value, (int, float)):
            try:
                dt_value = datetime.fromtimestamp(value, tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                dt_value = None

        if dt_value is None:
            return None

        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        else:
            dt_value = dt_value.astimezone(timezone.utc)

        return dt_value.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _coerce_finite_float(value: Any) -> float | None:
        coerced_value = coerce_float(value)
        if coerced_value is None:
            return None
        if not isfinite(coerced_value):
            return None
        return coerced_value

    @staticmethod
    def _coerce_non_negative_int(value: Any) -> int | None:
        coerced_value = coerce_int(value)
        if coerced_value is None:
            return None
        if coerced_value < 0:
            return None
        return coerced_value

    @staticmethod
    def _coerce_mapping(payload: Any) -> dict[str, Any]:
        if payload is None:
            return {}
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, Mapping):
            return dict(payload.items())
        if hasattr(payload, "items"):
            try:
                return dict(payload.items())
            except Exception:
                return {}
        try:
            return dict(payload)
        except Exception:
            return {}

    def _build_overview(
        self,
        *,
        symbol: str,
        info: dict[str, Any],
        fast_info: dict[str, Any],
    ) -> TickerOverview:
        raw_quote_type = first_non_null(
            coerce_str(info.get("quoteType")),
            coerce_str(fast_info.get("quoteType")),
            coerce_str(info.get("typeDisp")),
        )
        quote_type = raw_quote_type.upper() if raw_quote_type else None

        is_etf = quote_type == "ETF" if quote_type is not None else coerce_bool(info.get("isEtf"))
        if is_etf is None:
            is_etf = coerce_str(info.get("fundFamily")) is not None

        return TickerOverview(
            display_name=first_non_null(
                coerce_str(info.get("longName")),
                coerce_str(info.get("shortName")),
                coerce_str(info.get("displayName")),
                symbol,
            ),
            quote_type=quote_type,
            exchange=first_non_null(
                coerce_str(info.get("exchange")),
                coerce_str(info.get("fullExchangeName")),
                coerce_str(fast_info.get("exchange")),
            ),
            currency=first_non_null(
                coerce_str(info.get("currency")),
                coerce_str(fast_info.get("currency")),
            ),
            sector=coerce_str(info.get("sector")),
            industry=coerce_str(info.get("industry")),
            website=coerce_str(info.get("website")),
            summary=coerce_str(info.get("longBusinessSummary")),
            current_price=first_non_null(
                coerce_float(fast_info.get("lastPrice")),
                coerce_float(info.get("currentPrice")),
                coerce_float(info.get("regularMarketPrice")),
            ),
            previous_close=first_non_null(
                coerce_float(fast_info.get("previousClose")),
                coerce_float(info.get("previousClose")),
                coerce_float(info.get("regularMarketPreviousClose")),
            ),
            open_price=first_non_null(
                coerce_float(fast_info.get("open")),
                coerce_float(info.get("open")),
                coerce_float(info.get("regularMarketOpen")),
            ),
            day_low=first_non_null(
                coerce_float(fast_info.get("dayLow")),
                coerce_float(info.get("dayLow")),
                coerce_float(info.get("regularMarketDayLow")),
            ),
            day_high=first_non_null(
                coerce_float(fast_info.get("dayHigh")),
                coerce_float(info.get("dayHigh")),
                coerce_float(info.get("regularMarketDayHigh")),
            ),
            fifty_two_week_low=first_non_null(
                coerce_float(fast_info.get("yearLow")),
                coerce_float(info.get("fiftyTwoWeekLow")),
            ),
            fifty_two_week_high=first_non_null(
                coerce_float(fast_info.get("yearHigh")),
                coerce_float(info.get("fiftyTwoWeekHigh")),
            ),
            volume=first_non_null(
                coerce_int(fast_info.get("lastVolume")),
                coerce_int(info.get("volume")),
                coerce_int(info.get("regularMarketVolume")),
            ),
            average_volume=first_non_null(
                coerce_int(fast_info.get("tenDayAverageVolume")),
                coerce_int(info.get("averageVolume")),
                coerce_int(info.get("averageDailyVolume10Day")),
            ),
            market_cap=first_non_null(
                coerce_float(fast_info.get("marketCap")),
                coerce_float(info.get("marketCap")),
            ),
            trailing_pe=coerce_float(info.get("trailingPE")),
            forward_pe=coerce_float(info.get("forwardPE")),
            dividend_yield=coerce_float(info.get("dividendYield")),
            beta=coerce_float(info.get("beta")),
            shares_outstanding=first_non_null(
                coerce_int(fast_info.get("shares")),
                coerce_int(info.get("sharesOutstanding")),
            ),
            analyst_target_mean=coerce_float(info.get("targetMeanPrice")),
            earnings_date=coerce_datetime_string(info.get("earningsDate")),
            is_etf=is_etf,
        )

    def _build_financial_summary(
        self,
        *,
        info: dict[str, Any],
        fast_info: dict[str, Any],
    ) -> FinancialSummary:
        return FinancialSummary(
            revenue_ttm=coerce_float(info.get("totalRevenue")),
            net_income_ttm=first_non_null(
                coerce_float(info.get("netIncomeToCommon")),
                coerce_float(info.get("netIncome")),
            ),
            ebitda=coerce_float(info.get("ebitda")),
            gross_margins=coerce_float(info.get("grossMargins")),
            operating_margins=coerce_float(info.get("operatingMargins")),
            profit_margins=coerce_float(info.get("profitMargins")),
            free_cash_flow=first_non_null(
                coerce_float(info.get("freeCashflow")),
                coerce_float(info.get("freeCashFlow")),
            ),
            total_cash=first_non_null(
                coerce_float(info.get("totalCash")),
                coerce_float(fast_info.get("totalCash")),
            ),
            total_debt=coerce_float(info.get("totalDebt")),
            debt_to_equity=coerce_float(info.get("debtToEquity")),
            return_on_equity=coerce_float(info.get("returnOnEquity")),
            return_on_assets=coerce_float(info.get("returnOnAssets")),
        )

    def _map_news_item(self, item: dict[str, Any]) -> TickerNewsItem | None:
        content = item.get("content") if isinstance(item.get("content"), dict) else {}

        title = first_non_null(
            coerce_str(item.get("title")),
            coerce_str(content.get("title")),
            coerce_str(content.get("headline")),
        )
        publisher = first_non_null(
            coerce_str(item.get("publisher")),
            self._extract_provider_name(item.get("provider")),
            self._extract_provider_name(content.get("provider")),
        )
        link = first_non_null(
            coerce_str(item.get("link")),
            self._extract_url(item.get("canonicalUrl")),
            self._extract_url(content.get("canonicalUrl")),
            self._extract_url(content.get("clickThroughUrl")),
            self._extract_url(item.get("url")),
        )
        published_at = first_non_null(
            coerce_datetime_string(item.get("published_at")),
            coerce_datetime_string(item.get("providerPublishTime")),
            coerce_datetime_string(item.get("pubDate")),
            coerce_datetime_string(content.get("pubDate")),
            coerce_datetime_string(content.get("displayTime")),
        )
        summary = first_non_null(
            coerce_str(item.get("summary")),
            coerce_str(content.get("summary")),
            coerce_str(item.get("description")),
            coerce_str(content.get("description")),
        )
        source_type = first_non_null(
            coerce_str(item.get("source_type")),
            coerce_str(item.get("type")),
            coerce_str(content.get("contentType")),
            coerce_str(content.get("type")),
        )

        if title is None and link is None and summary is None:
            return None

        return TickerNewsItem(
            title=title,
            publisher=publisher,
            link=link,
            published_at=published_at,
            summary=summary,
            source_type=source_type,
        )

    @staticmethod
    def _extract_url(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return coerce_str(value)
        if isinstance(value, Mapping):
            return first_non_null(
                coerce_str(value.get("url")),
                coerce_str(value.get("link")),
                coerce_str(value.get("rawUrl")),
            )
        return None

    @staticmethod
    def _extract_provider_name(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return coerce_str(value)
        if isinstance(value, Mapping):
            return first_non_null(
                coerce_str(value.get("displayName")),
                coerce_str(value.get("name")),
            )
        return None

    def _extract_earnings_dates(self, payload: Any) -> list[str]:
        parsed_dates: list[str] = []

        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return parsed_dates

        for row_index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            row_date = first_non_null(
                coerce_datetime_string(row_mapping.get("Earnings Date")),
                coerce_datetime_string(row_mapping.get("Date")),
                coerce_datetime_string(row_mapping.get("EarningsDate")),
                coerce_datetime_string(row_index),
            )
            if row_date is None:
                continue
            parsed_dates.append(row_date)
            if len(parsed_dates) >= EARNINGS_DATES_LIMIT:
                break

        return self._dedupe_preserve_order(parsed_dates)

    @staticmethod
    def _extract_calendar_earnings_dates(calendar_data: dict[str, Any]) -> list[str]:
        raw_dates = calendar_data.get("Earnings Date")
        if raw_dates is None:
            return []

        values = raw_dates if isinstance(raw_dates, list) else [raw_dates]
        parsed_dates = [coerce_datetime_string(value) for value in values]
        return [value for value in parsed_dates if value is not None]

    def _extract_recommendation_snapshot(self, payload: Any) -> AnalystRecommendationSnapshot:
        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return AnalystRecommendationSnapshot()

        candidates: list[dict[str, Any]] = []
        for _index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            if row_mapping:
                candidates.append(row_mapping)

        if not candidates:
            return AnalystRecommendationSnapshot()

        chosen = next(
            (item for item in candidates if coerce_str(item.get("period")) == "0m"),
            candidates[0],
        )
        return AnalystRecommendationSnapshot(
            period=coerce_str(chosen.get("period")),
            strong_buy=coerce_int(chosen.get("strongBuy")),
            buy=coerce_int(chosen.get("buy")),
            hold=coerce_int(chosen.get("hold")),
            sell=coerce_int(chosen.get("sell")),
            strong_sell=coerce_int(chosen.get("strongSell")),
        )

    def _extract_recent_analyst_actions(self, payload: Any) -> list[AnalystActionEvent]:
        iterrows = getattr(payload, "iterrows", None)
        if not callable(iterrows):
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=ANALYST_ACTION_WINDOW_DAYS)
        parsed_events: list[tuple[datetime | None, AnalystActionEvent]] = []

        for row_index, row in iterrows():
            row_mapping = self._coerce_mapping(row)
            graded_at = first_non_null(
                coerce_datetime_string(row_mapping.get("GradeDate")),
                coerce_datetime_string(row_mapping.get("date")),
                coerce_datetime_string(row_index),
            )
            parsed_dt = self._parse_iso_timestamp(graded_at)
            if parsed_dt is not None and parsed_dt < cutoff:
                continue

            event = AnalystActionEvent(
                graded_at=graded_at,
                firm=coerce_str(row_mapping.get("Firm")),
                to_grade=coerce_str(row_mapping.get("ToGrade")),
                from_grade=coerce_str(row_mapping.get("FromGrade")),
                action=coerce_str(row_mapping.get("Action")),
                price_target_action=coerce_str(row_mapping.get("priceTargetAction")),
                current_price_target=coerce_float(row_mapping.get("currentPriceTarget")),
                prior_price_target=coerce_float(row_mapping.get("priorPriceTarget")),
            )
            if self._analyst_action_has_no_material_data(event):
                continue

            parsed_events.append((parsed_dt, event))

        parsed_events.sort(
            key=lambda item: item[0] if item[0] is not None else datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        return [item[1] for item in parsed_events[:MAX_ANALYST_ACTION_EVENTS]]

    @staticmethod
    def _parse_iso_timestamp(value: str | None) -> datetime | None:
        if value is None:
            return None
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return parsed

    @staticmethod
    def _analyst_action_has_no_material_data(event: AnalystActionEvent) -> bool:
        return all(
            value is None
            for value in (
                event.graded_at,
                event.firm,
                event.to_grade,
                event.from_grade,
                event.action,
                event.price_target_action,
                event.current_price_target,
                event.prior_price_target,
            )
        )

    @staticmethod
    def _earnings_context_has_no_material_data(earnings_context: EarningsContext) -> bool:
        return all(
            value is None
            for value in (
                earnings_context.next_earnings_date,
                earnings_context.eps_estimate_low,
                earnings_context.eps_estimate_avg,
                earnings_context.eps_estimate_high,
                earnings_context.revenue_estimate_low,
                earnings_context.revenue_estimate_avg,
                earnings_context.revenue_estimate_high,
            )
        )

    @staticmethod
    def _recommendation_has_material_data(snapshot: AnalystRecommendationSnapshot) -> bool:
        return any(
            value is not None
            for value in (
                snapshot.strong_buy,
                snapshot.buy,
                snapshot.hold,
                snapshot.sell,
                snapshot.strong_sell,
            )
        )

    def _analyst_context_has_no_material_data(self, analyst_context: AnalystContext) -> bool:
        return all(
            value is None
            for value in (
                analyst_context.current_price_target,
                analyst_context.target_low,
                analyst_context.target_high,
                analyst_context.target_mean,
                analyst_context.target_median,
            )
        ) and not self._recommendation_has_material_data(analyst_context.recommendation_summary) and not analyst_context.recent_actions

    @staticmethod
    def _build_earnings_limitations(earnings_context: EarningsContext) -> list[str]:
        limitations: list[str] = []
        if earnings_context.next_earnings_date is None:
            limitations.append("Upcoming earnings date is unavailable from the data provider.")
        if (
            earnings_context.eps_estimate_low is None
            and earnings_context.eps_estimate_avg is None
            and earnings_context.eps_estimate_high is None
        ):
            limitations.append("EPS estimates are unavailable from the data provider.")
        if (
            earnings_context.revenue_estimate_low is None
            and earnings_context.revenue_estimate_avg is None
            and earnings_context.revenue_estimate_high is None
        ):
            limitations.append("Revenue estimates are unavailable from the data provider.")
        return limitations

    @staticmethod
    def _build_analyst_limitations(
            *,
        analyst_context: AnalystContext,
        recommendation_has_data: bool,
    ) -> list[str]:
        limitations: list[str] = []
        if (
            analyst_context.target_low is None
            and analyst_context.target_high is None
            and analyst_context.target_mean is None
            and analyst_context.target_median is None
        ):
            limitations.append("Analyst price targets are unavailable from the data provider.")
        if not recommendation_has_data:
            limitations.append("Analyst recommendation summary is unavailable from the data provider.")
        if not analyst_context.recent_actions:
            limitations.append(
                f"No analyst upgrades/downgrades were returned in the last {ANALYST_ACTION_WINDOW_DAYS} days."
            )
        return limitations

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

    @staticmethod
    def _overview_has_no_material_data(overview: TickerOverview) -> bool:
        return all(
            value is None
            for value in (
                overview.quote_type,
                overview.exchange,
                overview.currency,
                overview.current_price,
                overview.market_cap,
                overview.volume,
                overview.fifty_two_week_low,
                overview.fifty_two_week_high,
            )
        )

    @staticmethod
    def _financial_summary_has_no_material_data(summary: FinancialSummary) -> bool:
        return all(
            value is None
            for value in (
                summary.revenue_ttm,
                summary.net_income_ttm,
                summary.ebitda,
                summary.free_cash_flow,
                summary.total_cash,
                summary.total_debt,
            )
        )

    @staticmethod
    def _build_overview_limitations(overview: TickerOverview) -> list[str]:
        limitations: list[str] = []
        if overview.current_price is None:
            limitations.append("Current price is unavailable from the data provider.")
        if overview.market_cap is None:
            limitations.append("Market cap is unavailable from the data provider.")
        if overview.summary is None:
            limitations.append("Company summary is unavailable from the data provider.")
        if overview.earnings_date is None:
            limitations.append("Earnings date is unavailable from the data provider.")
        return limitations

    @staticmethod
    def _build_news_limitations(news_items: list[TickerNewsItem]) -> list[str]:
        if not news_items:
            return ["No recent news items were returned by the data provider."]

        limitations: list[str] = []
        total = len(news_items)

        missing_title = sum(1 for item in news_items if item.title is None)
        missing_link = sum(1 for item in news_items if item.link is None)
        missing_published_at = sum(1 for item in news_items if item.published_at is None)

        if missing_title / total >= 0.5:
            limitations.append("Many news items are missing titles from the data provider.")
        if missing_link / total >= 0.5:
            limitations.append("Many news items are missing links from the data provider.")
        if missing_published_at / total >= 0.5:
            limitations.append("Many news items are missing publish timestamps from the data provider.")

        return limitations

    @staticmethod
    def _build_financial_limitations(summary: FinancialSummary) -> list[str]:
        limitations: list[str] = []

        if summary.revenue_ttm is None or summary.net_income_ttm is None:
            limitations.append(
                "Revenue and/or net income metrics are unavailable from the data provider."
            )
        if summary.free_cash_flow is None:
            limitations.append("Free cash flow is unavailable from the data provider.")
        if summary.total_cash is None or summary.total_debt is None:
            limitations.append(
                "Total cash and/or total debt metrics are unavailable from the data provider."
            )
        if summary.return_on_equity is None and summary.return_on_assets is None:
            limitations.append("Return metrics are unavailable from the data provider.")

        return limitations
