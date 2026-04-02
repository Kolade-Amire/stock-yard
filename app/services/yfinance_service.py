from collections.abc import Iterable, Mapping
from datetime import date, datetime, timedelta, timezone
from math import isfinite
from typing import Any, Protocol, cast

import yfinance as yf
from curl_cffi.curl import CurlError
from curl_cffi.requests.exceptions import RequestException
from starlette.concurrency import run_in_threadpool
from yfinance.exceptions import YFException

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.schemas.ticker import (
    AnalystActionTimelineEvent,
    AnalystActionEvent,
    AnalystContext,
    AnalystContextResponse,
    AnalystHistoryResponse,
    AnalystRecommendationSnapshot,
    AnalystRecommendationBreakdown,
    AnalystSummary,
    AnalystSummaryResponse,
    ComparisonSeriesItem,
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
    GrowthEstimatePoint,
    HolderEntry,
    InsiderRosterEntry,
    MajorHolderMetric,
    OptionContract,
    OptionsChainResponse,
    OptionsExpirationsResponse,
    OwnershipPagination,
    OwnershipResponse,
    PriceBar,
    RevenueEstimatePoint,
    TickerCompareResponse,
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
    IndustryCompanyReference,
    IndustryDetailResponse,
    IndustryGrowthCompanyReference,
    IndustryOverview,
    IndustryPerformingCompanyReference,
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
DEFAULT_OWNERSHIP_LIMIT = 25
MAX_OWNERSHIP_LIMIT = 100
MAX_COMPARE_SYMBOLS = 5
MAX_PLAIN_ALPHA_SYMBOL_LENGTH = 5
SYMBOL_VALIDATION_SEARCH_LIMIT = 10
MAX_ANALYST_HISTORY_ACTION_EVENTS = 25
MAX_BENCHMARK_HOLDINGS = 5
MAX_BENCHMARK_SECTOR_WEIGHTS = 5
MAX_SECTOR_PULSE_FUNDS = 3
MAX_SECTOR_PULSE_COMPANIES = 3
ALLOWED_OWNERSHIP_SECTIONS = frozenset({"all", "institutional", "mutual_funds", "insider_roster"})
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
ALLOWED_HISTORY_PERIODS = frozenset(
    {"1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"}
)
ALLOWED_HISTORY_INTERVALS = frozenset(
    {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo"}
)
ALLOWED_HISTORY_PERIODS_BY_INTERVAL = {
    "1m": frozenset({"1d", "5d"}),
    "2m": frozenset({"1d", "5d", "1mo"}),
    "5m": frozenset({"1d", "5d", "1mo"}),
    "15m": frozenset({"1d", "5d", "1mo"}),
    "30m": frozenset({"1d", "5d", "1mo"}),
    "60m": frozenset({"1d", "5d", "1mo"}),
    "90m": frozenset({"1d", "5d", "1mo"}),
    "1h": frozenset({"1d", "5d", "1mo"}),
    "1d": ALLOWED_HISTORY_PERIODS,
    "5d": ALLOWED_HISTORY_PERIODS,
    "1wk": ALLOWED_HISTORY_PERIODS,
    "1mo": ALLOWED_HISTORY_PERIODS,
    "3mo": ALLOWED_HISTORY_PERIODS,
}


class _SupportsIterrows(Protocol):
    def iterrows(self) -> Iterable[tuple[Any, Any]]: ...


YFINANCE_PROVIDER_EXCEPTIONS = (
    YFException,
    RequestException,
    CurlError,
    AttributeError,
    KeyError,
    IndexError,
    TypeError,
    ValueError,
    RuntimeError,
)
ROW_ACCESS_EXCEPTIONS = (AttributeError, KeyError, IndexError, TypeError)
TO_PYDATETIME_EXCEPTIONS = (AttributeError, TypeError, ValueError, OSError, OverflowError)
MAPPING_COERCION_EXCEPTIONS = (KeyError, TypeError, ValueError)


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
        self._industry_detail_cache = TTLCache[IndustryDetailResponse](cache_ttl_sectors_seconds)
        self._financial_summary_cache = TTLCache[FinancialSummaryResponse](
            cache_ttl_financials_seconds
        )
        self._financial_trends_cache = TTLCache[FinancialTrendsResponse](cache_ttl_financials_seconds)
        self._ownership_cache = TTLCache[OwnershipResponse](cache_ttl_financials_seconds)
        self._earnings_context_cache = TTLCache[EarningsContextResponse](cache_ttl_earnings_seconds)
        self._earnings_history_cache = TTLCache[EarningsHistoryResponse](cache_ttl_earnings_seconds)
        self._earnings_estimates_cache = TTLCache[EarningsEstimatesResponse](
            cache_ttl_earnings_seconds
        )
        self._analyst_context_cache = TTLCache[AnalystContextResponse](cache_ttl_analyst_seconds)
        self._analyst_summary_cache = TTLCache[AnalystSummaryResponse](cache_ttl_analyst_seconds)
        self._analyst_history_cache = TTLCache[AnalystHistoryResponse](cache_ttl_analyst_seconds)
        self._compare_cache = TTLCache[TickerCompareResponse](cache_ttl_history_seconds)
        self._symbol_validation_cache = TTLCache[bool](cache_ttl_overview_seconds)
        self._options_expirations_cache = TTLCache[OptionsExpirationsResponse](
            cache_ttl_history_seconds
        )
        self._options_chain_cache = TTLCache[OptionsChainResponse](cache_ttl_history_seconds)

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
        offset: int = 0,
        active_only: bool = True,
    ) -> EarningsCalendarResponse:
        normalized_start, normalized_end = self._normalize_earnings_calendar_range(
            start=start,
            end=end,
        )
        bounded_limit = self._normalize_and_validate_earnings_calendar_limit(limit)
        normalized_offset = self._normalize_and_validate_offset(offset=offset, field_name="offset")

        cache_key = (
            f"{normalized_start.isoformat()}:{normalized_end.isoformat()}:"
            f"{bounded_limit}:{normalized_offset}:{int(active_only)}"
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
            normalized_offset,
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

    async def get_industry_detail(self, *, industry_key: str) -> IndustryDetailResponse:
        normalized_key = self._normalize_and_validate_industry_key(industry_key)
        cached = self._industry_detail_cache.get(normalized_key)
        if cached is not None:
            self._logger.info("Industry detail cache hit for %s", normalized_key)
            return cached

        response = await run_in_threadpool(self._build_industry_detail_sync, normalized_key)
        self._industry_detail_cache.set(normalized_key, response)
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

    async def get_financial_trends(self, symbol: str) -> FinancialTrendsResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._financial_trends_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Financial trends cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_financial_trends_sync, normalized_symbol)
        self._financial_trends_cache.set(normalized_symbol, response)
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

    async def get_earnings_history(self, symbol: str) -> EarningsHistoryResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._earnings_history_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Earnings history cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_earnings_history_sync, normalized_symbol)
        self._earnings_history_cache.set(normalized_symbol, response)
        return response

    async def get_earnings_estimates(self, symbol: str) -> EarningsEstimatesResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._earnings_estimates_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Earnings estimates cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_earnings_estimates_sync, normalized_symbol)
        self._earnings_estimates_cache.set(normalized_symbol, response)
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

    async def get_analyst_summary(self, symbol: str) -> AnalystSummaryResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._analyst_summary_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Analyst summary cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_analyst_summary_sync, normalized_symbol)
        self._analyst_summary_cache.set(normalized_symbol, response)
        return response

    async def get_analyst_history(self, symbol: str) -> AnalystHistoryResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._analyst_history_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Analyst history cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_analyst_history_sync, normalized_symbol)
        self._analyst_history_cache.set(normalized_symbol, response)
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

    async def compare_tickers(
        self,
        *,
        symbols: str,
        period: str,
        interval: str,
    ) -> TickerCompareResponse:
        normalized_symbols = self._normalize_and_validate_compare_symbols(symbols)
        normalized_period, normalized_interval = self._validate_history_period_interval(
            period=period,
            interval=interval,
        )

        cache_key = (
            f"{','.join(normalized_symbols)}:{normalized_period}:{normalized_interval}"
        )
        cached = self._compare_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Compare cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._compare_tickers_sync,
            normalized_symbols,
            normalized_period,
            normalized_interval,
        )
        self._compare_cache.set(cache_key, response)
        return response

    async def get_ticker_ownership(
        self,
        *,
        symbol: str,
        section: str = "all",
        limit: int = DEFAULT_OWNERSHIP_LIMIT,
        offset: int = 0,
    ) -> OwnershipResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        normalized_section = self._normalize_and_validate_ownership_section(section)
        bounded_limit = self._normalize_and_validate_ownership_limit(limit)
        normalized_offset = self._normalize_and_validate_offset(offset=offset, field_name="offset")

        cache_key = f"{normalized_symbol}:{normalized_section}:{bounded_limit}:{normalized_offset}"
        cached = self._ownership_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Ownership cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_ticker_ownership_sync,
            normalized_symbol,
            normalized_section,
            bounded_limit,
            normalized_offset,
        )
        self._ownership_cache.set(cache_key, response)
        return response

    async def get_option_expirations(self, symbol: str) -> OptionsExpirationsResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        cached = self._options_expirations_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Option expirations cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_option_expirations_sync, normalized_symbol)
        self._options_expirations_cache.set(normalized_symbol, response)
        return response

    async def get_option_chain(self, symbol: str, expiration: str) -> OptionsChainResponse:
        normalized_symbol = self._normalize_and_validate_symbol(symbol)
        normalized_expiration = self._normalize_and_validate_option_expiration(expiration)

        cache_key = f"{normalized_symbol}:{normalized_expiration}"
        cached = self._options_chain_cache.get(cache_key)
        if cached is not None:
            self._logger.info("Option chain cache hit for %s", cache_key)
            return cached

        response = await run_in_threadpool(
            self._get_option_chain_sync,
            normalized_symbol,
            normalized_expiration,
        )
        self._options_chain_cache.set(cache_key, response)
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
        if (
            normalized_symbol.isalpha()
            and len(normalized_symbol) > MAX_PLAIN_ALPHA_SYMBOL_LENGTH
        ):
            raise ApiError(
                code="INVALID_SYMBOL",
                message="Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                status_code=400,
                details={"symbol": symbol},
            )
        if (
            normalized_symbol.isalpha()
            and len(normalized_symbol) == MAX_PLAIN_ALPHA_SYMBOL_LENGTH
            and not self._has_exact_symbol_search_match(normalized_symbol)
        ):
            raise ApiError(
                code="INVALID_SYMBOL",
                message="Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                status_code=400,
                details={"symbol": symbol},
            )
        return normalized_symbol

    def _has_exact_symbol_search_match(self, symbol: str) -> bool:
        cached = self._symbol_validation_cache.get(symbol)
        if cached is not None:
            return cached

        raw_quotes = self._fetch_search_quotes(
            query=symbol,
            limit=SYMBOL_VALIDATION_SEARCH_LIMIT,
        )
        has_exact_match = any(
            result is not None and result.symbol == symbol
            for result in (self._map_search_result(quote) for quote in raw_quotes)
        )
        self._symbol_validation_cache.set(symbol, has_exact_match)
        return has_exact_match

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

    @staticmethod
    def _normalize_and_validate_industry_key(industry_key: str) -> str:
        normalized_key = industry_key.strip().lower()
        if (
            not normalized_key
            or normalized_key.startswith("-")
            or normalized_key.endswith("-")
            or any(not (char.isalnum() or char == "-") for char in normalized_key)
        ):
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Industry key format is invalid.",
                status_code=400,
                details={"industryKey": industry_key},
            )
        return normalized_key

    def _normalize_and_validate_compare_symbols(self, symbols: str) -> list[str]:
        raw_symbols = [part.strip() for part in symbols.split(",")]
        normalized_symbols: list[str] = []
        seen: set[str] = set()

        for raw_symbol in raw_symbols:
            if not raw_symbol:
                continue
            normalized_symbol = self._normalize_and_validate_symbol(raw_symbol)
            if normalized_symbol in seen:
                continue
            normalized_symbols.append(normalized_symbol)
            seen.add(normalized_symbol)

        if len(normalized_symbols) < 2 or len(normalized_symbols) > MAX_COMPARE_SYMBOLS:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Comparison requests require between 2 and 5 unique symbols.",
                status_code=400,
                details={
                    "symbols": symbols,
                    "minSymbols": 2,
                    "maxSymbols": MAX_COMPARE_SYMBOLS,
                },
            )

        return normalized_symbols

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

    def _normalize_and_validate_option_expiration(self, expiration: str) -> str:
        try:
            return self._parse_iso_date(value=expiration, field_name="expiration").isoformat()
        except ApiError as exc:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Invalid options expiration format.",
                status_code=400,
                details={
                    "field": "expiration",
                    "value": expiration,
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

    @staticmethod
    def _normalize_and_validate_ownership_limit(limit: int) -> int:
        if limit < 1 or limit > MAX_OWNERSHIP_LIMIT:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Ownership limit is outside the supported range.",
                status_code=400,
                details={
                    "limit": limit,
                    "minLimit": 1,
                    "maxLimit": MAX_OWNERSHIP_LIMIT,
                },
            )
        return limit

    @staticmethod
    def _normalize_and_validate_ownership_section(section: str) -> str:
        normalized_section = section.strip().lower()
        if normalized_section not in ALLOWED_OWNERSHIP_SECTIONS:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported ownership section.",
                status_code=400,
                details={
                    "section": section,
                    "allowedSections": sorted(ALLOWED_OWNERSHIP_SECTIONS),
                },
            )
        return normalized_section

    @staticmethod
    def _normalize_and_validate_offset(*, offset: int, field_name: str) -> int:
        if offset < 0:
            raise ApiError(
                code="VALIDATION_ERROR",
                message=f"{field_name} must be greater than or equal to zero.",
                status_code=400,
                details={field_name: offset, "minValue": 0},
            )
        return offset

    @staticmethod
    def _validate_history_period_interval(
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
                self._coerce_optional_text(quote.get("shortName")),
                self._coerce_optional_text(quote.get("longName")),
                self._coerce_optional_text(quote.get("displayName")),
                symbol,
            ),
            exchange=first_non_null(
                self._coerce_optional_text(quote.get("exchange")),
                self._coerce_optional_text(quote.get("fullExchangeName")),
                self._coerce_optional_text(quote.get("exchangeName")),
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
            except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning("Benchmark info fetch failed for %s: %s", symbol, exc)
            limitations.append("Quote metadata is unavailable from the data provider.")

        try:
            fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning("Benchmark fast_info fetch failed for %s: %s", symbol, exc)
            limitations.append("Fast quote data is unavailable from the data provider.")

        try:
            funds_data = getattr(ticker, "funds_data", None)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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

        benchmark_fund = BenchmarkFund.model_validate(
            {
                "symbol": symbol,
                "benchmarkKey": benchmark_key,
                "benchmarkName": benchmark_name,
                "category": category,
                "displayName": first_non_null(
                    coerce_str(info.get("longName")),
                    coerce_str(info.get("shortName")),
                    coerce_str(info.get("displayName")),
                    symbol,
                ),
                "currentPrice": current_price,
                "previousClose": previous_close,
                "dayChange": day_change,
                "dayChangePercent": day_change_percent,
                "currency": first_non_null(
                    coerce_str(info.get("currency")),
                    coerce_str(fast_info.get("currency")),
                ),
                "expenseRatio": expense_ratio,
                "netAssets": net_assets,
                "yield_": first_non_null(
                    coerce_float(info.get("yield")),
                    coerce_float(info.get("trailingAnnualDividendYield")),
                ),
                "fundFamily": first_non_null(
                    coerce_str(fund_overview.get("family")),
                    coerce_str(info.get("fundFamily")),
                ),
                "topHoldings": top_holdings,
                "sectorWeights": sector_weights,
                "dataLimitations": [],
            }
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
        offset: int,
        active_only: bool,
    ) -> EarningsCalendarResponse:
        try:
            calendars = yf.Calendars(start=start, end=end)
            raw_events = calendars.get_earnings_calendar(
                start=start,
                end=end,
                limit=limit + 1,
                offset=offset,
                filter_most_active=active_only,
            )
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance earnings calendar fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch earnings calendar from market data provider.",
                status_code=502,
                details={
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "limit": limit,
                    "offset": offset,
                    "activeOnly": active_only,
                },
            ) from exc

        if raw_events is None or getattr(raw_events, "empty", False):
            return EarningsCalendarResponse(
                start=start.isoformat(),
                end=end.isoformat(),
                limit=limit,
                offset=offset,
                activeOnly=active_only,
                returnedCount=0,
                hasMore=False,
                nextOffset=None,
                events=[],
                dataLimitations=[],
            )

        events: list[EarningsCalendarEvent] = []
        skipped_rows = 0
        rows = self._get_iterrows(raw_events)
        if rows is None:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings calendar data is unavailable.",
                status_code=404,
                details={"start": start.isoformat(), "end": end.isoformat(), "offset": offset},
            )

        for index, row in rows:
            mapped_event = self._map_earnings_calendar_event(index=index, row=row)
            if mapped_event is None:
                skipped_rows += 1
                continue
            events.append(mapped_event)
            if len(events) >= limit + 1:
                break

        if not events and skipped_rows > 0:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings calendar data is unavailable.",
                status_code=404,
                details={"start": start.isoformat(), "end": end.isoformat(), "offset": offset},
            )

        limitations: list[str] = []
        if skipped_rows > 0:
            limitations.append(
                f"{skipped_rows} earnings calendar entries were omitted because provider fields were incomplete."
            )

        has_more = len(events) > limit
        trimmed_events = events[:limit]
        returned_count = len(trimmed_events)

        return EarningsCalendarResponse(
            start=start.isoformat(),
            end=end.isoformat(),
            limit=limit,
            offset=offset,
            activeOnly=active_only,
            returnedCount=returned_count,
            hasMore=has_more,
            nextOffset=offset + returned_count if has_more else None,
            events=trimmed_events,
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
            except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance sector init failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to initialize sector data from market data provider.",
                status_code=502,
                details={"sectorKey": sector_key},
            ) from exc

        try:
            name = self._coerce_optional_text(sector.name)
            symbol = self._coerce_optional_text(sector.symbol)
            overview = self._map_sector_overview(sector.overview)
            top_etfs = self._map_sector_fund_references(sector.top_etfs)
            top_mutual_funds = self._map_sector_fund_references(sector.top_mutual_funds)
            top_companies = self._map_sector_company_references(sector.top_companies)
            industries = self._map_sector_industries(sector.industries)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        holdings: list[BenchmarkHolding] = []
        for index, row in rows:
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
                self._coerce_optional_text(overview_mapping.get("message_board_id")),
                self._coerce_optional_text(overview_mapping.get("messageBoardId")),
            ),
            description=self._coerce_optional_text(overview_mapping.get("description")),
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
            name = self._coerce_optional_text(raw_name)
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
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        companies: list[SectorCompanyReference] = []
        for index, row in rows:
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
                    name=self._coerce_optional_text(row_mapping.get("name")),
                    rating=self._coerce_optional_text(row_mapping.get("rating")),
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
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        industries: list[SectorIndustryReference] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            key = coerce_str(index)
            if key is None:
                continue
            industries.append(
                SectorIndustryReference(
                    key=key,
                    name=self._coerce_optional_text(row_mapping.get("name")),
                    symbol=self._coerce_optional_text(row_mapping.get("symbol")),
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
        except ROW_ACCESS_EXCEPTIONS:
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
            except TO_PYDATETIME_EXCEPTIONS:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
            search_factory = getattr(yf, "Search", None)
            if callable(search_factory):
                typed_search_factory = cast(Any, search_factory)
                search = typed_search_factory(query, max_results=limit, news_count=0)
                quotes = getattr(search, "quotes", [])
            else:
                search_function = getattr(yf, "search", None)
                if not callable(search_function):
                    raise RuntimeError("yfinance search API is unavailable.")
                typed_search_function = cast(Any, search_function)
                payload = typed_search_function(query, max_results=limit, news_count=0)
                quotes = payload.get("quotes", []) if isinstance(payload, dict) else []
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance search failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to search symbols from market data provider.",
                status_code=502,
            ) from exc

        if not isinstance(quotes, list):
            return []
        return [item for item in quotes if isinstance(item, dict)]

    @staticmethod
    def _map_search_result(quote: dict[str, Any]) -> TickerSearchResult | None:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
            except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning("yfinance analyst_price_targets fetch failed for %s: %s", symbol, exc)
            limitations.append("Analyst price targets are unavailable from the data provider.")

        recommendation_snapshot = AnalystRecommendationSnapshot()
        recommendation_populated = False
        try:
            raw_recommendations = ticker.get_recommendations_summary()
            recommendation_snapshot = self._extract_recommendation_snapshot(raw_recommendations)
            recommendation_populated = self._recommendation_has_material_data(recommendation_snapshot)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
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

        rows = self._get_iterrows(history)
        if rows is None:
            return []

        bars: list[PriceBar] = []
        for index, row in rows:
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
            except TO_PYDATETIME_EXCEPTIONS:
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
            except MAPPING_COERCION_EXCEPTIONS:
                return {}
        if isinstance(payload, Iterable):
            try:
                return dict(payload)
            except MAPPING_COERCION_EXCEPTIONS:
                return {}
        return {}

    @staticmethod
    def _get_iterrows(payload: Any) -> Iterable[tuple[Any, Any]] | None:
        try:
            rows = cast(_SupportsIterrows, payload).iterrows()
        except (AttributeError, TypeError):
            return None
        if not isinstance(rows, Iterable):
            return None
        return rows

    @staticmethod
    def _coerce_optional_text(value: Any) -> str | None:
        normalized = coerce_str(value)
        if normalized is None:
            return None
        if normalized.strip().lower() in {"nan", "none", "null"}:
            return None
        return normalized

    def _build_industry_detail_sync(self, industry_key: str) -> IndustryDetailResponse:
        try:
            industry = yf.Industry(industry_key)
            name = self._coerce_optional_text(industry.name)
            symbol = self._coerce_optional_text(industry.symbol)
            sector_key = self._coerce_optional_text(getattr(industry, "sector_key", None))
            sector_name = self._coerce_optional_text(getattr(industry, "sector_name", None))
            overview = self._map_industry_overview(getattr(industry, "overview", None))
            top_companies = self._map_industry_company_references(
                getattr(industry, "top_companies", None)
            )
            top_growth_companies = self._map_industry_growth_companies(
                getattr(industry, "top_growth_companies", None)
            )
            top_performing_companies = self._map_industry_performing_companies(
                getattr(industry, "top_performing_companies", None)
            )
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance industry fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch industry data from market data provider.",
                status_code=502,
                details={"industryKey": industry_key},
            ) from exc

        detail = IndustryDetailResponse(
            key=industry_key,
            name=name,
            symbol=symbol,
            sectorKey=sector_key,
            sectorName=sector_name,
            overview=overview,
            topCompanies=top_companies,
            topGrowthCompanies=top_growth_companies,
            topPerformingCompanies=top_performing_companies,
            dataLimitations=[],
        )

        if self._industry_detail_has_no_material_data(detail):
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported industry key.",
                status_code=400,
                details={"industryKey": industry_key},
            )

        limitations: list[str] = []
        if detail.name is None:
            limitations.append("Industry name is unavailable from the data provider.")
        if detail.symbol is None:
            limitations.append("Industry symbol is unavailable from the data provider.")
        if detail.overview.description is None:
            limitations.append("Industry description is unavailable from the data provider.")
        if not detail.topCompanies:
            limitations.append("Top companies are unavailable from the data provider.")
        if not detail.topGrowthCompanies:
            limitations.append("Top growth companies are unavailable from the data provider.")
        if not detail.topPerformingCompanies:
            limitations.append("Top performing companies are unavailable from the data provider.")

        detail.dataLimitations = self._dedupe_preserve_order(limitations)
        return detail

    def _get_financial_trends_sync(self, symbol: str) -> FinancialTrendsResponse:
        try:
            ticker = yf.Ticker(symbol)
            annual_income_stmt = getattr(ticker, "income_stmt", None)
            quarterly_income_stmt = getattr(ticker, "quarterly_income_stmt", None)
            annual_cash_flow = getattr(ticker, "cash_flow", None)
            quarterly_cash_flow = getattr(ticker, "quarterly_cash_flow", None)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance financial trends fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch financial trends from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        annual_points = self._build_financial_trend_points(
            income_stmt=annual_income_stmt,
            cash_flow=annual_cash_flow,
        )
        quarterly_points = self._build_financial_trend_points(
            income_stmt=quarterly_income_stmt,
            cash_flow=quarterly_cash_flow,
        )

        if not annual_points and not quarterly_points:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Financial trends are unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations: list[str] = []
        if not annual_points:
            limitations.append("Annual financial trends are unavailable from the data provider.")
        if not quarterly_points:
            limitations.append("Quarterly financial trends are unavailable from the data provider.")

        return FinancialTrendsResponse(
            symbol=symbol,
            annual=annual_points,
            quarterly=quarterly_points,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_earnings_history_sync(self, symbol: str) -> EarningsHistoryResponse:
        try:
            ticker = yf.Ticker(symbol)
            payload = getattr(ticker, "earnings_history", None)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance earnings history fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch earnings history from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        events = self._map_earnings_history_events(payload)
        if not events:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings history is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        return EarningsHistoryResponse(symbol=symbol, events=events, dataLimitations=[])

    def _get_earnings_estimates_sync(self, symbol: str) -> EarningsEstimatesResponse:
        try:
            ticker = yf.Ticker(symbol)
            raw_eps_estimates = getattr(ticker, "earnings_estimate", None)
            raw_revenue_estimates = getattr(ticker, "revenue_estimate", None)
            raw_growth_estimates = getattr(ticker, "growth_estimates", None)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance earnings estimates fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch earnings estimates from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        eps_estimates = self._map_earnings_estimate_points(raw_eps_estimates)
        revenue_estimates = self._map_revenue_estimate_points(raw_revenue_estimates)
        growth_estimates = self._map_growth_estimate_points(raw_growth_estimates)

        if not eps_estimates and not revenue_estimates and not growth_estimates:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Earnings estimates are unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations: list[str] = []
        if not eps_estimates:
            limitations.append("EPS estimates are unavailable from the data provider.")
        if not revenue_estimates:
            limitations.append("Revenue estimates are unavailable from the data provider.")
        if not growth_estimates:
            limitations.append("Growth estimates are unavailable from the data provider.")

        return EarningsEstimatesResponse(
            symbol=symbol,
            epsEstimates=eps_estimates,
            revenueEstimates=revenue_estimates,
            growthEstimates=growth_estimates,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _compare_tickers_sync(
        self,
        symbols: list[str],
        period: str,
        interval: str,
    ) -> TickerCompareResponse:
        series: list[ComparisonSeriesItem] = []
        limitations: list[str] = []

        for symbol in symbols:
            overview_response = self._get_ticker_overview_sync(symbol)
            history_response = self._get_ticker_history_sync(symbol, period, interval)
            overview = overview_response.overview

            current_price = overview.current_price
            change_percent = (
                ((current_price - overview.previous_close) / overview.previous_close) * 100
                if current_price is not None and overview.previous_close not in (None, 0)
                else None
            )

            series.append(
                ComparisonSeriesItem(
                    symbol=symbol,
                    displayName=overview.display_name,
                    currentPrice=current_price,
                    changePercent=change_percent,
                    bars=history_response.bars,
                )
            )

            if current_price is None:
                limitations.append(f"{symbol} current price is unavailable from the data provider.")
            if change_percent is None:
                limitations.append(
                    f"{symbol} day-over-day percent change could not be calculated from provider quote data."
                )

        return TickerCompareResponse(
            symbols=symbols,
            period=period,
            interval=interval,
            series=series,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _map_industry_overview(self, payload: Any) -> IndustryOverview:
        overview_mapping = self._coerce_mapping(payload)
        return IndustryOverview(
            companiesCount=self._coerce_non_negative_int(
                first_non_null(
                    overview_mapping.get("companies_count"),
                    overview_mapping.get("companiesCount"),
                )
            ),
            marketCap=self._coerce_finite_float(
                first_non_null(overview_mapping.get("market_cap"), overview_mapping.get("marketCap"))
            ),
            messageBoardId=first_non_null(
                self._coerce_optional_text(overview_mapping.get("message_board_id")),
                self._coerce_optional_text(overview_mapping.get("messageBoardId")),
            ),
            description=self._coerce_optional_text(overview_mapping.get("description")),
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

    def _map_industry_company_references(
        self,
        payload: Any,
    ) -> list[IndustryCompanyReference]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        companies: list[IndustryCompanyReference] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            symbol = normalize_symbol(
                first_non_null(coerce_str(index), coerce_str(row_mapping.get("symbol"))) or ""
            )
            if not symbol:
                continue
            companies.append(
                IndustryCompanyReference(
                    symbol=symbol,
                    name=self._coerce_optional_text(row_mapping.get("name")),
                    rating=self._coerce_optional_text(row_mapping.get("rating")),
                    marketWeight=self._coerce_finite_float(
                        first_non_null(
                            row_mapping.get("market weight"),
                            row_mapping.get("marketWeight"),
                        )
                    ),
                )
            )
        return companies

    def _map_industry_growth_companies(
        self,
        payload: Any,
    ) -> list[IndustryGrowthCompanyReference]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        companies: list[IndustryGrowthCompanyReference] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            symbol = normalize_symbol(
                first_non_null(coerce_str(index), coerce_str(row_mapping.get("symbol"))) or ""
            )
            if not symbol:
                continue
            companies.append(
                IndustryGrowthCompanyReference(
                    symbol=symbol,
                    name=self._coerce_optional_text(row_mapping.get("name")),
                    ytdReturn=self._coerce_finite_float(
                        first_non_null(row_mapping.get("ytd return"), row_mapping.get("ytdReturn"))
                    ),
                    growthEstimate=self._coerce_finite_float(
                        first_non_null(
                            row_mapping.get("growth estimate"),
                            row_mapping.get("growthEstimate"),
                        )
                    ),
                )
            )
        return companies

    def _map_industry_performing_companies(
        self,
        payload: Any,
    ) -> list[IndustryPerformingCompanyReference]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        companies: list[IndustryPerformingCompanyReference] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            symbol = normalize_symbol(
                first_non_null(coerce_str(index), coerce_str(row_mapping.get("symbol"))) or ""
            )
            if not symbol:
                continue
            companies.append(
                IndustryPerformingCompanyReference(
                    symbol=symbol,
                    name=self._coerce_optional_text(row_mapping.get("name")),
                    ytdReturn=self._coerce_finite_float(
                        first_non_null(row_mapping.get("ytd return"), row_mapping.get("ytdReturn"))
                    ),
                    lastPrice=self._coerce_finite_float(
                        first_non_null(row_mapping.get("last price"), row_mapping.get("lastPrice"))
                    ),
                    targetPrice=self._coerce_finite_float(
                        first_non_null(
                            row_mapping.get("target price"),
                            row_mapping.get("targetPrice"),
                        )
                    ),
                )
            )
        return companies

    def _build_financial_trend_points(
        self,
        *,
        income_stmt: Any,
        cash_flow: Any,
    ) -> list[FinancialTrendPoint]:
        revenue_series = self._extract_statement_series(
            income_stmt,
            row_labels=("Total Revenue", "Operating Revenue"),
        )
        net_income_series = self._extract_statement_series(
            income_stmt,
            row_labels=(
                "Net Income",
                "Net Income Common Stockholders",
                "Net Income Including Noncontrolling Interests",
                "Net Income From Continuing And Discontinued Operation",
                "Net Income From Continuing Operation Net Minority Interest",
                "Net Income Continuous Operations",
            ),
        )
        operating_cash_flow_series = self._extract_statement_series(
            cash_flow,
            row_labels=("Operating Cash Flow",),
        )
        capital_expenditure_series = self._extract_statement_series(
            cash_flow,
            row_labels=("Capital Expenditure",),
        )
        free_cash_flow_series = self._extract_statement_series(
            cash_flow,
            row_labels=("Free Cash Flow",),
        )

        periods = sorted(
            {
                *revenue_series.keys(),
                *net_income_series.keys(),
                *operating_cash_flow_series.keys(),
                *capital_expenditure_series.keys(),
                *free_cash_flow_series.keys(),
            }
        )

        points: list[FinancialTrendPoint] = []
        for period_end in periods:
            operating_cash_flow = operating_cash_flow_series.get(period_end)
            capital_expenditure = capital_expenditure_series.get(period_end)
            free_cash_flow = free_cash_flow_series.get(period_end)
            if (
                free_cash_flow is None
                and operating_cash_flow is not None
                and capital_expenditure is not None
            ):
                free_cash_flow = operating_cash_flow + capital_expenditure

            point = FinancialTrendPoint(
                periodEnd=period_end,
                revenue=revenue_series.get(period_end),
                netIncome=net_income_series.get(period_end),
                operatingCashFlow=operating_cash_flow,
                capitalExpenditure=capital_expenditure,
                freeCashFlow=free_cash_flow,
            )
            if any(
                value is not None
                for value in (
                    point.revenue,
                    point.netIncome,
                    point.operatingCashFlow,
                    point.capitalExpenditure,
                    point.freeCashFlow,
                )
            ):
                points.append(point)

        return points

    def _extract_statement_series(
        self,
        payload: Any,
        *,
        row_labels: tuple[str, ...],
    ) -> dict[str, float]:
        for row_label in row_labels:
            series = self._extract_statement_row(payload, row_label=row_label)
            if series:
                return series
        return {}

    def _extract_statement_row(self, payload: Any, *, row_label: str) -> dict[str, float]:
        loc = getattr(payload, "loc", None)
        if loc is None:
            return {}

        try:
            row = loc[row_label]
        except ROW_ACCESS_EXCEPTIONS:
            return {}

        row_mapping = self._coerce_mapping(row)
        values: dict[str, float] = {}
        for raw_period, raw_value in row_mapping.items():
            period_end = self._coerce_period_end(raw_period)
            value = self._coerce_finite_float(raw_value)
            if period_end is None or value is None:
                continue
            values[period_end] = value
        return values

    def _map_earnings_history_events(self, payload: Any) -> list[EarningsHistoryEvent]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        events: list[EarningsHistoryEvent] = []
        for index, row in rows:
            report_date = self._coerce_period_end(index)
            row_mapping = self._coerce_mapping(row)
            if report_date is None:
                continue
            events.append(
                EarningsHistoryEvent(
                    reportDate=report_date,
                    quarter=self._quarter_label_from_period_end(report_date),
                    epsEstimate=self._coerce_finite_float(row_mapping.get("epsEstimate")),
                    epsActual=self._coerce_finite_float(row_mapping.get("epsActual")),
                    surprisePercent=self._coerce_finite_float(row_mapping.get("surprisePercent")),
                )
            )

        events.sort(key=lambda item: item.reportDate)
        return events

    def _map_earnings_estimate_points(self, payload: Any) -> list[EarningsEstimatePoint]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        points: list[EarningsEstimatePoint] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            period = coerce_str(index)
            if period is None:
                continue
            points.append(
                EarningsEstimatePoint(
                    period=period,
                    avg=self._coerce_finite_float(row_mapping.get("avg")),
                    low=self._coerce_finite_float(row_mapping.get("low")),
                    high=self._coerce_finite_float(row_mapping.get("high")),
                    yearAgoEps=self._coerce_finite_float(row_mapping.get("yearAgoEps")),
                    numberOfAnalysts=self._coerce_non_negative_int(
                        row_mapping.get("numberOfAnalysts")
                    ),
                    growth=self._coerce_finite_float(row_mapping.get("growth")),
                )
            )
        return points

    def _map_revenue_estimate_points(self, payload: Any) -> list[RevenueEstimatePoint]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        points: list[RevenueEstimatePoint] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            period = coerce_str(index)
            if period is None:
                continue
            points.append(
                RevenueEstimatePoint(
                    period=period,
                    avg=self._coerce_finite_float(row_mapping.get("avg")),
                    low=self._coerce_finite_float(row_mapping.get("low")),
                    high=self._coerce_finite_float(row_mapping.get("high")),
                    numberOfAnalysts=self._coerce_non_negative_int(
                        row_mapping.get("numberOfAnalysts")
                    ),
                    yearAgoRevenue=self._coerce_finite_float(row_mapping.get("yearAgoRevenue")),
                    growth=self._coerce_finite_float(row_mapping.get("growth")),
                )
            )
        return points

    def _map_growth_estimate_points(self, payload: Any) -> list[GrowthEstimatePoint]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        points: list[GrowthEstimatePoint] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            period = coerce_str(index)
            if period is None:
                continue
            points.append(
                GrowthEstimatePoint(
                    period=period,
                    stockTrend=self._coerce_finite_float(row_mapping.get("stockTrend")),
                    indexTrend=self._coerce_finite_float(row_mapping.get("indexTrend")),
                )
            )
        return points

    def _get_analyst_summary_sync(self, symbol: str) -> AnalystSummaryResponse:
        try:
            ticker = yf.Ticker(symbol)
            info = self._coerce_mapping(getattr(ticker, "info", {}))
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance analyst summary fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch analyst summary from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        limitations: list[str] = []

        try:
            price_targets = self._coerce_mapping(ticker.get_analyst_price_targets())
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning(
                "yfinance analyst_price_targets fetch failed for %s: %s",
                symbol,
                exc,
            )
            price_targets = {}
            limitations.append("Analyst price targets are unavailable from the data provider.")

        try:
            raw_recommendations = ticker.get_recommendations_summary()
            recommendation_snapshot = self._extract_recommendation_snapshot(raw_recommendations)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning(
                "yfinance recommendations_summary fetch failed for %s: %s",
                symbol,
                exc,
            )
            recommendation_snapshot = AnalystRecommendationSnapshot()
            limitations.append("Analyst recommendation summary is unavailable from the data provider.")

        try:
            raw_actions = ticker.get_upgrades_downgrades()
            recent_action_count = self._count_recent_analyst_actions(raw_actions)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning(
                "yfinance upgrades_downgrades fetch failed for %s: %s",
                symbol,
                exc,
            )
            recent_action_count = 0
            limitations.append("Recent analyst action history is unavailable from the data provider.")

        summary = AnalystSummary(
            currentPriceTarget=self._coerce_finite_float(price_targets.get("current")),
            targetLow=first_non_null(
                self._coerce_finite_float(price_targets.get("low")),
                self._coerce_finite_float(info.get("targetLowPrice")),
            ),
            targetHigh=first_non_null(
                self._coerce_finite_float(price_targets.get("high")),
                self._coerce_finite_float(info.get("targetHighPrice")),
            ),
            targetMean=first_non_null(
                self._coerce_finite_float(price_targets.get("mean")),
                self._coerce_finite_float(info.get("targetMeanPrice")),
            ),
            targetMedian=first_non_null(
                self._coerce_finite_float(price_targets.get("median")),
                self._coerce_finite_float(info.get("targetMedianPrice")),
            ),
            recommendationSummary=self._build_public_recommendation_breakdown(recommendation_snapshot),
            recentActionCount=recent_action_count,
            recentActionWindowDays=ANALYST_ACTION_WINDOW_DAYS,
        )

        if self._analyst_summary_has_no_material_data(summary):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Analyst summary is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        if summary.currentPriceTarget is None:
            limitations.append("Current analyst price target is unavailable from the data provider.")
        if summary.targetMean is None:
            limitations.append("Mean analyst price target is unavailable from the data provider.")
        if not self._public_recommendation_has_material_data(summary.recommendationSummary):
            limitations.append("Analyst recommendation summary is unavailable from the data provider.")

        return AnalystSummaryResponse(
            symbol=symbol,
            analystSummary=summary,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_analyst_history_sync(self, symbol: str) -> AnalystHistoryResponse:
        try:
            ticker = yf.Ticker(symbol)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance analyst history init failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to initialize analyst history from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        limitations: list[str] = []

        try:
            raw_recommendations = getattr(ticker, "recommendations", None)
            recommendation_history = self._extract_recommendation_history(raw_recommendations)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning("yfinance recommendations fetch failed for %s: %s", symbol, exc)
            recommendation_history = []
            limitations.append("Analyst recommendation history is unavailable from the data provider.")

        try:
            raw_actions = ticker.get_upgrades_downgrades()
            actions = self._extract_analyst_history_actions(raw_actions)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.warning(
                "yfinance upgrades_downgrades history fetch failed for %s: %s",
                symbol,
                exc,
            )
            actions = []
            limitations.append("Recent analyst action timeline is unavailable from the data provider.")

        if not recommendation_history and not actions:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Analyst history is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        return AnalystHistoryResponse(
            symbol=symbol,
            recommendationHistory=recommendation_history,
            actions=actions,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_ticker_ownership_sync(
        self,
        symbol: str,
        section: str,
        limit: int,
        offset: int,
    ) -> OwnershipResponse:
        try:
            ticker = yf.Ticker(symbol)
            major_holders = getattr(ticker, "major_holders", None)
            institutional_holders = getattr(ticker, "institutional_holders", None)
            mutualfund_holders = getattr(ticker, "mutualfund_holders", None)
            insider_roster = getattr(ticker, "insider_roster_holders", None)
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance ownership fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch ownership data from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        mapped_major_holders = self._map_major_holder_metrics(major_holders)
        mapped_institutional_holders = self._map_holder_entries(institutional_holders)
        mapped_mutual_fund_holders = self._map_holder_entries(mutualfund_holders)
        mapped_insider_roster = self._map_insider_roster_entries(insider_roster)

        full_response = OwnershipResponse(
            symbol=symbol,
            requestedSection=section,
            limit=limit,
            offset=offset,
            majorHolders=mapped_major_holders,
            institutionalHolders=mapped_institutional_holders,
            mutualFundHolders=mapped_mutual_fund_holders,
            insiderRoster=mapped_insider_roster,
            institutionalPagination=None,
            mutualFundPagination=None,
            insiderRosterPagination=None,
            dataLimitations=[],
        )

        if self._ownership_has_no_material_data(full_response):
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Ownership data is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        limitations: list[str] = []
        if not mapped_major_holders:
            limitations.append("Major holder metrics are unavailable from the data provider.")
        if section in {"all", "institutional"} and not mapped_institutional_holders:
            limitations.append("Institutional holders are unavailable from the data provider.")
        if section in {"all", "mutual_funds"} and not mapped_mutual_fund_holders:
            limitations.append("Mutual fund holders are unavailable from the data provider.")
        if section in {"all", "insider_roster"} and not mapped_insider_roster:
            limitations.append("Insider roster is unavailable from the data provider.")

        sliced_institutional = self._slice_paginated_items(mapped_institutional_holders, limit, offset)
        sliced_mutual_fund = self._slice_paginated_items(mapped_mutual_fund_holders, limit, offset)
        sliced_insider_roster = self._slice_paginated_items(mapped_insider_roster, limit, offset)

        return OwnershipResponse(
            symbol=symbol,
            requestedSection=section,
            limit=limit,
            offset=offset,
            majorHolders=mapped_major_holders,
            institutionalHolders=(
                sliced_institutional if section in {"all", "institutional"} else []
            ),
            mutualFundHolders=(
                sliced_mutual_fund if section in {"all", "mutual_funds"} else []
            ),
            insiderRoster=(
                sliced_insider_roster if section in {"all", "insider_roster"} else []
            ),
            institutionalPagination=(
                self._build_ownership_pagination(
                    total_available=len(mapped_institutional_holders),
                    limit=limit,
                    offset=offset,
                    returned_count=len(sliced_institutional),
                )
                if section in {"all", "institutional"}
                else None
            ),
            mutualFundPagination=(
                self._build_ownership_pagination(
                    total_available=len(mapped_mutual_fund_holders),
                    limit=limit,
                    offset=offset,
                    returned_count=len(sliced_mutual_fund),
                )
                if section in {"all", "mutual_funds"}
                else None
            ),
            insiderRosterPagination=(
                self._build_ownership_pagination(
                    total_available=len(mapped_insider_roster),
                    limit=limit,
                    offset=offset,
                    returned_count=len(sliced_insider_roster),
                )
                if section in {"all", "insider_roster"}
                else None
            ),
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _get_option_expirations_sync(self, symbol: str) -> OptionsExpirationsResponse:
        try:
            ticker = yf.Ticker(symbol)
            expirations = getattr(ticker, "options", ())
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance options expirations fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch options expirations from market data provider.",
                status_code=502,
                details={"symbol": symbol},
            ) from exc

        normalized_expirations = [
            expiration
            for expiration in expirations
            if isinstance(expiration, str) and expiration.strip()
        ]
        if not normalized_expirations:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Options expirations are unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        return OptionsExpirationsResponse(symbol=symbol, expirations=normalized_expirations)

    def _get_option_chain_sync(self, symbol: str, expiration: str) -> OptionsChainResponse:
        try:
            ticker = yf.Ticker(symbol)
            expirations = tuple(getattr(ticker, "options", ()))
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance options chain init failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to initialize options chain from market data provider.",
                status_code=502,
                details={"symbol": symbol, "expiration": expiration},
            ) from exc

        if not expirations:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Options chain is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol},
            )

        if expiration not in expirations:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported options expiration for this ticker.",
                status_code=400,
                details={"symbol": symbol, "expiration": expiration, "allowedExpirations": list(expirations)},
            )

        try:
            chain = ticker.option_chain(expiration)
            fast_info = self._coerce_mapping(getattr(ticker, "fast_info", {}))
            info = self._coerce_mapping(getattr(ticker, "info", {}))
        except YFINANCE_PROVIDER_EXCEPTIONS as exc:
            self._logger.exception("yfinance options chain fetch failed", exc_info=exc)
            raise ApiError(
                code="PROVIDER_ERROR",
                message="Failed to fetch options chain from market data provider.",
                status_code=502,
                details={"symbol": symbol, "expiration": expiration},
            ) from exc

        calls = self._map_option_contracts(getattr(chain, "calls", None))
        puts = self._map_option_contracts(getattr(chain, "puts", None))
        if not calls and not puts:
            raise ApiError(
                code="DATA_UNAVAILABLE",
                message="Options chain is unavailable for this ticker.",
                status_code=404,
                details={"symbol": symbol, "expiration": expiration},
            )

        limitations: list[str] = []
        underlying_price = first_non_null(
            self._coerce_finite_float(fast_info.get("lastPrice")),
            self._coerce_finite_float(info.get("currentPrice")),
            self._coerce_finite_float(info.get("regularMarketPrice")),
        )
        if underlying_price is None:
            limitations.append("Underlying price is unavailable from the data provider.")
        if not calls:
            limitations.append("Call contracts are unavailable from the data provider.")
        if not puts:
            limitations.append("Put contracts are unavailable from the data provider.")

        return OptionsChainResponse(
            symbol=symbol,
            expiration=expiration,
            underlyingPrice=underlying_price,
            calls=calls,
            puts=puts,
            dataLimitations=self._dedupe_preserve_order(limitations),
        )

    def _industry_detail_has_no_material_data(self, detail: IndustryDetailResponse) -> bool:
        return (
            detail.name is None
            and detail.symbol is None
            and detail.sectorKey is None
            and detail.sectorName is None
            and not self._industry_overview_has_material_data(detail.overview)
            and not detail.topCompanies
            and not detail.topGrowthCompanies
            and not detail.topPerformingCompanies
        )

    @staticmethod
    def _industry_overview_has_material_data(overview: IndustryOverview) -> bool:
        return any(
            value is not None
            for value in (
                overview.companiesCount,
                overview.marketCap,
                overview.messageBoardId,
                overview.description,
                overview.marketWeight,
                overview.employeeCount,
            )
        )

    @staticmethod
    def _coerce_period_end(value: Any) -> str | None:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if hasattr(value, "to_pydatetime"):
            try:
                parsed = value.to_pydatetime()
            except TO_PYDATETIME_EXCEPTIONS:
                parsed = None
            if isinstance(parsed, datetime):
                return parsed.date().isoformat()
        if isinstance(value, str):
            try:
                return date.fromisoformat(value[:10]).isoformat()
            except ValueError:
                return None
        return None

    @staticmethod
    def _quarter_label_from_period_end(period_end: str) -> str | None:
        try:
            period_date = date.fromisoformat(period_end)
        except ValueError:
            return None
        quarter = ((period_date.month - 1) // 3) + 1
        return f"Q{quarter} {period_date.year}"

    @staticmethod
    def _analyst_summary_has_no_material_data(summary: AnalystSummary) -> bool:
        return (
            summary.currentPriceTarget is None
            and summary.targetLow is None
            and summary.targetHigh is None
            and summary.targetMean is None
            and summary.targetMedian is None
            and not YFinanceService._public_recommendation_has_material_data(
                summary.recommendationSummary
            )
            and summary.recentActionCount == 0
        )

    @staticmethod
    def _public_recommendation_has_material_data(
        recommendation: AnalystRecommendationBreakdown,
    ) -> bool:
        return any(
            value is not None
            for value in (
                recommendation.period,
                recommendation.strongBuy,
                recommendation.buy,
                recommendation.hold,
                recommendation.sell,
                recommendation.strongSell,
            )
        )

    @staticmethod
    def _build_public_recommendation_breakdown(
        payload: AnalystRecommendationSnapshot,
    ) -> AnalystRecommendationBreakdown:
        return AnalystRecommendationBreakdown(
            period=payload.period,
            strongBuy=payload.strong_buy,
            buy=payload.buy,
            hold=payload.hold,
            sell=payload.sell,
            strongSell=payload.strong_sell,
        )

    def _extract_recommendation_history(
        self,
        payload: Any,
    ) -> list[AnalystRecommendationBreakdown]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        history: list[AnalystRecommendationBreakdown] = []
        for _, row in rows:
            row_mapping = self._coerce_mapping(row)
            item = AnalystRecommendationBreakdown(
                period=coerce_str(row_mapping.get("period")),
                strongBuy=self._coerce_non_negative_int(row_mapping.get("strongBuy")),
                buy=self._coerce_non_negative_int(row_mapping.get("buy")),
                hold=self._coerce_non_negative_int(row_mapping.get("hold")),
                sell=self._coerce_non_negative_int(row_mapping.get("sell")),
                strongSell=self._coerce_non_negative_int(row_mapping.get("strongSell")),
            )
            if self._public_recommendation_has_material_data(item):
                history.append(item)
        return history

    def _extract_analyst_history_actions(
        self,
        payload: Any,
    ) -> list[AnalystActionTimelineEvent]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        threshold = datetime.now(tz=timezone.utc) - timedelta(days=ANALYST_ACTION_WINDOW_DAYS)
        actions: list[tuple[datetime, AnalystActionTimelineEvent]] = []

        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            graded_at = first_non_null(
                coerce_datetime_string(index),
                coerce_datetime_string(row_mapping.get("gradedAt")),
                coerce_datetime_string(row_mapping.get("epochGradeDate")),
                coerce_datetime_string(row_mapping.get("date")),
            )
            parsed_dt = self._parse_iso_timestamp(graded_at)
            if parsed_dt is None or parsed_dt < threshold:
                continue

            action = AnalystActionTimelineEvent(
                gradedAt=graded_at,
                firm=coerce_str(row_mapping.get("firm")),
                toGrade=coerce_str(row_mapping.get("toGrade")),
                fromGrade=coerce_str(row_mapping.get("fromGrade")),
                action=coerce_str(row_mapping.get("action")),
                priceTargetAction=coerce_str(row_mapping.get("priceTargetAction")),
                currentPriceTarget=self._coerce_finite_float(row_mapping.get("currentPriceTarget")),
                priorPriceTarget=self._coerce_finite_float(row_mapping.get("priorPriceTarget")),
            )
            actions.append((parsed_dt, action))

        actions.sort(key=lambda item: item[0], reverse=True)
        return [action for _, action in actions[:MAX_ANALYST_HISTORY_ACTION_EVENTS]]

    def _count_recent_analyst_actions(self, payload: Any) -> int:
        rows = self._get_iterrows(payload)
        if rows is None:
            return 0

        threshold = datetime.now(tz=timezone.utc) - timedelta(days=ANALYST_ACTION_WINDOW_DAYS)
        count = 0
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            graded_at = first_non_null(
                coerce_datetime_string(index),
                coerce_datetime_string(row_mapping.get("gradedAt")),
                coerce_datetime_string(row_mapping.get("epochGradeDate")),
                coerce_datetime_string(row_mapping.get("date")),
            )
            parsed_dt = self._parse_iso_timestamp(graded_at)
            if parsed_dt is not None and parsed_dt >= threshold:
                count += 1
        return count

    def _map_major_holder_metrics(self, payload: Any) -> list[MajorHolderMetric]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        metrics: list[MajorHolderMetric] = []
        for index, row in rows:
            row_mapping = self._coerce_mapping(row)
            key = coerce_str(index)
            if key is None:
                continue
            metrics.append(
                MajorHolderMetric(
                    key=key,
                    label=self._format_holder_metric_label(key),
                    value=self._coerce_finite_float(row_mapping.get("Value")),
                )
            )
        return metrics

    def _map_holder_entries(self, payload: Any) -> list[HolderEntry]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        holders: list[HolderEntry] = []
        for _, row in rows:
            row_mapping = self._coerce_mapping(row)
            entry = HolderEntry(
                dateReported=self._coerce_calendar_timestamp(row_mapping.get("Date Reported")),
                holder=coerce_str(row_mapping.get("Holder")),
                pctHeld=self._coerce_finite_float(row_mapping.get("pctHeld")),
                shares=self._coerce_non_negative_int(row_mapping.get("Shares")),
                value=self._coerce_finite_float(row_mapping.get("Value")),
                pctChange=self._coerce_finite_float(row_mapping.get("pctChange")),
            )
            if (
                entry.holder is None
                and entry.pctHeld is None
                and entry.shares is None
                and entry.value is None
            ):
                continue
            holders.append(entry)
        return holders

    def _map_insider_roster_entries(self, payload: Any) -> list[InsiderRosterEntry]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        entries: list[InsiderRosterEntry] = []
        for _, row in rows:
            row_mapping = self._coerce_mapping(row)
            entry = InsiderRosterEntry(
                name=coerce_str(row_mapping.get("Name")),
                position=coerce_str(row_mapping.get("Position")),
                url=coerce_str(row_mapping.get("URL")),
                mostRecentTransaction=coerce_str(row_mapping.get("Most Recent Transaction")),
                latestTransactionDate=self._coerce_calendar_timestamp(
                    row_mapping.get("Latest Transaction Date")
                ),
                sharesOwnedDirectly=self._coerce_non_negative_int(
                    row_mapping.get("Shares Owned Directly")
                ),
                positionDirectDate=self._coerce_calendar_timestamp(
                    row_mapping.get("Position Direct Date")
                ),
            )
            if (
                entry.name is None
                and entry.position is None
                and entry.latestTransactionDate is None
                and entry.sharesOwnedDirectly is None
            ):
                continue
            entries.append(entry)
        return entries

    @staticmethod
    def _slice_paginated_items(items: list[Any], limit: int, offset: int) -> list[Any]:
        if offset >= len(items):
            return []
        return items[offset : offset + limit]

    @staticmethod
    def _build_ownership_pagination(
        *,
        total_available: int,
        limit: int,
        offset: int,
        returned_count: int,
    ) -> OwnershipPagination:
        has_more = offset + returned_count < total_available
        return OwnershipPagination(
            offset=offset,
            limit=limit,
            returnedCount=returned_count,
            totalAvailable=total_available,
            hasMore=has_more,
            nextOffset=offset + returned_count if has_more else None,
        )

    def _map_option_contracts(self, payload: Any) -> list[OptionContract]:
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        contracts: list[OptionContract] = []
        for _, row in rows:
            row_mapping = self._coerce_mapping(row)
            contract_symbol = coerce_str(row_mapping.get("contractSymbol"))
            if contract_symbol is None:
                continue
            contracts.append(
                OptionContract(
                    contractSymbol=contract_symbol,
                    lastTradeDate=self._coerce_calendar_timestamp(row_mapping.get("lastTradeDate")),
                    strike=self._coerce_finite_float(row_mapping.get("strike")),
                    lastPrice=self._coerce_finite_float(row_mapping.get("lastPrice")),
                    bid=self._coerce_finite_float(row_mapping.get("bid")),
                    ask=self._coerce_finite_float(row_mapping.get("ask")),
                    change=self._coerce_finite_float(row_mapping.get("change")),
                    percentChange=self._coerce_finite_float(row_mapping.get("percentChange")),
                    volume=self._coerce_non_negative_int(row_mapping.get("volume")),
                    openInterest=self._coerce_non_negative_int(row_mapping.get("openInterest")),
                    impliedVolatility=self._coerce_finite_float(
                        row_mapping.get("impliedVolatility")
                    ),
                    inTheMoney=coerce_bool(row_mapping.get("inTheMoney")),
                    contractSize=coerce_str(row_mapping.get("contractSize")),
                    currency=coerce_str(row_mapping.get("currency")),
                )
            )
        return contracts

    @staticmethod
    def _format_holder_metric_label(value: str) -> str:
        normalized = value.replace("_", " ").strip()
        if not normalized:
            return value
        words: list[str] = []
        current = normalized[0]
        for char in normalized[1:]:
            if char.isupper() and current and not current[-1].isupper():
                words.append(current)
                current = char
            else:
                current += char
        words.append(current)
        return " ".join(word.capitalize() for word in words)

    @staticmethod
    def _ownership_has_no_material_data(response: OwnershipResponse) -> bool:
        return (
            not response.majorHolders
            and not response.institutionalHolders
            and not response.mutualFundHolders
            and not response.insiderRoster
        )

    @staticmethod
    def _build_overview(
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

    @staticmethod
    def _build_financial_summary(
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

        rows = self._get_iterrows(payload)
        if rows is None:
            return parsed_dates

        for row_index, row in rows:
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
        rows = self._get_iterrows(payload)
        if rows is None:
            return AnalystRecommendationSnapshot()

        candidates: list[dict[str, Any]] = []
        for _index, row in rows:
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
        rows = self._get_iterrows(payload)
        if rows is None:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=ANALYST_ACTION_WINDOW_DAYS)
        parsed_events: list[tuple[datetime | None, AnalystActionEvent]] = []

        for row_index, row in rows:
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
