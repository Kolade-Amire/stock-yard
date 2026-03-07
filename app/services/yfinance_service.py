from collections.abc import Mapping
from datetime import datetime, timezone
from math import isfinite
from typing import Any

import yfinance as yf
from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.schemas.ticker import (
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
MAX_NEWS_LIMIT = 50
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
        cache_ttl_financials_seconds: int,
    ) -> None:
        self._logger = get_logger(__name__)
        self._overview_cache = TTLCache[TickerOverviewResponse](cache_ttl_overview_seconds)
        self._history_cache = TTLCache[TickerHistoryResponse](cache_ttl_history_seconds)
        self._news_cache = TTLCache[TickerNewsResponse](cache_ttl_news_seconds)
        self._financial_summary_cache = TTLCache[FinancialSummaryResponse](
            cache_ttl_financials_seconds
        )

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
