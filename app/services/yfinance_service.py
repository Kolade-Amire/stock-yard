from collections.abc import Mapping
from typing import Any

import yfinance as yf
from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.core.logging import get_logger
from app.schemas.ticker import (
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


class YFinanceService:
    def __init__(self, *, cache_ttl_overview_seconds: int) -> None:
        self._logger = get_logger(__name__)
        self._overview_cache = TTLCache[TickerOverviewResponse](cache_ttl_overview_seconds)

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
        normalized_symbol = normalize_symbol(symbol)
        if not is_valid_symbol(normalized_symbol):
            raise ApiError(
                code="INVALID_SYMBOL",
                message="Ticker symbol format is invalid.",
                status_code=400,
                details={"symbol": symbol},
            )

        cached = self._overview_cache.get(normalized_symbol)
        if cached is not None:
            self._logger.info("Overview cache hit for %s", normalized_symbol)
            return cached

        response = await run_in_threadpool(self._get_ticker_overview_sync, normalized_symbol)
        self._overview_cache.set(normalized_symbol, response)
        return response

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

        limitations = self._build_data_limitations(overview)
        return TickerOverviewResponse(
            symbol=symbol,
            overview=overview,
            dataLimitations=limitations,
        )

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
    def _build_data_limitations(overview: TickerOverview) -> list[str]:
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
