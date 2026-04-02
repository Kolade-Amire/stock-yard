import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.core.dependencies import get_yfinance_service
from app.core.errors import ApiError
from app.main import app
from app.schemas.ticker import (
    TickerOverview,
    TickerOverviewResponse,
)
from app.services.yfinance_service import YFinanceService


def _build_service() -> YFinanceService:
    return YFinanceService(
        cache_ttl_overview_seconds=60,
        cache_ttl_history_seconds=60,
        cache_ttl_news_seconds=60,
        cache_ttl_movers_seconds=60,
        cache_ttl_benchmarks_seconds=60,
        cache_ttl_earnings_calendar_seconds=60,
        cache_ttl_sectors_seconds=60,
        cache_ttl_financials_seconds=60,
        cache_ttl_earnings_seconds=60,
        cache_ttl_analyst_seconds=60,
    )


class YFinanceServiceSymbolValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = _build_service()

    def test_normalize_and_validate_symbol_accepts_supported_symbol_shapes(self) -> None:
        accepted_symbols = ("AAPL", "MSFT", "AMZN", "BRK-B", "BTC-USD", "^GSPC")

        for symbol in accepted_symbols:
            with self.subTest(symbol=symbol):
                self.assertEqual(self.service._normalize_and_validate_symbol(symbol), symbol)

    def test_normalize_and_validate_symbol_rejects_obvious_company_names(self) -> None:
        rejected_symbols = ("MICROSOFT", "AMAZON")

        for symbol in rejected_symbols:
            with self.subTest(symbol=symbol):
                with self.assertRaises(ApiError) as context:
                    self.service._normalize_and_validate_symbol(symbol)

                self.assertEqual(context.exception.code, "INVALID_SYMBOL")
                self.assertEqual(context.exception.status_code, 400)
                self.assertEqual(
                    context.exception.message,
                    "Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                )
                self.assertEqual(context.exception.details, {"symbol": symbol})

    def test_normalize_and_validate_symbol_rejects_unresolved_five_letter_name(self) -> None:
        with patch.object(self.service, "_has_exact_symbol_search_match", return_value=False) as match_mock:
            with self.assertRaises(ApiError) as context:
                self.service._normalize_and_validate_symbol("APPLE")

        self.assertEqual(context.exception.code, "INVALID_SYMBOL")
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(
            context.exception.message,
            "Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
        )
        self.assertEqual(context.exception.details, {"symbol": "APPLE"})
        match_mock.assert_called_once_with("APPLE")

    def test_normalize_and_validate_symbol_accepts_exact_five_letter_symbol_match(self) -> None:
        with patch.object(self.service, "_has_exact_symbol_search_match", return_value=True) as match_mock:
            normalized_symbol = self.service._normalize_and_validate_symbol("GOOGL")

        self.assertEqual(normalized_symbol, "GOOGL")
        match_mock.assert_called_once_with("GOOGL")


class YFinanceServiceApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = _build_service()
        app.dependency_overrides[get_yfinance_service] = lambda: self.service
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self.client.close()

    def test_overview_endpoint_rejects_company_name_before_provider_call(self) -> None:
        with patch.object(self.service, "_get_ticker_overview_sync") as overview_mock:
            response = self.client.get("/api/v1/tickers/MICROSOFT")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "INVALID_SYMBOL",
                    "message": "Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                    "details": {"symbol": "MICROSOFT"},
                }
            },
        )
        overview_mock.assert_not_called()

    def test_history_endpoint_rejects_company_name_before_provider_call(self) -> None:
        with patch.object(self.service, "_get_ticker_history_sync") as history_mock:
            response = self.client.get(
                "/api/v1/tickers/AMAZON/history",
                params={"period": "6mo", "interval": "1d"},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "INVALID_SYMBOL",
                    "message": "Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                    "details": {"symbol": "AMAZON"},
                }
            },
        )
        history_mock.assert_not_called()

    def test_overview_endpoint_rejects_unresolved_five_letter_name_before_provider_call(self) -> None:
        with (
            patch.object(self.service, "_has_exact_symbol_search_match", return_value=False) as match_mock,
            patch.object(self.service, "_get_ticker_overview_sync") as overview_mock,
        ):
            response = self.client.get("/api/v1/tickers/APPLE")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "INVALID_SYMBOL",
                    "message": "Ticker symbol must be a valid market symbol such as AAPL or MSFT.",
                    "details": {"symbol": "APPLE"},
                }
            },
        )
        match_mock.assert_called_once_with("APPLE")
        overview_mock.assert_not_called()

    def test_overview_endpoint_keeps_valid_symbol_flow(self) -> None:
        overview_response = TickerOverviewResponse(
            symbol="AAPL",
            overview=TickerOverview(display_name="Apple Inc.", current_price=190.0),
            dataLimitations=[],
        )

        with patch.object(
            self.service,
            "_get_ticker_overview_sync",
            return_value=overview_response,
        ) as overview_mock:
            response = self.client.get("/api/v1/tickers/AAPL")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), overview_response.model_dump())
        overview_mock.assert_called_once_with("AAPL")

    def test_search_endpoint_still_returns_provider_symbol_for_free_text_query(self) -> None:
        with patch.object(
            self.service,
            "_fetch_search_quotes",
            return_value=[
                {
                    "symbol": "AMZN",
                    "shortname": "Amazon.com, Inc.",
                    "exchange": "NMS",
                    "quoteType": "EQUITY",
                }
            ],
        ) as search_mock:
            response = self.client.get("/api/v1/tickers/search", params={"q": "amazon"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "query": "amazon",
                "results": [
                    {
                        "symbol": "AMZN",
                        "name": "Amazon.com, Inc.",
                        "exchange": "NMS",
                        "quoteType": "EQUITY",
                    }
                ],
            },
        )
        search_mock.assert_called_once_with(query="amazon", limit=10)

    def test_coerce_mapping_returns_empty_dict_for_expected_key_error(self) -> None:
        class _KeyErrorMapping:
            def items(self) -> list[tuple[str, str]]:
                raise KeyError("currency")

        self.assertEqual(self.service._coerce_mapping(_KeyErrorMapping()), {})


if __name__ == "__main__":
    unittest.main()
