from fastapi import APIRouter, Depends, Query

from app.core.dependencies import get_yfinance_service
from app.schemas.ticker import (
    AnalystHistoryResponse,
    AnalystSummaryResponse,
    EarningsEstimatesResponse,
    EarningsHistoryResponse,
    FinancialSummaryResponse,
    FinancialTrendsResponse,
    OptionsChainResponse,
    OptionsExpirationsResponse,
    OwnershipResponse,
    TickerCompareResponse,
    TickerHistoryResponse,
    TickerNewsResponse,
    TickerOverviewResponse,
    TickerSearchResponse,
)
from app.services.yfinance_service import YFinanceService

router = APIRouter(prefix="/tickers", tags=["tickers"])


@router.get("/search", response_model=TickerSearchResponse)
async def search_tickers(
    q: str = Query(..., min_length=1),
    service: YFinanceService = Depends(get_yfinance_service),
) -> TickerSearchResponse:
    return await service.search_tickers(query=q, limit=10)


@router.get("/compare", response_model=TickerCompareResponse)
async def compare_tickers(
    symbols: str = Query(..., min_length=1),
    period: str = Query(..., min_length=1),
    interval: str = Query(..., min_length=1),
    service: YFinanceService = Depends(get_yfinance_service),
) -> TickerCompareResponse:
    return await service.compare_tickers(symbols=symbols, period=period, interval=interval)


@router.get("/{symbol}", response_model=TickerOverviewResponse)
async def get_ticker_overview(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> TickerOverviewResponse:
    return await service.get_ticker_overview(symbol=symbol)


@router.get("/{symbol}/history", response_model=TickerHistoryResponse)
async def get_ticker_history(
    symbol: str,
    period: str = Query(..., min_length=1),
    interval: str = Query(..., min_length=1),
    service: YFinanceService = Depends(get_yfinance_service),
) -> TickerHistoryResponse:
    return await service.get_ticker_history(symbol=symbol, period=period, interval=interval)


@router.get("/{symbol}/news", response_model=TickerNewsResponse)
async def get_ticker_news(
    symbol: str,
    limit: int = Query(default=10, ge=1, le=50),
    service: YFinanceService = Depends(get_yfinance_service),
) -> TickerNewsResponse:
    return await service.get_ticker_news(symbol=symbol, limit=limit)


@router.get("/{symbol}/financial-summary", response_model=FinancialSummaryResponse)
async def get_financial_summary(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> FinancialSummaryResponse:
    return await service.get_financial_summary(symbol=symbol)


@router.get("/{symbol}/financials/trends", response_model=FinancialTrendsResponse)
async def get_financial_trends(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> FinancialTrendsResponse:
    return await service.get_financial_trends(symbol=symbol)


@router.get("/{symbol}/earnings/history", response_model=EarningsHistoryResponse)
async def get_earnings_history(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> EarningsHistoryResponse:
    return await service.get_earnings_history(symbol=symbol)


@router.get("/{symbol}/earnings/estimates", response_model=EarningsEstimatesResponse)
async def get_earnings_estimates(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> EarningsEstimatesResponse:
    return await service.get_earnings_estimates(symbol=symbol)


@router.get("/{symbol}/analyst/summary", response_model=AnalystSummaryResponse)
async def get_analyst_summary(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> AnalystSummaryResponse:
    return await service.get_analyst_summary(symbol=symbol)


@router.get("/{symbol}/analyst/history", response_model=AnalystHistoryResponse)
async def get_analyst_history(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> AnalystHistoryResponse:
    return await service.get_analyst_history(symbol=symbol)


@router.get("/{symbol}/ownership", response_model=OwnershipResponse)
async def get_ticker_ownership(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> OwnershipResponse:
    return await service.get_ticker_ownership(symbol=symbol)


@router.get("/{symbol}/options/expirations", response_model=OptionsExpirationsResponse)
async def get_option_expirations(
    symbol: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> OptionsExpirationsResponse:
    return await service.get_option_expirations(symbol=symbol)


@router.get("/{symbol}/options/chain", response_model=OptionsChainResponse)
async def get_option_chain(
    symbol: str,
    expiration: str = Query(..., min_length=1),
    service: YFinanceService = Depends(get_yfinance_service),
) -> OptionsChainResponse:
    return await service.get_option_chain(symbol=symbol, expiration=expiration)
