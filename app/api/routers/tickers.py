from fastapi import APIRouter, Depends, Query

from app.core.dependencies import get_yfinance_service
from app.schemas.ticker import (
    FinancialSummaryResponse,
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
