from fastapi import APIRouter, Depends, Query

from app.core.dependencies import get_yfinance_service
from app.schemas.ticker import TickerOverviewResponse, TickerSearchResponse
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
