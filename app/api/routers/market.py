from fastapi import APIRouter, Depends, Query

from app.core.dependencies import get_yfinance_service
from app.schemas.market import (
    BenchmarkFundsResponse,
    EarningsCalendarResponse,
    IndustryDetailResponse,
    MarketMoversResponse,
    SectorDetailResponse,
    SectorPulseResponse,
)
from app.services.yfinance_service import YFinanceService

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/movers", response_model=MarketMoversResponse)
async def get_market_movers(
    screen: str = Query(..., min_length=1),
    limit: int = Query(default=10),
    service: YFinanceService = Depends(get_yfinance_service),
) -> MarketMoversResponse:
    return await service.get_market_movers(screen=screen, limit=limit)


@router.get("/benchmarks", response_model=BenchmarkFundsResponse)
async def get_benchmark_funds(
    service: YFinanceService = Depends(get_yfinance_service),
) -> BenchmarkFundsResponse:
    return await service.get_benchmark_funds()


@router.get("/earnings-calendar", response_model=EarningsCalendarResponse)
async def get_earnings_calendar(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    offset: int = Query(default=0),
    active_only: bool = Query(default=True, alias="activeOnly"),
    service: YFinanceService = Depends(get_yfinance_service),
) -> EarningsCalendarResponse:
    return await service.get_earnings_calendar(
        start=start,
        end=end,
        limit=limit,
        offset=offset,
        active_only=active_only,
    )


@router.get("/sectors/pulse", response_model=SectorPulseResponse)
async def get_sector_pulse(
    service: YFinanceService = Depends(get_yfinance_service),
) -> SectorPulseResponse:
    return await service.get_sector_pulse()


@router.get("/sectors/{sector_key}", response_model=SectorDetailResponse)
async def get_sector_detail(
    sector_key: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> SectorDetailResponse:
    return await service.get_sector_detail(sector_key=sector_key)


@router.get("/industries/{industry_key}", response_model=IndustryDetailResponse)
async def get_industry_detail(
    industry_key: str,
    service: YFinanceService = Depends(get_yfinance_service),
) -> IndustryDetailResponse:
    return await service.get_industry_detail(industry_key=industry_key)
