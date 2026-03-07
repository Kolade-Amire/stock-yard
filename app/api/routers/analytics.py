from fastapi import APIRouter, Depends, Query, status

from app.core.dependencies import get_analytics_service
from app.schemas.analytics import (
    AnalyticsEventIngestRequest,
    AnalyticsEventIngestResponse,
    PopularTickersResponse,
)
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.post(
    "/events",
    response_model=AnalyticsEventIngestResponse,
    status_code=status.HTTP_201_CREATED,
)
async def ingest_analytics_event(
    payload: AnalyticsEventIngestRequest,
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsEventIngestResponse:
    return await service.ingest_event(payload)


@router.get("/popular", response_model=PopularTickersResponse)
async def get_popular_tickers(
    window: str = Query(default="24h", min_length=2, max_length=8),
    limit: int = Query(default=10, ge=1, le=50),
    service: AnalyticsService = Depends(get_analytics_service),
) -> PopularTickersResponse:
    return await service.get_popular(window=window, limit=limit)
