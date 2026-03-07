from fastapi import APIRouter, Depends, Query, Request, status

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
    request: Request,
    service: AnalyticsService = Depends(get_analytics_service),
) -> AnalyticsEventIngestResponse:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    forwarded_ip = forwarded_for.split(",", maxsplit=1)[0].strip() if forwarded_for else ""
    client_ip = forwarded_ip or (request.client.host if request.client else None)
    return await service.ingest_event(payload, client_ip=client_ip)


@router.get("/popular", response_model=PopularTickersResponse)
async def get_popular_tickers(
    window: str = Query(default="24h", min_length=2, max_length=8),
    limit: int = Query(default=10, ge=1, le=50),
    service: AnalyticsService = Depends(get_analytics_service),
) -> PopularTickersResponse:
    return await service.get_popular(window=window, limit=limit)
