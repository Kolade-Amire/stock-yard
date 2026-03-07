from functools import lru_cache

from app.core.config import Settings, get_settings
from app.services.yfinance_service import YFinanceService


def get_app_settings() -> Settings:
    return get_settings()


@lru_cache
def get_yfinance_service() -> YFinanceService:
    settings = get_settings()
    return YFinanceService(cache_ttl_overview_seconds=settings.cache_ttl_overview_seconds)
