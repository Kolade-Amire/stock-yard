from functools import lru_cache

from app.core.config import Settings, get_settings
from app.db.sqlite import SQLiteDatabase
from app.repositories.analytics_repository import AnalyticsRepository
from app.services.analytics_service import AnalyticsService
from app.services.yfinance_service import YFinanceService


def get_app_settings() -> Settings:
    return get_settings()


@lru_cache
def get_yfinance_service() -> YFinanceService:
    settings = get_settings()
    return YFinanceService(
        cache_ttl_overview_seconds=settings.cache_ttl_overview_seconds,
        cache_ttl_history_seconds=settings.cache_ttl_history_seconds,
        cache_ttl_news_seconds=settings.cache_ttl_news_seconds,
        cache_ttl_financials_seconds=settings.cache_ttl_financials_seconds,
    )


@lru_cache
def get_sqlite_database() -> SQLiteDatabase:
    settings = get_settings()
    return SQLiteDatabase(db_path=settings.sqlite_db_path)


@lru_cache
def get_analytics_repository() -> AnalyticsRepository:
    return AnalyticsRepository(database=get_sqlite_database())


@lru_cache
def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(repository=get_analytics_repository())
