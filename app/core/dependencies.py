from functools import lru_cache

from app.core.errors import ApiError
from app.core.config import Settings, get_settings
from app.db.sqlite import SQLiteDatabase
from app.providers.llm.base import LLMProvider
from app.providers.llm.gemini_provider import GeminiProvider
from app.providers.llm.openai_compat_provider import OpenAICompatProvider
from app.repositories.analytics_repository import AnalyticsRepository
from app.services.analytics_service import AnalyticsService
from app.services.chat_service import ChatService
from app.services.yfinance_service import YFinanceService
from app.utils.rate_limit import SlidingWindowRateLimiter


def get_app_settings() -> Settings:
    return get_settings()


@lru_cache
def get_yfinance_service() -> YFinanceService:
    settings = get_settings()
    return YFinanceService(
        cache_ttl_overview_seconds=settings.cache_ttl_overview_seconds,
        cache_ttl_history_seconds=settings.cache_ttl_history_seconds,
        cache_ttl_news_seconds=settings.cache_ttl_news_seconds,
        cache_ttl_movers_seconds=settings.cache_ttl_movers_seconds,
        cache_ttl_benchmarks_seconds=settings.cache_ttl_benchmarks_seconds,
        cache_ttl_earnings_calendar_seconds=settings.cache_ttl_earnings_calendar_seconds,
        cache_ttl_sectors_seconds=settings.cache_ttl_sectors_seconds,
        cache_ttl_financials_seconds=settings.cache_ttl_financials_seconds,
        cache_ttl_earnings_seconds=settings.cache_ttl_earnings_seconds,
        cache_ttl_analyst_seconds=settings.cache_ttl_analyst_seconds,
    )


@lru_cache
def get_sqlite_database() -> SQLiteDatabase:
    settings = get_settings()
    return SQLiteDatabase(db_path=settings.sqlite_db_path)


@lru_cache
def get_analytics_repository() -> AnalyticsRepository:
    return AnalyticsRepository(database=get_sqlite_database())


@lru_cache
def get_analytics_rate_limiter() -> SlidingWindowRateLimiter:
    return SlidingWindowRateLimiter(max_events=60, window_seconds=60)


@lru_cache
def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(
        repository=get_analytics_repository(),
        rate_limiter=get_analytics_rate_limiter(),
    )


@lru_cache
def get_llm_provider() -> LLMProvider:
    settings = get_settings()
    if settings.llm_provider == "gemini":
        return GeminiProvider(
            api_key=settings.gemini_api_key,
            model=settings.gemini_model,
        )
    if settings.llm_provider == "openai_compat":
        return OpenAICompatProvider(
            base_url=settings.openai_compat_base_url,
            api_key=settings.openai_compat_api_key,
            model=settings.openai_compat_model,
        )

    raise ApiError(
        code="LLM_ERROR",
        message="Unsupported LLM provider configuration.",
        status_code=500,
        details={"llmProvider": settings.llm_provider},
    )


@lru_cache
def get_chat_service() -> ChatService:
    settings = get_settings()
    return ChatService(
        yfinance_service=get_yfinance_service(),
        llm_provider=get_llm_provider(),
        max_turns=settings.chat_max_turns,
        max_tool_call_rounds=settings.chat_max_tool_call_rounds,
        history_recent_bars_limit=settings.chat_history_recent_bars_limit,
        news_tool_default_limit=settings.chat_news_tool_default_limit,
        session_ttl_seconds=settings.chat_session_ttl_seconds,
        session_max_tool_entries=settings.chat_session_max_tool_entries,
        session_max_sessions=settings.chat_session_max_sessions,
        memo_ttl_overview_seconds=settings.cache_ttl_overview_seconds,
        memo_ttl_history_seconds=settings.cache_ttl_history_seconds,
        memo_ttl_news_seconds=settings.cache_ttl_news_seconds,
        memo_ttl_financials_seconds=settings.cache_ttl_financials_seconds,
        memo_ttl_earnings_seconds=settings.cache_ttl_earnings_seconds,
        memo_ttl_analyst_seconds=settings.cache_ttl_analyst_seconds,
        tool_gating_mode=settings.chat_tool_gating_mode,
    )
