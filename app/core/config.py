from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = Field(default="development", alias="APP_ENV")
    app_name: str = Field(default="Stock Insight Backend", alias="APP_NAME")
    api_prefix: str = Field(default="/api/v1", alias="API_PREFIX")
    sqlite_db_path: str = Field(default="./data/app.db", alias="SQLITE_DB_PATH")
    llm_provider: str = Field(default="gemini", alias="LLM_PROVIDER")
    gemini_api_key: str | None = Field(default=None, alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.5-flash", alias="GEMINI_MODEL")
    openai_compat_base_url: str = Field(
        default="http://localhost:1234/v1",
        alias="OPENAI_COMPAT_BASE_URL",
    )
    openai_compat_api_key: str = Field(default="dummy", alias="OPENAI_COMPAT_API_KEY")
    openai_compat_model: str = Field(default="local-model", alias="OPENAI_COMPAT_MODEL")
    chat_max_turns: int = Field(default=6, alias="CHAT_MAX_TURNS", ge=1, le=100)
    chat_max_tool_call_rounds: int = Field(
        default=2,
        alias="CHAT_MAX_TOOL_CALL_ROUNDS",
        ge=1,
        le=10,
    )
    chat_history_recent_bars_limit: int = Field(
        default=12,
        alias="CHAT_HISTORY_RECENT_BARS_LIMIT",
        ge=1,
        le=100,
    )
    chat_news_tool_default_limit: int = Field(
        default=3,
        alias="CHAT_NEWS_TOOL_DEFAULT_LIMIT",
        ge=1,
        le=10,
    )
    chat_tool_gating_mode: str = Field(
        default="balanced",
        alias="CHAT_TOOL_GATING_MODE",
    )
    trust_proxy_headers: bool = Field(
        default=False,
        alias="TRUST_PROXY_HEADERS",
    )
    allowed_origins: list[str] = Field(
        default_factory=lambda: ["http://localhost:3000"],
        alias="ALLOWED_ORIGINS",
    )
    cache_ttl_overview_seconds: int = Field(
        default=300,
        alias="CACHE_TTL_OVERVIEW_SECONDS",
    )
    cache_ttl_history_seconds: int = Field(
        default=300,
        alias="CACHE_TTL_HISTORY_SECONDS",
    )
    cache_ttl_news_seconds: int = Field(
        default=900,
        alias="CACHE_TTL_NEWS_SECONDS",
    )
    cache_ttl_financials_seconds: int = Field(
        default=3600,
        alias="CACHE_TTL_FINANCIALS_SECONDS",
    )
    cache_ttl_earnings_seconds: int = Field(
        default=3600,
        alias="CACHE_TTL_EARNINGS_SECONDS",
    )
    cache_ttl_analyst_seconds: int = Field(
        default=3600,
        alias="CACHE_TTL_ANALYST_SECONDS",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
        extra="ignore",
    )

    @field_validator("allowed_origins", mode="before")
    @staticmethod
    def parse_allowed_origins(value: str | list[str]) -> list[str]:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("llm_provider", mode="before")
    @staticmethod
    def parse_llm_provider(value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"gemini", "openai_compat"}:
            raise ValueError("LLM_PROVIDER must be either 'gemini' or 'openai_compat'.")
        return normalized

    @field_validator("chat_tool_gating_mode", mode="before")
    @staticmethod
    def parse_chat_tool_gating_mode(value: str) -> str:
        normalized = value.strip().lower()
        if normalized != "balanced":
            raise ValueError("CHAT_TOOL_GATING_MODE must be 'balanced'.")
        return normalized


@lru_cache
def get_settings() -> Settings:
    return Settings()
