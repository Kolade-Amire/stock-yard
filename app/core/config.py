from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = Field(default="development", alias="APP_ENV")
    app_name: str = Field(default="Stock Insight Backend", alias="APP_NAME")
    api_prefix: str = Field(default="/api/v1", alias="API_PREFIX")
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


@lru_cache
def get_settings() -> Settings:
    return Settings()
