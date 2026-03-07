import re
from datetime import datetime, timezone

from starlette.concurrency import run_in_threadpool

from app.core.errors import ApiError
from app.repositories.analytics_repository import AnalyticsRepository
from app.schemas.analytics import (
    AnalyticsEventIngestRequest,
    AnalyticsEventIngestResponse,
    PopularTicker,
    PopularTickersResponse,
)
from app.utils.rate_limit import SlidingWindowRateLimiter
from app.utils.symbols import is_valid_symbol, normalize_symbol

ALLOWED_EVENT_TYPES = frozenset({"search", "view", "chat_opened", "chat_message"})
WINDOW_PATTERN = re.compile(r"^(?P<value>\d+)(?P<unit>[hd])$")
MAX_WINDOW_SECONDS = 30 * 24 * 60 * 60
MAX_SESSION_ID_LENGTH = 128


class AnalyticsService:
    def __init__(
        self,
        repository: AnalyticsRepository,
        rate_limiter: SlidingWindowRateLimiter,
    ) -> None:
        self._repository = repository
        self._rate_limiter = rate_limiter

    async def ingest_event(
        self,
        payload: AnalyticsEventIngestRequest,
        client_ip: str | None,
    ) -> AnalyticsEventIngestResponse:
        symbol = self._normalize_and_validate_symbol(payload.symbol)
        event_type = self._normalize_and_validate_event_type(payload.eventType)
        session_id = self._normalize_session_id(payload.sessionId)
        rate_limit_key = self._build_rate_limit_key(client_ip=client_ip, session_id=session_id)
        self._enforce_ingest_rate_limit(rate_limit_key)

        created_at_epoch = await run_in_threadpool(
            self._repository.insert_event,
            symbol=symbol,
            event_type=event_type,
            session_id=session_id,
        )
        recorded_at = datetime.fromtimestamp(created_at_epoch, tz=timezone.utc).isoformat().replace(
            "+00:00",
            "Z",
        )
        return AnalyticsEventIngestResponse(
            accepted=True,
            symbol=symbol,
            eventType=event_type,
            sessionId=session_id,
            recordedAt=recorded_at,
        )

    async def get_popular(self, *, window: str, limit: int) -> PopularTickersResponse:
        normalized_window, window_seconds = self._normalize_and_validate_window(window)

        aggregates = await run_in_threadpool(
            self._repository.get_popular_symbols,
            window_seconds=window_seconds,
            limit=limit,
        )
        generated_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")
        return PopularTickersResponse(
            window=normalized_window,
            limit=limit,
            generatedAt=generated_at,
            results=[
                PopularTicker(
                    symbol=aggregate.symbol,
                    score=aggregate.score,
                    totalEvents=aggregate.total_events,
                    searchEvents=aggregate.search_events,
                    viewEvents=aggregate.view_events,
                    chatOpenedEvents=aggregate.chat_opened_events,
                    chatMessageEvents=aggregate.chat_message_events,
                )
                for aggregate in aggregates
            ],
        )

    @staticmethod
    def _normalize_and_validate_symbol(symbol: str) -> str:
        normalized_symbol = normalize_symbol(symbol)
        if not is_valid_symbol(normalized_symbol):
            raise ApiError(
                code="INVALID_SYMBOL",
                message="Ticker symbol format is invalid.",
                status_code=400,
                details={"symbol": symbol},
            )
        return normalized_symbol

    @staticmethod
    def _normalize_and_validate_event_type(event_type: str) -> str:
        normalized_event_type = event_type.strip().lower()
        if normalized_event_type not in ALLOWED_EVENT_TYPES:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Unsupported analytics event type.",
                status_code=400,
                details={
                    "eventType": event_type,
                    "allowedEventTypes": sorted(ALLOWED_EVENT_TYPES),
                },
            )
        return normalized_event_type

    @staticmethod
    def _normalize_session_id(session_id: str | None) -> str | None:
        if session_id is None:
            return None

        normalized_session_id = session_id.strip()
        if not normalized_session_id:
            return None

        if len(normalized_session_id) > MAX_SESSION_ID_LENGTH:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Session ID is too long.",
                status_code=400,
                details={"maxLength": MAX_SESSION_ID_LENGTH},
            )
        return normalized_session_id

    @staticmethod
    def _normalize_and_validate_window(window: str) -> tuple[str, int]:
        normalized_window = window.strip().lower()
        match = WINDOW_PATTERN.fullmatch(normalized_window)
        if match is None:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Invalid analytics window format.",
                status_code=400,
                details={"window": window, "expectedFormat": "e.g. 24h or 7d"},
            )

        amount = int(match.group("value"))
        unit = match.group("unit")
        if amount <= 0:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Analytics window must be greater than zero.",
                status_code=400,
                details={"window": window},
            )

        if unit == "h":
            window_seconds = amount * 60 * 60
        else:
            window_seconds = amount * 24 * 60 * 60

        if window_seconds > MAX_WINDOW_SECONDS:
            raise ApiError(
                code="VALIDATION_ERROR",
                message="Analytics window exceeds the supported range.",
                status_code=400,
                details={"maxWindow": "30d"},
            )

        return normalized_window, window_seconds

    def _enforce_ingest_rate_limit(self, key: str) -> None:
        decision = self._rate_limiter.check(key)
        if decision.allowed:
            return

        raise ApiError(
            code="RATE_LIMITED",
            message="Too many analytics events. Please retry shortly.",
            status_code=429,
            details={
                "limit": self._rate_limiter.max_events,
                "windowSeconds": self._rate_limiter.window_seconds,
                "retryAfterSeconds": decision.retry_after_seconds,
            },
            headers={"Retry-After": str(decision.retry_after_seconds)},
        )

    @staticmethod
    def _build_rate_limit_key(client_ip: str | None, session_id: str | None) -> str:
        normalized_client_ip = (client_ip or "unknown").strip() or "unknown"
        if session_id:
            return f"{normalized_client_ip}:{session_id}"
        return normalized_client_ip
