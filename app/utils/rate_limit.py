import time
from collections import deque
from dataclasses import dataclass
from math import ceil
from threading import Lock


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int


class SlidingWindowRateLimiter:
    def __init__(self, *, max_events: int, window_seconds: int) -> None:
        self._max_events = max(1, max_events)
        self._window_seconds = max(1, window_seconds)
        self._events_by_key: dict[str, deque[float]] = {}
        self._cleanup_interval = 500
        self._checks_since_cleanup = 0
        self._lock = Lock()

    @property
    def max_events(self) -> int:
        return self._max_events

    @property
    def window_seconds(self) -> int:
        return self._window_seconds

    def check(self, key: str) -> RateLimitDecision:
        now = time.monotonic()
        with self._lock:
            # Periodic global cleanup keeps memory bounded for stale client keys.
            self._checks_since_cleanup += 1
            if self._checks_since_cleanup >= self._cleanup_interval:
                self._cleanup_stale_keys(now)
                self._checks_since_cleanup = 0

            events = self._events_by_key.get(key)
            if events is None:
                events = deque()

            self._prune_old_events(events, now)
            if len(events) >= self._max_events:
                oldest_event = events[0]
                retry_after = ceil(self._window_seconds - (now - oldest_event))
                return RateLimitDecision(allowed=False, retry_after_seconds=max(1, retry_after))

            events.append(now)
            self._events_by_key[key] = events
            return RateLimitDecision(allowed=True, retry_after_seconds=0)

    def _prune_old_events(self, events: deque[float], now: float) -> None:
        threshold = now - self._window_seconds
        while events and events[0] <= threshold:
            events.popleft()

    def _cleanup_stale_keys(self, now: float) -> None:
        stale_keys: list[str] = []
        for key, events in self._events_by_key.items():
            self._prune_old_events(events, now)
            if not events:
                stale_keys.append(key)

        for stale_key in stale_keys:
            self._events_by_key.pop(stale_key, None)
