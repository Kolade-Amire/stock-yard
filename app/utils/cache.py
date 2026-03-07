import time
from threading import Lock
from typing import Generic, TypeVar

T = TypeVar("T")


class TTLCache(Generic[T]):
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = max(0, ttl_seconds)
        self._store: dict[str, tuple[float, T]] = {}
        self._lock = Lock()

    def get(self, key: str) -> T | None:
        if self._ttl_seconds == 0:
            return None

        now = time.monotonic()
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None
            expires_at, value = item
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: T) -> None:
        if self._ttl_seconds == 0:
            return

        expires_at = time.monotonic() + self._ttl_seconds
        with self._lock:
            self._store[key] = (expires_at, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()
