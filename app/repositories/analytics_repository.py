import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock

from app.core.errors import ApiError
from app.db.sqlite import SQLiteDatabase

EVENT_WEIGHTS: dict[str, int] = {
    "search": 1,
    "view": 2,
    "chat_opened": 0,
    "chat_message": 3,
}


@dataclass(frozen=True)
class PopularSymbolAggregate:
    symbol: str
    score: int
    total_events: int
    search_events: int
    view_events: int
    chat_opened_events: int
    chat_message_events: int


class AnalyticsRepository:
    def __init__(self, database: SQLiteDatabase) -> None:
        self._database = database
        self._schema_initialized = False
        self._schema_lock = Lock()

    def insert_event(
        self,
        *,
        symbol: str,
        event_type: str,
        session_id: str | None,
    ) -> int:
        self._ensure_schema()

        created_at = int(datetime.now(tz=timezone.utc).timestamp())
        try:
            with self._database.connect() as connection:
                connection.execute(
                    """
                    INSERT INTO analytics_events (symbol, event_type, session_id, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (symbol, event_type, session_id, created_at),
                )
                connection.commit()
        except sqlite3.Error as exc:
            raise ApiError(
                code="INTERNAL_ERROR",
                message="Failed to record analytics event.",
                status_code=500,
            ) from exc

        return created_at

    def get_popular_symbols(
        self,
        *,
        window_seconds: int,
        limit: int,
    ) -> list[PopularSymbolAggregate]:
        self._ensure_schema()

        cutoff_timestamp = int(datetime.now(tz=timezone.utc).timestamp()) - window_seconds

        try:
            with self._database.connect() as connection:
                rows = connection.execute(
                    """
                    SELECT
                        symbol,
                        SUM(
                            CASE event_type
                                WHEN 'search' THEN ?
                                WHEN 'view' THEN ?
                                WHEN 'chat_opened' THEN ?
                                WHEN 'chat_message' THEN ?
                                ELSE 0
                            END
                        ) AS score,
                        COUNT(*) AS total_events,
                        SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_events,
                        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS view_events,
                        SUM(CASE WHEN event_type = 'chat_opened' THEN 1 ELSE 0 END) AS chat_opened_events,
                        SUM(CASE WHEN event_type = 'chat_message' THEN 1 ELSE 0 END) AS chat_message_events
                    FROM analytics_events
                    WHERE created_at >= ?
                    GROUP BY symbol
                    ORDER BY score DESC, total_events DESC, symbol ASC
                    LIMIT ?
                    """,
                    (
                        EVENT_WEIGHTS["search"],
                        EVENT_WEIGHTS["view"],
                        EVENT_WEIGHTS["chat_opened"],
                        EVENT_WEIGHTS["chat_message"],
                        cutoff_timestamp,
                        limit,
                    ),
                ).fetchall()
        except sqlite3.Error as exc:
            raise ApiError(
                code="INTERNAL_ERROR",
                message="Failed to read analytics aggregates.",
                status_code=500,
            ) from exc

        return [
            PopularSymbolAggregate(
                symbol=str(row["symbol"]),
                score=int(row["score"] or 0),
                total_events=int(row["total_events"] or 0),
                search_events=int(row["search_events"] or 0),
                view_events=int(row["view_events"] or 0),
                chat_opened_events=int(row["chat_opened_events"] or 0),
                chat_message_events=int(row["chat_message_events"] or 0),
            )
            for row in rows
        ]

    def _ensure_schema(self) -> None:
        if self._schema_initialized:
            return

        with self._schema_lock:
            if self._schema_initialized:
                return
            self._create_schema()
            self._schema_initialized = True

    def _create_schema(self) -> None:
        try:
            with self._database.connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS analytics_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        event_type TEXT NOT NULL CHECK (
                            event_type IN ('search', 'view', 'chat_opened', 'chat_message')
                        ),
                        session_id TEXT,
                        created_at INTEGER NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_analytics_events_created_at
                    ON analytics_events (created_at)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_analytics_events_symbol_created_at
                    ON analytics_events (symbol, created_at)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_analytics_events_event_type_created_at
                    ON analytics_events (event_type, created_at)
                    """
                )
                connection.commit()
        except sqlite3.Error as exc:
            raise ApiError(
                code="INTERNAL_ERROR",
                message="Failed to initialize analytics database schema.",
                status_code=500,
            ) from exc
