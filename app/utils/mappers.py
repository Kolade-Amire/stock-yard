from datetime import date, datetime, timezone
from typing import Any

DATETIME_COERCION_EXCEPTIONS = (
    AttributeError,
    TypeError,
    ValueError,
    OverflowError,
    OSError,
)


def first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def coerce_datetime_string(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, list) and value:
        return coerce_datetime_string(value[0])

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace(
            "+00:00",
            "Z",
        )

    if isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(value, tz=timezone.utc)
            return parsed.isoformat().replace("+00:00", "Z")
        except (OverflowError, OSError, ValueError):
            return None

    if hasattr(value, "to_pydatetime"):
        try:
            return coerce_datetime_string(value.to_pydatetime())
        except DATETIME_COERCION_EXCEPTIONS:
            return None

    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except DATETIME_COERCION_EXCEPTIONS:
            return None

    return coerce_str(value)
