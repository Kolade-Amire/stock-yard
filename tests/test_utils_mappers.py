import unittest
from datetime import datetime, timezone

from app.utils.mappers import coerce_datetime_string


class _ToPyDateTimeValueError:
    def to_pydatetime(self) -> datetime:
        raise ValueError("invalid datetime payload")


class _ToPyDateTimeRuntimeError:
    def to_pydatetime(self) -> datetime:
        raise RuntimeError("unexpected conversion bug")


class _IsoFormatValueError:
    def isoformat(self) -> str:
        raise ValueError("invalid isoformat payload")


class _IsoFormatRuntimeError:
    def isoformat(self) -> str:
        raise RuntimeError("unexpected isoformat bug")


class UtilsMappersTests(unittest.TestCase):
    def test_coerce_datetime_string_formats_datetime(self) -> None:
        value = datetime(2026, 3, 15, 10, 30, tzinfo=timezone.utc)

        result = coerce_datetime_string(value)

        self.assertEqual(result, "2026-03-15T10:30:00Z")

    def test_coerce_datetime_string_returns_none_for_expected_to_pydatetime_failure(self) -> None:
        self.assertIsNone(coerce_datetime_string(_ToPyDateTimeValueError()))

    def test_coerce_datetime_string_returns_none_for_expected_isoformat_failure(self) -> None:
        self.assertIsNone(coerce_datetime_string(_IsoFormatValueError()))

    def test_coerce_datetime_string_propagates_unexpected_to_pydatetime_error(self) -> None:
        with self.assertRaises(RuntimeError):
            coerce_datetime_string(_ToPyDateTimeRuntimeError())

    def test_coerce_datetime_string_propagates_unexpected_isoformat_error(self) -> None:
        with self.assertRaises(RuntimeError):
            coerce_datetime_string(_IsoFormatRuntimeError())


if __name__ == "__main__":
    unittest.main()
