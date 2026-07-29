"""Tests for the Data Cloud type system (types.py).

Focus: the off-core Query v3 API returns LOWERCASE type names, while the
Postgres metadata catalog emits CAPITALIZED names. Both must convert and map
identically (Finding C).
"""

from datetime import date, datetime
from decimal import Decimal

import pytest

from salesforce_datacloud_connector.types import (
    DATETIME,
    NUMBER,
    STRING,
    build_description_tuple,
    convert_datacloud_value,
)


class TestConvertLowercaseV3Types:
    """v3 wire sends lowercase type names — the primary Finding C path."""

    def test_varchar(self):
        assert convert_datacloud_value("hello", "varchar") == "hello"
        assert isinstance(convert_datacloud_value("hello", "varchar"), str)

    def test_numeric_scale_zero_is_int(self):
        result = convert_datacloud_value("42", "numeric", scale=0)
        assert result == 42
        assert isinstance(result, int)

    def test_numeric_low_precision_is_float(self):
        result = convert_datacloud_value("3.14", "numeric", precision=10, scale=2)
        assert result == pytest.approx(3.14)
        assert isinstance(result, float)

    def test_numeric_high_precision_is_decimal(self):
        result = convert_datacloud_value("123.456789", "numeric", precision=20, scale=6)
        assert result == Decimal("123.456789")
        assert isinstance(result, Decimal)

    def test_integer_and_bigint_are_int(self):
        assert convert_datacloud_value("7", "integer") == 7
        assert isinstance(convert_datacloud_value("7", "integer"), int)
        assert convert_datacloud_value("9000000000", "bigint") == 9000000000
        assert isinstance(convert_datacloud_value("9000000000", "bigint"), int)

    def test_float_and_double_are_float(self):
        assert convert_datacloud_value("1.5", "float") == pytest.approx(1.5)
        assert isinstance(convert_datacloud_value("1.5", "float"), float)
        assert convert_datacloud_value("2.25", "double") == pytest.approx(2.25)
        assert isinstance(convert_datacloud_value("2.25", "double"), float)

    def test_timestamptz_is_tzaware_datetime(self):
        result = convert_datacloud_value("2024-01-15T10:30:00+00:00", "timestamptz")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_date_is_date(self):
        result = convert_datacloud_value("2024-01-15", "date")
        assert result == date(2024, 1, 15)
        assert isinstance(result, date)

    def test_boolean_from_strings(self):
        assert convert_datacloud_value("true", "boolean") is True
        assert convert_datacloud_value("false", "boolean") is False
        assert convert_datacloud_value("t", "boolean") is True
        assert convert_datacloud_value("no", "boolean") is False

    def test_boolean_from_bool(self):
        assert convert_datacloud_value(True, "boolean") is True
        assert convert_datacloud_value(False, "boolean") is False

    def test_text_is_str(self):
        assert convert_datacloud_value(123, "text") == "123"
        assert isinstance(convert_datacloud_value(123, "text"), str)


class TestConvertCapitalizedTypesRegression:
    """PG catalog / canonical names are capitalized — must still convert (no regression)."""

    def test_capitalized_varchar(self):
        assert convert_datacloud_value("hello", "Varchar") == "hello"

    def test_capitalized_numeric_scale_zero(self):
        result = convert_datacloud_value("42", "Numeric", scale=0)
        assert result == 42
        assert isinstance(result, int)

    def test_capitalized_timestamptz(self):
        result = convert_datacloud_value("2024-01-15T10:30:00+00:00", "TimestampTZ")
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_capitalized_date(self):
        assert convert_datacloud_value("2024-01-15", "Date") == date(2024, 1, 15)

    def test_capitalized_boolean(self):
        assert convert_datacloud_value("true", "Boolean") is True

    def test_capitalized_integer(self):
        assert convert_datacloud_value("7", "Integer") == 7


class TestConvertEdgeCases:
    def test_null_returns_none_regardless_of_type(self):
        assert convert_datacloud_value(None, "numeric") is None
        assert convert_datacloud_value(None, "Varchar") is None
        assert convert_datacloud_value(None, "timestamptz") is None

    def test_unknown_type_returns_value_as_is(self):
        sentinel = object()
        assert convert_datacloud_value(sentinel, "geography") is sentinel

    def test_invalid_numeric_raises_valueerror_with_original_type_name(self):
        with pytest.raises(ValueError, match="numeric"):
            convert_datacloud_value("not-a-number", "numeric", scale=0)


class TestBuildDescriptionTuple:
    """type_code lookup must be case-insensitive for both wire shapes."""

    def test_lowercase_numeric_maps_to_number(self):
        desc = build_description_tuple({"name": "age", "type": "numeric", "scale": 0})
        assert desc[0] == "age"
        assert desc[1] == NUMBER

    def test_lowercase_timestamptz_maps_to_datetime(self):
        desc = build_description_tuple({"name": "created", "type": "timestamptz"})
        assert desc[1] == DATETIME

    def test_lowercase_varchar_maps_to_string(self):
        desc = build_description_tuple({"name": "label", "type": "varchar"})
        assert desc[1] == STRING

    def test_capitalized_numeric_still_maps_to_number(self):
        desc = build_description_tuple({"name": "age", "type": "Numeric", "scale": 0})
        assert desc[1] == NUMBER

    def test_unknown_type_defaults_to_string(self):
        desc = build_description_tuple({"name": "mystery", "type": "geography"})
        assert desc[1] == STRING

    def test_nullable_and_precision_passthrough(self):
        desc = build_description_tuple(
            {"name": "amount", "type": "numeric", "nullable": False, "precision": 18, "scale": 2}
        )
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        assert desc[4] == 18
        assert desc[5] == 2
        assert desc[6] is False
