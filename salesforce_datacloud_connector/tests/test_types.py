"""Tests for the Data Cloud type system (types.py).

Focus: the off-core Query v3 API returns LOWERCASE type names, while the
Postgres metadata catalog emits CAPITALIZED names. Both must convert and map
identically (Finding C).
"""

from datetime import date, datetime, time
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

    def test_numeric_decimal_input_is_preserved_exactly(self):
        """Arrow's decimal128/decimal256 columns decode to an exact Decimal
        before this function ever sees them (see from_arrow_bytes); it must
        not re-coerce that value through float() and lose precision, even
        at a precision/scale that would otherwise select the float branch."""
        value = Decimal("3.14159265358979")
        result = convert_datacloud_value(value, "numeric", precision=15, scale=14)
        assert result == value
        assert isinstance(result, Decimal)

    def test_numeric_decimal_input_with_scale_zero_is_int(self):
        result = convert_datacloud_value(Decimal("42"), "numeric", scale=0)
        assert result == 42
        assert isinstance(result, int)

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

    def test_time_is_time(self):
        result = convert_datacloud_value("14:30:00", "time")
        assert result == time(14, 30, 0)
        assert isinstance(result, time)

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

    def test_bool_from_bool(self):
        assert convert_datacloud_value(True, "bool") is True
        assert convert_datacloud_value(False, "bool") is False

    def test_smallint_and_oid_are_int(self):
        assert convert_datacloud_value("7", "smallint") == 7
        assert isinstance(convert_datacloud_value("7", "smallint"), int)
        assert convert_datacloud_value(100000, "oid") == 100000
        assert isinstance(convert_datacloud_value(100000, "oid"), int)

    def test_float4_and_float8_are_float(self):
        assert convert_datacloud_value("1.5", "float4") == pytest.approx(1.5)
        assert isinstance(convert_datacloud_value("1.5", "float4"), float)
        assert convert_datacloud_value("2.25", "float8") == pytest.approx(2.25)
        assert isinstance(convert_datacloud_value("2.25", "float8"), float)

    def test_char_is_str(self):
        assert convert_datacloud_value("x", "char") == "x"
        assert isinstance(convert_datacloud_value("x", "char"), str)

    def test_bytea_passes_through_native_bytes_unchanged(self):
        """bytea is Arrow-only (the JSON sink rejects it with a 501), so
        nanoarrow has already decoded it to native bytes by the time it
        reaches here -- no base64 decoding is needed."""
        assert convert_datacloud_value(b"hello", "bytea") == b"hello"
        assert isinstance(convert_datacloud_value(b"hello", "bytea"), bytes)

    def test_json_arrow_raw_string_is_parsed(self):
        """The Arrow output format gives json columns as a raw JSON-encoded
        string (confirmed live); parse it so callers see the same Python
        value regardless of output format."""
        result = convert_datacloud_value('{"a":1}', "json")
        assert result == {"a": 1}

    def test_json_already_parsed_dict_passes_through_unchanged(self):
        """The JSON output format pre-parses json columns into native
        dicts/lists already; str()-ing that would corrupt it into repr()."""
        value = {"a": 1}
        assert convert_datacloud_value(value, "json") is value

    def test_varchar_json_like_string_passes_through_unparsed(self):
        """Arrow has no distinct physical type for json columns -- they
        arrive tagged "varchar" like any other string column (see
        models.py's _ARROW_TYPE_TO_DATACLOUD_TYPE comment), so the "json"
        branch above is unreachable from the Arrow path. This locks in that
        documented limitation: a json column's raw text comes through the
        "varchar" branch unparsed, not through json.loads()."""
        result = convert_datacloud_value('{"a":1}', "varchar")
        assert result == '{"a":1}'
        assert isinstance(result, str)

    def test_interval_passes_through_unchanged(self):
        """Neither wire representation (ISO-8601 string over JSON, a raw
        (months, days, nanoseconds) tuple over Arrow) maps losslessly onto a
        Python stdlib type, so interval is a deliberate pass-through."""
        assert convert_datacloud_value("P1D", "interval") == "P1D"
        assert convert_datacloud_value((0, 1, 7200000000000), "interval") == (0, 1, 7200000000000)


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

    def test_capitalized_time(self):
        assert convert_datacloud_value("14:30:00", "Time") == time(14, 30, 0)

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

    def test_lowercase_time_maps_to_datetime(self):
        desc = build_description_tuple({"name": "start", "type": "time"})
        assert desc[1] == DATETIME

    def test_capitalized_numeric_still_maps_to_number(self):
        desc = build_description_tuple({"name": "age", "type": "Numeric", "scale": 0})
        assert desc[1] == NUMBER

    def test_unknown_type_defaults_to_string(self):
        desc = build_description_tuple({"name": "mystery", "type": "geography"})
        assert desc[1] == STRING

    def test_bytea_maps_to_binary(self):
        from salesforce_datacloud_connector.types import BINARY

        desc = build_description_tuple({"name": "payload", "type": "bytea"})
        assert desc[1] == BINARY

    def test_smallint_and_oid_map_to_number(self):
        assert build_description_tuple({"name": "n", "type": "smallint"})[1] == NUMBER
        assert build_description_tuple({"name": "n", "type": "oid"})[1] == NUMBER

    def test_nullable_and_precision_passthrough(self):
        desc = build_description_tuple(
            {"name": "amount", "type": "numeric", "nullable": False, "precision": 18, "scale": 2}
        )
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        assert desc[4] == 18
        assert desc[5] == 2
        assert desc[6] is False
