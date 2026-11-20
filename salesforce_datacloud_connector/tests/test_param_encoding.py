"""No-network tests for Query v3 parameter encoding."""

import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.api.param_encoding import encode_parameter
from salesforce_datacloud_connector.exceptions import ProgrammingError


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, {"type": "varchar", "value": None}),
        (True, {"type": "bool", "value": "true"}),
        (False, {"type": "bool", "value": "false"}),
        (5, {"type": "bigint", "value": "5"}),
        (3_000_000_000, {"type": "bigint", "value": "3000000000"}),
        (2**63 - 1, {"type": "bigint", "value": "9223372036854775807"}),
        (-(2**63), {"type": "bigint", "value": "-9223372036854775808"}),
        (2**63, {"type": "numeric", "precision": 19, "scale": 0, "value": "9223372036854775808"}),
        (1.5, {"type": "float8", "value": "1.5"}),
        (float("nan"), {"type": "float8", "value": "NaN"}),
        (float("inf"), {"type": "float8", "value": "Infinity"}),
        (float("-inf"), {"type": "float8", "value": "-Infinity"}),
        (Decimal("12.34"), {"type": "numeric", "precision": 4, "scale": 2, "value": "12.34"}),
        (Decimal("0"), {"type": "numeric", "precision": 1, "scale": 0, "value": "0"}),
        (Decimal("-1.50"), {"type": "numeric", "precision": 3, "scale": 2, "value": "-1.50"}),
        (Decimal("0.001"), {"type": "numeric", "precision": 3, "scale": 3, "value": "0.001"}),
        (Decimal("1E+3"), {"type": "numeric", "precision": 4, "scale": 0, "value": "1000"}),
        (date(2026, 1, 2), {"type": "date", "value": "2026-01-02"}),
        (time(3, 4, 5, 123456), {"type": "time", "value": "03:04:05.123456"}),
        (datetime(2026, 1, 2, 3, 4, 5), {"type": "timestamp", "value": "2026-01-02T03:04:05"}),
        (
            datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=5, minutes=30))),
            {"type": "timestamptz", "value": "2026-01-02T03:04:05+05:30"},
        ),
        ("it's", {"type": "varchar", "value": "it's"}),
        ([1, 2], {"type": "array", "inner": {"type": "bigint"}, "value": "[1, 2]"}),
        ([1, None], {"type": "array", "inner": {"type": "bigint"}, "value": "[1, null]"}),
        (["a"], {"type": "array", "inner": {"type": "varchar"}, "value": '["a"]'}),
        ([], {"type": "array", "inner": {"type": "varchar"}, "value": "[]"}),
    ],
)
def test_encode_parameter(value, expected):
    assert encode_parameter(value) == expected


def test_38_digit_decimal_ok_39_rejected():
    assert encode_parameter(Decimal("9" * 38))["precision"] == 38
    with pytest.raises(ProgrammingError):
        encode_parameter(Decimal("9" * 39))


@pytest.mark.parametrize(
    "value",
    [Decimal("NaN"), Decimal("Infinity"), b"x", {"a": 1}, {1}, [1, "a"], [[1]], time(1, tzinfo=timezone.utc), 10**40],
)
def test_unsupported_values_raise_programming_error(value):
    with pytest.raises(ProgrammingError):
        encode_parameter(value)


def test_bind_parameters_is_json_serialisable_and_positional():
    sql, params = DataCloudQueryClient._bind_parameters(
        object.__new__(DataCloudQueryClient), "SELECT :d, :d, :n", {"d": date(2026, 1, 2), "n": Decimal("1.5")}
    )
    assert sql == "SELECT ?, ?, ?"
    assert [p["type"] for p in params] == ["date", "date", "numeric"]
    json.dumps(params)


def test_array_of_strings_and_dates_uses_string_elements():
    entry = encode_parameter(["a", "b"])
    assert json.loads(entry["value"]) == ["a", "b"]
    entry = encode_parameter([date(2024, 1, 2)])
    assert json.loads(entry["value"]) == ["2024-01-02"]


class _LoudInt(int):
    """Like IntEnum before Python 3.11: str() is not the number."""

    __str__ = __repr__ = lambda self: "LoudInt.A"


class _LoudFloat(float):
    __str__ = __repr__ = lambda self: "LoudFloat.A"


def test_int_and_float_subclasses_are_sent_as_numbers():
    assert encode_parameter(_LoudInt(5)) == {"type": "bigint", "value": "5"}
    assert encode_parameter(_LoudFloat(1.5)) == {"type": "float8", "value": "1.5"}
    assert json.loads(encode_parameter([_LoudInt(5), _LoudInt(6)])["value"]) == [5, 6]
    assert json.loads(encode_parameter([_LoudFloat(1.5)])["value"]) == [1.5]


@pytest.mark.parametrize(
    "value, inner, elements",
    [
        ((1, 2), {"type": "bigint"}, [1, 2]),
        ([True, None, False], {"type": "bool"}, [True, None, False]),
        ([1.5, float("nan"), float("-inf")], {"type": "float8"}, [1.5, "NaN", "-Infinity"]),
        ([time(1, 2, 3)], {"type": "time"}, ["01:02:03"]),
        ([datetime(2026, 1, 2, 3)], {"type": "timestamp"}, ["2026-01-02T03:00:00"]),
        ([datetime(2026, 1, 2, 3, tzinfo=timezone.utc)], {"type": "timestamptz"}, ["2026-01-02T03:00:00+00:00"]),
        ([None, None], {"type": "varchar"}, [None, None]),
    ],
)
def test_array_inner_type_and_elements(value, inner, elements):
    entry = encode_parameter(value)
    assert entry["inner"] == inner
    assert json.loads(entry["value"]) == elements


def test_decimal_array_uses_one_type_wide_enough_for_every_element():
    entry = encode_parameter([Decimal("1.5"), Decimal("12.25"), None, Decimal("123")])
    assert entry["inner"] == {"type": "numeric", "precision": 5, "scale": 2}
    assert json.loads(entry["value"]) == ["1.5", "12.25", None, "123"]


def test_decimal_array_that_needs_more_than_38_digits_is_rejected():
    with pytest.raises(ProgrammingError, match="38 digits"):
        encode_parameter([Decimal("9" * 38), Decimal("0.5")])


@pytest.mark.parametrize("value", [[1, 2.5], [True, 1], [date(2026, 1, 2), datetime(2026, 1, 2)], [1, 2**63]])
def test_arrays_of_different_kinds_are_rejected(value):
    with pytest.raises(ProgrammingError, match="mixed element types"):
        encode_parameter(value)
