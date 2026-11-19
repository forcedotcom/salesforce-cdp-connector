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
