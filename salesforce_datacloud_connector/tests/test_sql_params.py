"""No-network tests for the SQL-aware :name placeholder scanner."""

import pytest

from salesforce_datacloud_connector.api.client import DataCloudQueryClient
from salesforce_datacloud_connector.exceptions import ProgrammingError


def bind(sql, params):
    return DataCloudQueryClient._bind_parameters(object.__new__(DataCloudQueryClient), sql, params)


@pytest.mark.parametrize(
    "sql, params, expected_sql, expected_values",
    [
        ("SELECT :a", {"a": "x"}, "SELECT ?", ["x"]),
        ("SELECT :a, :b, :a", {"a": "1", "b": "2"}, "SELECT ?, ?, ?", ["1", "2", "1"]),
        ("SELECT ':name' AS s, :v", {"name": "n", "v": "x"}, "SELECT ':name' AS s, ?", ["x"]),
        ("SELECT 'it''s :name', :v", {"v": "x"}, "SELECT 'it''s :name', ?", ["x"]),
        ("SELECT E'it\\':name', :v", {"v": "x"}, "SELECT E'it\\':name', ?", ["x"]),
        ('SELECT 1 AS ":name", :v', {"v": "x"}, 'SELECT 1 AS ":name", ?', ["x"]),
        ("SELECT 1 /* :name */, :v", {"v": "x"}, "SELECT 1 /* :name */, ?", ["x"]),
        ("SELECT 1 /* a /* b */ :v", {"v": "x"}, "SELECT 1 /* a /* b */ ?", ["x"]),
        ("SELECT 1 -- :name\n, :v", {"v": "x"}, "SELECT 1 -- :name\n, ?", ["x"]),
        ("SELECT $$:name$$, :v", {"v": "x"}, "SELECT $$:name$$, ?", ["x"]),
        ("SELECT $t$ :name $t$, :v", {"v": "x"}, "SELECT $t$ :name $t$, ?", ["x"]),
        ("SELECT 7::int, :v", {"v": "x"}, "SELECT 7::int, ?", ["x"]),
        ("SELECT '12:30'::time, :v", {"v": "x"}, "SELECT '12:30'::time, ?", ["x"]),
        ("SELECT :my-name", {"my": "a", "my-name": "b"}, "SELECT ?", ["b"]),
        ("SELECT :a-1", {"a": 5}, "SELECT ?-1", [5]),
        ("SELECT :a - :b", {"a": 1, "b": 2}, "SELECT ? - ?", [1, 2]),
    ],
)
def test_scanner(sql, params, expected_sql, expected_values):
    new_sql, entries = bind(sql, params)
    assert new_sql == expected_sql
    assert [str(e["value"]) for e in entries] == [str(v) for v in expected_values]


def test_placeholder_after_unterminated_literal_is_not_scanned():
    new_sql, entries = bind("SELECT 'oops :name", {"name": "x"})
    assert new_sql == "SELECT 'oops :name"
    assert entries == []


def test_dollar_sign_inside_identifier_is_not_a_quote():
    new_sql, entries = bind("SELECT a$b, :v", {"v": "x"})
    assert new_sql == "SELECT a$b, ?"
    assert len(entries) == 1


def test_missing_name_is_reported():
    with pytest.raises(ProgrammingError, match=":missing"):
        bind("SELECT :missing", {"other": 1})


def test_missing_hyphenated_name_hints_at_spacing():
    with pytest.raises(ProgrammingError, match="subtraction"):
        bind("SELECT :a-1", {"b": 1})


def test_qmark_sql_takes_values_in_dict_order():
    new_sql, entries = bind("SELECT ':name', ?", {"x": "1"})
    assert new_sql == "SELECT ':name', ?"
    assert [str(e["value"]) for e in entries] == ["1"]


def test_values_are_ignored_when_the_sql_has_no_placeholder_in_code():
    new_sql, entries = bind("SELECT ':name' /* :name */ -- ?\n", {"name": "x"})
    assert new_sql == "SELECT ':name' /* :name */ -- ?\n"
    assert entries == []


def test_bare_dollar_is_not_a_quote():
    new_sql, entries = bind("SELECT '$' || $1, :v", {"v": "x"})
    assert new_sql == "SELECT '$' || $1, ?"
    assert len(entries) == 1


@pytest.mark.parametrize("sql", ["SELECT :a, ?", "SELECT ?, :a", "SELECT ? /* :b */, :a"])
def test_mixing_named_and_positional_placeholders_is_rejected(sql):
    with pytest.raises(ProgrammingError, match="Cannot mix"):
        bind(sql, {"a": 1})


def test_question_mark_in_a_string_or_comment_is_not_a_positional_placeholder():
    new_sql, entries = bind("SELECT '?' AS q, :a /* ? */ -- ?\n", {"a": 1})
    assert new_sql == "SELECT '?' AS q, ? /* ? */ -- ?\n"
    assert len(entries) == 1


@pytest.mark.parametrize("params", [{}, None])
def test_named_placeholder_without_any_values_is_reported(params):
    with pytest.raises(ProgrammingError, match=":v"):
        bind("SELECT :v", params)


@pytest.mark.parametrize("params", [{}, None])
@pytest.mark.parametrize("sql", ["SELECT 1", "SELECT ':v', 7::int, '12:30'::time /* :v */", "SELECT a[1:2] FROM t"])
def test_no_values_and_no_placeholder_in_code_leaves_the_sql_alone(sql, params):
    assert bind(sql, params) == (sql, [])


@pytest.mark.parametrize(
    "sql, params, expected_sql, bound",
    [
        ("SELECT 'a\\' AS s, :v", {"v": 1}, "SELECT 'a\\' AS s, ?", 1),  # standard string: backslash is not an escape
        ("SELECT e'a\\':v', :v", {"v": 1}, "SELECT e'a\\':v', ?", 1),
        ("SELECT U&'\\0041:v', :v", {"v": 1}, "SELECT U&'\\0041:v', ?", 1),
        ("SELECT $é$ :v $é$, :v", {"v": 1}, "SELECT $é$ :v $é$, ?", 1),
        ("SELECT 5--:v\n, :v", {"v": 1}, "SELECT 5--:v\n, ?", 1),
        ("SELECT 1 - -:v", {"v": 1}, "SELECT 1 - -?", 1),
        ("SELECT :naïve, :1", {"naïve": 1, "1": 2}, "SELECT ?, ?", 2),
        ("SELECT :a-b", {"a": 5, "a-b": 9}, "SELECT ?", 1),
    ],
)
def test_lexing_cases_checked_against_hyper(sql, params, expected_sql, bound):
    new_sql, entries = bind(sql, params)
    assert new_sql == expected_sql
    assert len(entries) == bound
