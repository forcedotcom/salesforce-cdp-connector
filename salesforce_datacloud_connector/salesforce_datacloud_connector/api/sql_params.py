"""
Find :name placeholders in SQL text.

A placeholder is only a placeholder in code: the scanner skips single-quoted
strings (including E'...' strings with backslash escapes), double-quoted
identifiers, -- line comments, /* */ block comments and $tag$...$tag$
dollar-quoted text, so ':name' inside any of those is left alone. A "::"
cast is never a placeholder either.
"""

import re
from typing import Callable, Iterable, List, Tuple

# A name is word characters, optionally joined by hyphens (":my-name").
_NAME_RE = re.compile(r"\w+(?:-\w+)*")
_DOLLAR_TAG_RE = re.compile(r"\$(?:[^\W\d]\w*)?\$")


def _skip_quoted(sql: str, i: int, quote: str, backslash: bool) -> int:
    """Return the index just past the quoted run that starts at sql[i] (an opening quote)."""
    n = len(sql)
    i += 1
    while i < n:
        c = sql[i]
        if backslash and c == "\\":
            i += 2
            continue
        if c == quote:
            if i + 1 < n and sql[i + 1] == quote:  # doubled quote is an escaped quote
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _skip_block_comment(sql: str, i: int) -> int:
    """Return the index just past the /* */ comment starting at sql[i]; Hyper does not nest them."""
    end = sql.find("*/", i + 2)
    return len(sql) if end == -1 else end + 2


def rewrite_named_parameters(sql: str, resolve: Callable[[str, List[str]], str]) -> Tuple[str, List[str], bool]:
    """
    Replace every :name placeholder in SQL code with "?".

    Args:
        sql: Query text
        resolve: Called with the scanned name and the other candidate names
            (shorter hyphen prefixes, longest first); returns the name to use.

    Returns:
        (sql_with_qmarks, names_in_order_of_appearance, has_qmark) where
        has_qmark says whether the SQL code already contains a "?" placeholder.
    """
    out: List[str] = []
    names: List[str] = []
    has_qmark = False
    n = len(sql)
    i = 0
    while i < n:
        c = sql[i]
        if c == "'":
            j = _skip_quoted(sql, i, "'", backslash=i > 0 and sql[i - 1] in "eE" and not _is_word(sql, i - 2))
        elif c == '"':
            j = _skip_quoted(sql, i, '"', backslash=False)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
        elif sql.startswith("/*", i):
            j = _skip_block_comment(sql, i)
        elif c == "$" and not _is_word(sql, i - 1):
            tag = _DOLLAR_TAG_RE.match(sql, i)
            if tag:
                end = sql.find(tag.group(0), tag.end())
                j = n if end == -1 else end + len(tag.group(0))
            else:
                j = i + 1
        elif c == ":" and not (i > 0 and (sql[i - 1] == ":" or _is_word(sql, i - 1))):
            match = _NAME_RE.match(sql, i + 1)
            if match and not sql.startswith(":", i + 1):
                name = match.group(0)
                names.append(resolve(name, _hyphen_prefixes(name)))
                out.append("?")
                # resolve() may have chosen a shorter name; only consume what it used.
                j = i + 1 + len(names[-1])
                i = j
                continue
            j = i + 1
        else:
            has_qmark = has_qmark or c == "?"
            j = i + 1
        out.append(sql[i:j])
        i = j
    return "".join(out), names, has_qmark


def _is_word(sql: str, i: int) -> bool:
    return i >= 0 and (sql[i].isalnum() or sql[i] == "_")


def _hyphen_prefixes(name: str) -> List[str]:
    """Shorter names obtained by cutting at a hyphen, longest first (":a-b-c" -> "a-b", "a")."""
    parts = name.split("-")
    return ["-".join(parts[:k]) for k in range(len(parts) - 1, 0, -1)]


def pick_name(name: str, candidates: Iterable[str], known: Iterable[str]) -> str:
    """Prefer the full (hyphenated) name; fall back to a shorter prefix only if that one is the supplied key."""
    known = set(known)
    if name in known:
        return name
    for candidate in candidates:
        if candidate in known:
            return candidate
    return name
