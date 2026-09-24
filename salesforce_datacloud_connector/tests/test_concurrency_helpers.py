"""
Tests for the deterministic concurrency test utilities themselves.

These prove the helper can *discriminate* a thundering-herd race (an unlocked
check-then-fetch admits every caller) from a correctly double-checked-locked
cache (exactly one caller reaches the fetch). If the gate did not actually hold
callers until release, one of these two assertions would fail — so together
they are the helper's own safety net, which the Stage 2/3 production tests then
rely on.
"""

import threading
import time

import pytest

from tests._concurrency_helpers import ConcurrencyGate, run_concurrently


def test_run_concurrently_collects_results_and_errors_by_index():
    """run_concurrently returns each worker's result at its index and surfaces
    raised exceptions rather than swallowing them."""
    def worker(i):
        if i == 3:
            raise ValueError("boom")
        return i * 10

    results, errors = run_concurrently(worker, parties=5)

    assert results[0] == 0
    assert results[1] == 10
    assert results[2] == 20
    assert results[4] == 40
    assert [(i, type(e)) for i, e in errors] == [(3, ValueError)]


def test_run_concurrently_surfaces_worker_exception_promptly():
    """A worker exception must still be surfaced to the caller (not swallowed,
    and not mistaken for a deadlock) even with the shared-deadline join."""
    def worker(i):
        if i == 0:
            raise ValueError("boom")
        return i

    start = time.monotonic()
    results, errors = run_concurrently(worker, parties=4, join_timeout=5.0)
    elapsed = time.monotonic() - start

    assert [(i, type(e)) for i, e in errors] == [(0, ValueError)]
    assert results[1] == 1
    # Well-behaved workers finish essentially immediately; this should not
    # have consumed anything close to the join deadline.
    assert elapsed < 5.0


def test_run_concurrently_fails_fast_on_deadlocked_worker():
    """A worker that never returns (e.g. blocked forever on a lock it will
    never get) must make run_concurrently raise within the configured
    deadline instead of hanging pytest forever.

    Pre-fix, this worker would hang the join loop indefinitely (each
    ``thread.join(timeout=...)`` call times out but the code never checks
    ``is_alive()``, so it returns normally with the deadlocked thread still
    running as a non-daemon thread -- which would itself keep the interpreter
    alive after the test process tries to exit). Post-fix, the shared
    deadline elapses, the stuck thread is detected via ``is_alive()``, and a
    clear RuntimeError is raised promptly.
    """
    never_release = threading.Event()

    def deadlocked_worker(i):
        if i == 0:
            # Blocks forever -- simulates a real deadlock (e.g. a lock that is
            # never released).
            never_release.wait()
            return "unreachable"
        return i

    start = time.monotonic()
    with pytest.raises(RuntimeError, match="deadlock"):
        run_concurrently(deadlocked_worker, parties=3, join_timeout=0.3)
    elapsed = time.monotonic() - start

    # Must fail fast: bounded by the deadline plus a small scheduling margin,
    # never anywhere close to "forever".
    assert elapsed < 5.0


def test_gate_detects_thundering_herd_without_lock():
    """An unlocked check-then-fetch cache lets all N callers reach the fetch.
    The gate must observe every entry — this is the race a lock would prevent."""
    gate = ConcurrencyGate()
    parties = 8
    state = {"value": None}

    def unlocked_get(_i):
        if state["value"] is None:        # racy check
            gate.enter()                  # held open until release()
            state["value"] = "computed"   # racy write
        return state["value"]

    results, errors = gate.run(unlocked_get, parties)

    assert errors == []
    assert all(r == "computed" for r in results)
    assert gate.entered == parties          # every caller fetched
    assert gate.max_concurrent == parties   # genuinely concurrent


def test_gate_confirms_single_fetch_with_double_checked_lock():
    """A correctly double-checked-locked cache admits exactly one fetch even
    though N callers race in. The gate must observe exactly one entry."""
    gate = ConcurrencyGate()
    parties = 8
    lock = threading.Lock()
    state = {"value": None}

    def locked_get(_i):
        if state["value"] is None:              # fast path (unlocked)
            with lock:
                if state["value"] is None:      # re-check under lock
                    gate.enter()                # only the winner reaches here
                    state["value"] = "computed"
        return state["value"]

    results, errors = gate.run(locked_get, parties, pileup_timeout=0.5)

    assert errors == []
    assert all(r == "computed" for r in results)
    assert gate.entered == 1
    assert gate.max_concurrent == 1
