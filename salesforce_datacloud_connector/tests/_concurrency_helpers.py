"""
Deterministic concurrency test utilities.

The point of these helpers is to make "N threads race into a contended section"
reproducible on every run instead of relying on scheduling luck:

* ``run_concurrently`` starts one thread per party, holds them all at a
  ``threading.Barrier`` so they enter the worker body simultaneously, then
  collects each worker's result (by index) and any exception it raised.

* ``ConcurrencyGate`` is a counting gate a worker calls from inside the
  section a lock is supposed to protect (e.g. the token-fetch / exchange). It
  records how many callers reached the section (``entered``) and the peak
  number that were inside it at once (``max_concurrent``), and it *holds*
  callers there long enough to expose overlap:

    - if every party reaches the gate, they pile up and are released together
      (this is the thundering-herd signature of unlocked check-then-act code);
    - if fewer than all parties arrive, the gate releases the ones present
      after ``pileup_timeout`` (the signature of correctly locked code, where
      exactly one caller reaches the fetch and the rest see the published
      value and never enter).

  A properly double-checked-locked cache therefore yields ``entered == 1`` and
  ``max_concurrent == 1``; the pre-fix racy code yields both equal to the party
  count. That gap is what the Stage 2/3 production tests assert on.
"""

import threading
from typing import Callable, List, Tuple


def run_concurrently(
    worker: Callable[[int], object],
    parties: int,
    start_timeout: float = 10.0,
    join_timeout: float = 30.0,
) -> Tuple[List[object], List[Tuple[int, BaseException]]]:
    """Run ``worker(i)`` for ``i`` in ``range(parties)``, each in its own thread,
    all released simultaneously.

    Returns ``(results, errors)`` where ``results[i]`` is worker ``i``'s return
    value (``None`` if it raised) and ``errors`` is a list of ``(i, exception)``
    sorted by index for every worker that raised.
    """
    results: List[object] = [None] * parties
    errors: List[Tuple[int, BaseException]] = []
    errors_lock = threading.Lock()
    start_barrier = threading.Barrier(parties)

    def runner(i: int) -> None:
        # Release all workers into the body at the same instant so the race is
        # real, not an artifact of staggered thread start-up.
        start_barrier.wait(timeout=start_timeout)
        try:
            results[i] = worker(i)
        except BaseException as exc:  # noqa: BLE001 - surface every failure to the test
            with errors_lock:
                errors.append((i, exc))

    threads = [threading.Thread(target=runner, args=(i,)) for i in range(parties)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=join_timeout)

    errors.sort(key=lambda pair: pair[0])
    return results, errors


class ConcurrencyGate:
    """A counting gate used from inside the section a lock should serialize.

    See the module docstring for the discrimination it provides between racy
    and correctly-locked code.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self.entered = 0
        self.max_concurrent = 0
        self._current = 0
        self._parties = 0
        self._pileup_timeout = 0.5
        self._released = False

    def enter(self) -> None:
        """Mark that a caller reached the contended section, and block until the
        gate releases (either the whole herd assembled, or ``pileup_timeout``
        elapsed with only some present)."""
        with self._cond:
            self.entered += 1
            self._current += 1
            if self._current > self.max_concurrent:
                self.max_concurrent = self._current

            if self.entered >= self._parties:
                # Whole herd assembled — release everyone at once.
                self._released = True
                self._cond.notify_all()
            else:
                # Wait for the rest of the herd; if they never come (the locked
                # case, where only one caller reaches here), give up after the
                # pileup timeout and proceed.
                self._cond.wait_for(lambda: self._released, timeout=self._pileup_timeout)
                self._released = True
                self._cond.notify_all()

            self._current -= 1

    def run(
        self,
        worker: Callable[[int], object],
        parties: int,
        pileup_timeout: float = 0.5,
    ) -> Tuple[List[object], List[Tuple[int, BaseException]]]:
        """Reset the gate for ``parties`` workers and run them concurrently.

        Returns the same ``(results, errors)`` tuple as ``run_concurrently``.
        """
        with self._cond:
            self.entered = 0
            self.max_concurrent = 0
            self._current = 0
            self._parties = parties
            self._pileup_timeout = pileup_timeout
            self._released = False

        return run_concurrently(worker, parties)
