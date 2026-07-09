"""Tests del mecanismo de timeout: independiente y complementario a la
detección de deadlocks por wait-for graph (ver `AGENTS.md` de este
subproyecto). Todos usan hilos reales -- el propio mecanismo que se prueba
sólo existe mientras un hilo está bloqueado dentro de `acquire()`.
"""

from __future__ import annotations

import threading
import time

import pytest

from lock_manager_deadlock_detector.models import (
    DeadlockAbortError,
    LockMode,
    LockTimeoutError,
    TransactionId,
)
from lock_manager_deadlock_detector.pipeline import LockManager

T1 = TransactionId(1)
T2 = TransactionId(2)
T3 = TransactionId(3)


def test_timeout_raises_after_configured_duration() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    started = time.monotonic()
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=0.1)
    elapsed = time.monotonic() - started
    assert 0.08 <= elapsed < 2.0  # se resolvió por el timeout, no instantáneo ni colgado


def test_manager_default_timeout_applies_when_not_overridden() -> None:
    mgr: LockManager[str] = LockManager(default_timeout=0.1)
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    started = time.monotonic()
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE)  # sin pasar timeout explícito
    elapsed = time.monotonic() - started
    assert elapsed >= 0.08


def test_per_call_timeout_overrides_manager_default() -> None:
    mgr: LockManager[str] = LockManager(default_timeout=5.0)
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    started = time.monotonic()
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=0.1)
    elapsed = time.monotonic() - started
    assert elapsed < 2.0  # no esperó los 5s del default del gestor


def test_explicit_none_timeout_overrides_manager_default_and_waits() -> None:
    mgr: LockManager[str] = LockManager(default_timeout=0.05)
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)

    def release_later() -> None:
        time.sleep(0.2)
        mgr.release(T1, "row")

    releaser = threading.Thread(target=release_later)
    releaser.start()
    # timeout=None explícito: debe esperar más allá del default_timeout corto del gestor
    mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=None)
    releaser.join(timeout=2.0)
    assert mgr.holds(T2, "row") is LockMode.EXCLUSIVE


def test_timed_out_request_is_fully_cleaned_up() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=0.05)
    assert mgr.snapshot_wait_for_graph() == {}
    mgr.release(T1, "row")
    # ninguna traza de la petición que expiró le impide a otra transacción tomarlo
    mgr.acquire(T3, "row", LockMode.EXCLUSIVE)
    assert mgr.holds(T3, "row") is LockMode.EXCLUSIVE


def test_deadlock_resolves_much_faster_than_a_generous_timeout() -> None:
    """Con un `default_timeout` generoso, un deadlock real de 2
    transacciones debe resolverse por detección de ciclos casi al
    instante -- nunca esperando a que expire el timeout. Esto demuestra
    que son dos mecanismos independientes, no una única ruta de código
    disfrazada de dos nombres."""
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    mgr.acquire(T1, "a", LockMode.EXCLUSIVE)
    mgr.acquire(T2, "b", LockMode.EXCLUSIVE)

    barrier = threading.Barrier(2)
    outcomes: dict[TransactionId, BaseException | None] = {}
    outcomes_lock = threading.Lock()

    def t1_worker() -> None:
        barrier.wait()
        outcome: BaseException | None = None
        try:
            mgr.acquire(T1, "b", LockMode.EXCLUSIVE)
        except DeadlockAbortError as exc:
            outcome = exc
        mgr.release_all(T1)
        with outcomes_lock:
            outcomes[T1] = outcome

    def t2_worker() -> None:
        barrier.wait()
        outcome: BaseException | None = None
        try:
            mgr.acquire(T2, "a", LockMode.EXCLUSIVE)
        except DeadlockAbortError as exc:
            outcome = exc
        mgr.release_all(T2)
        with outcomes_lock:
            outcomes[T2] = outcome

    started = time.monotonic()
    threads = [threading.Thread(target=t1_worker), threading.Thread(target=t2_worker)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)
    elapsed = time.monotonic() - started

    assert not any(t.is_alive() for t in threads)
    assert (
        elapsed < 2.0
    )  # muy por debajo de los 10s de default_timeout: fue el grafo, no el timeout
    aborted = [txn for txn, exc in outcomes.items() if exc is not None]
    assert len(aborted) == 1
