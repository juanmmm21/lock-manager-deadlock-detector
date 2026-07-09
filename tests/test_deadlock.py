"""Deadlocks inducidos deliberadamente con hilos reales -- no interleaving
simulado en un único hilo -- verificando que el detector de ciclos del
wait-for graph los resuelve siempre, sin dejar ninguna transacción
esperando para siempre. Cubre el requisito explícito del `AGENTS.md` de
este subproyecto: escenarios de 2 y de 3+ transacciones en ciclo.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Hashable
from typing import TypeVar

import pytest

from lock_manager_deadlock_detector.models import (
    DeadlockAbortError,
    LockMode,
    TransactionId,
)
from lock_manager_deadlock_detector.pipeline import LockManager

_ResourceKeyT = TypeVar("_ResourceKeyT", bound=Hashable)


def _run_ring_deadlock(
    mgr: LockManager[_ResourceKeyT], resources: list[_ResourceKeyT], txns: list[TransactionId]
) -> dict[TransactionId, str]:
    """El hilo `i` retiene `resources[i]` en exclusiva y luego pide
    `resources[(i + 1) % n]`, también en exclusiva. Con una barrera que
    obliga a las `n` transacciones a tener ya su propio recurso antes de
    que ninguna pida el siguiente, esto cierra un ciclo de tamaño `n` en el
    wait-for graph con certeza (no depende de una carrera de temporización).
    Devuelve, por transacción, `"completó"` o `"abortada"`.
    """
    n = len(resources)
    assert len(txns) == n
    barrier = threading.Barrier(n)
    results: dict[TransactionId, str] = {}
    results_lock = threading.Lock()

    def worker(index: int) -> None:
        txn = txns[index]
        own = resources[index]
        nxt = resources[(index + 1) % n]
        mgr.acquire(txn, own, LockMode.EXCLUSIVE)
        barrier.wait()
        try:
            mgr.acquire(txn, nxt, LockMode.EXCLUSIVE)
            outcome = "completó"
        except DeadlockAbortError:
            outcome = "abortada"
        mgr.release_all(txn)
        with results_lock:
            results[txn] = outcome

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads), "una transacción se quedó esperando para siempre"
    return results


def test_two_transaction_deadlock_is_resolved_without_permanent_hang() -> None:
    """El caso clásico: T1 retiene A y pide B; T2 retiene B y pide A."""
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    txns = [TransactionId(1), TransactionId(2)]
    results = _run_ring_deadlock(mgr, ["account-A", "account-B"], txns)

    assert set(results) == set(txns)
    aborted = [txn for txn, outcome in results.items() if outcome == "abortada"]
    completed = [txn for txn, outcome in results.items() if outcome == "completó"]
    assert len(aborted) == 1
    assert len(completed) == 1
    assert mgr.snapshot_wait_for_graph() == {}


def test_two_transaction_deadlock_victim_is_the_youngest() -> None:
    """La política por defecto (`YoungestTransactionVictimPolicy`) aborta
    siempre al `TransactionId` más alto del ciclo -- comprobado en ambos
    sentidos para descartar que el resultado dependa del orden de hilos."""
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    older, younger = TransactionId(10), TransactionId(20)
    results = _run_ring_deadlock(mgr, ["x", "y"], [older, younger])
    assert results[younger] == "abortada"
    assert results[older] == "completó"


def test_two_transaction_deadlock_via_shared_to_exclusive_upgrade() -> None:
    """Deadlock clásico de upgrade: dos transacciones tienen `SHARED`
    sobre el mismo recurso y ambas piden `EXCLUSIVE` a la vez -- ninguna
    puede progresar sin que la otra libere primero su `SHARED`."""
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    t1, t2 = TransactionId(1), TransactionId(2)
    mgr.acquire(t1, "row", LockMode.SHARED)
    mgr.acquire(t2, "row", LockMode.SHARED)

    barrier = threading.Barrier(2)
    results: dict[TransactionId, str] = {}
    results_lock = threading.Lock()

    def upgrade_worker(txn: TransactionId) -> None:
        barrier.wait()
        try:
            mgr.acquire(txn, "row", LockMode.EXCLUSIVE)
            outcome = "completó"
        except DeadlockAbortError:
            outcome = "abortada"
        mgr.release_all(txn)
        with results_lock:
            results[txn] = outcome

    threads = [
        threading.Thread(target=upgrade_worker, args=(t1,)),
        threading.Thread(target=upgrade_worker, args=(t2,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert not any(t.is_alive() for t in threads)

    aborted = [txn for txn, outcome in results.items() if outcome == "abortada"]
    completed = [txn for txn, outcome in results.items() if outcome == "completó"]
    assert len(aborted) == 1
    assert len(completed) == 1
    assert mgr.snapshot_wait_for_graph() == {}


def test_three_transaction_cycle_deadlock_is_resolved_without_permanent_hang() -> None:
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    txns = [TransactionId(1), TransactionId(2), TransactionId(3)]
    results = _run_ring_deadlock(mgr, ["page-1", "page-2", "page-3"], txns)

    assert set(results) == set(txns)
    aborted = [txn for txn, outcome in results.items() if outcome == "abortada"]
    completed = [txn for txn, outcome in results.items() if outcome == "completó"]
    assert len(aborted) == 1
    assert len(completed) == 2
    assert mgr.snapshot_wait_for_graph() == {}
    # la víctima es siempre la más joven del ciclo completo (todas participan en él)
    assert aborted == [max(txns)]


@pytest.mark.parametrize("cycle_size", [4, 5, 8])
def test_larger_cycles_are_resolved_without_permanent_hang(cycle_size: int) -> None:
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    txns = [TransactionId(i) for i in range(1, cycle_size + 1)]
    resources = [f"resource-{i}" for i in range(cycle_size)]
    results = _run_ring_deadlock(mgr, resources, txns)

    assert set(results) == set(txns)
    aborted = [txn for txn, outcome in results.items() if outcome == "abortada"]
    completed = [txn for txn, outcome in results.items() if outcome == "completó"]
    assert len(aborted) == 1
    assert len(completed) == cycle_size - 1
    assert mgr.snapshot_wait_for_graph() == {}


def test_non_cyclic_wait_chain_never_triggers_a_false_deadlock() -> None:
    """T3 espera a T2, que espera a T1 -- una cadena, no un ciclo. Ninguna
    transacción debe abortarse por deadlock: en cuanto T1 libera, la
    cadena se resuelve entera sin intervención del detector."""
    mgr: LockManager[str] = LockManager(default_timeout=10.0)
    t1, t2, t3 = TransactionId(1), TransactionId(2), TransactionId(3)
    mgr.acquire(t1, "row", LockMode.EXCLUSIVE)

    started = threading.Barrier(3)
    results: dict[TransactionId, str] = {}
    results_lock = threading.Lock()

    def waiter(txn: TransactionId) -> None:
        started.wait()
        try:
            mgr.acquire(txn, "row", LockMode.EXCLUSIVE)
            outcome = "completó"
        except DeadlockAbortError:
            outcome = "abortada"
        with results_lock:
            results[txn] = outcome

    t2_thread = threading.Thread(target=waiter, args=(t2,))
    t3_thread = threading.Thread(target=waiter, args=(t3,))
    t2_thread.start()
    t3_thread.start()
    started.wait()
    # deja que ambas peticiones se encolen antes de liberar t1
    time.sleep(0.1)
    mgr.release(t1, "row")
    t2_thread.join(timeout=5)
    # t2 ya debería tener el lock; libéralo para que t3 progrese también
    mgr.release(t2, "row")
    t3_thread.join(timeout=5)

    assert not t2_thread.is_alive()
    assert not t3_thread.is_alive()
    assert results == {t2: "completó", t3: "completó"}
    assert mgr.snapshot_wait_for_graph() == {}


def test_deadlock_detection_works_with_bytes_resource_keys() -> None:
    """`LockManager` es genérico sobre el tipo de recurso -- comprobado
    aquí con `bytes` en vez de `str`, tal como exige el `AGENTS.md`."""
    mgr: LockManager[bytes] = LockManager(default_timeout=10.0)
    txns = [TransactionId(1), TransactionId(2)]
    results = _run_ring_deadlock(mgr, [b"resource-a", b"resource-b"], txns)
    aborted = [txn for txn, outcome in results.items() if outcome == "abortada"]
    assert len(aborted) == 1
