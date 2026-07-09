"""Tests síncronos (sin hilos) de las reglas de compatibilidad shared/
exclusive, upgrades, liberación y las excepciones de uso incorrecto de la
API. Los escenarios que necesitan demostrar bloqueo real (colas de espera,
deadlocks, timeouts) viven en `test_timeout.py` y `test_deadlock.py`, que sí
usan hilos reales.
"""

from __future__ import annotations

import threading
import time

import pytest

from lock_manager_deadlock_detector.models import (
    LockManagerError,
    LockMode,
    LockNotHeldError,
    LockTimeoutError,
    TransactionId,
)
from lock_manager_deadlock_detector.pipeline import LockManager

T1 = TransactionId(1)
T2 = TransactionId(2)
T3 = TransactionId(3)


def test_shared_locks_are_mutually_compatible() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    mgr.acquire(T2, "row", LockMode.SHARED)
    mgr.acquire(T3, "row", LockMode.SHARED)
    assert mgr.holds(T1, "row") is LockMode.SHARED
    assert mgr.holds(T2, "row") is LockMode.SHARED
    assert mgr.holds(T3, "row") is LockMode.SHARED


def test_exclusive_lock_is_incompatible_with_shared() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=0.05)


def test_exclusive_lock_is_incompatible_with_exclusive() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=0.05)


def test_shared_lock_is_incompatible_with_exclusive_holder() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T2, "row", LockMode.SHARED, timeout=0.05)


def test_release_unblocks_a_conflicting_waiter_synchronously_retried() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    mgr.release(T1, "row")
    mgr.acquire(T2, "row", LockMode.EXCLUSIVE)  # ya no debería bloquear
    assert mgr.holds(T2, "row") is LockMode.EXCLUSIVE


def test_reacquiring_same_mode_is_a_noop() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    mgr.acquire(T1, "row", LockMode.SHARED)  # no debe fallar ni bloquear
    assert mgr.holds(T1, "row") is LockMode.SHARED


def test_reacquiring_weaker_mode_while_holding_exclusive_is_a_noop() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    mgr.acquire(T1, "row", LockMode.SHARED)  # ya tiene algo más fuerte: no-op
    assert mgr.holds(T1, "row") is LockMode.EXCLUSIVE


def test_upgrade_from_shared_to_exclusive_succeeds_when_sole_holder() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)
    assert mgr.holds(T1, "row") is LockMode.EXCLUSIVE


def test_upgrade_blocks_while_another_transaction_holds_shared() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    mgr.acquire(T2, "row", LockMode.SHARED)
    with pytest.raises(LockTimeoutError):
        mgr.acquire(T1, "row", LockMode.EXCLUSIVE, timeout=0.05)
    # el upgrade fallido no debe haber tocado el lock shared original
    assert mgr.holds(T1, "row") is LockMode.SHARED


def test_release_all_frees_every_resource_held_by_a_transaction() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "a", LockMode.EXCLUSIVE)
    mgr.acquire(T1, "b", LockMode.SHARED)
    mgr.acquire(T1, "c", LockMode.SHARED)
    released = mgr.release_all(T1)
    assert released == 3
    assert mgr.holds(T1, "a") is None
    assert mgr.holds(T1, "b") is None
    assert mgr.holds(T1, "c") is None
    # y ahora otra transacción puede tomarlos todos
    mgr.acquire(T2, "a", LockMode.EXCLUSIVE)
    mgr.acquire(T2, "b", LockMode.EXCLUSIVE)
    mgr.acquire(T2, "c", LockMode.EXCLUSIVE)


def test_release_all_on_transaction_with_no_locks_is_a_noop() -> None:
    mgr: LockManager[str] = LockManager()
    assert mgr.release_all(T1) == 0


def test_release_without_holding_raises() -> None:
    mgr: LockManager[str] = LockManager()
    with pytest.raises(LockNotHeldError):
        mgr.release(T1, "row")


def test_release_wrong_transaction_raises() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)
    with pytest.raises(LockNotHeldError):
        mgr.release(T2, "row")


def test_holds_returns_none_for_unheld_resource() -> None:
    mgr: LockManager[str] = LockManager()
    assert mgr.holds(T1, "row") is None


def test_independent_resources_do_not_conflict() -> None:
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row-a", LockMode.EXCLUSIVE)
    mgr.acquire(T2, "row-b", LockMode.EXCLUSIVE)  # recurso distinto: no debe bloquear
    assert mgr.holds(T1, "row-a") is LockMode.EXCLUSIVE
    assert mgr.holds(T2, "row-b") is LockMode.EXCLUSIVE


def test_no_barging_blocks_a_later_compatible_request_behind_an_incompatible_one() -> None:
    """T1 tiene shared. T2 pide exclusive y se queda encolado, bloqueado
    por T1. Mientras T2 sigue pendiente, T3 pide shared -- aunque sería
    compatible con lo que T1 ya tiene concedido, la política "no barging"
    no le deja adelantar a T2 en la cola, para no dejarlo en inanición
    indefinida si siguiesen llegando lectores compatibles."""
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.SHARED)

    t2_registered = threading.Event()

    def t2_worker() -> None:
        t2_registered.set()
        mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=2.0)
        mgr.release(T2, "row")

    thread = threading.Thread(target=t2_worker)
    thread.start()
    t2_registered.wait(timeout=2.0)
    time.sleep(0.05)  # deja que T2 termine de encolar su petición

    with pytest.raises(LockTimeoutError):
        mgr.acquire(T3, "row", LockMode.SHARED, timeout=0.05)

    mgr.release(T1, "row")  # desbloquea a T2, que ahora sí obtiene el exclusive
    thread.join(timeout=2.0)
    assert not thread.is_alive()


def test_concurrent_pending_request_for_same_transaction_is_rejected() -> None:
    """Una transacción sólo puede tener una petición de lock pendiente a
    la vez -- ver la restricción de uso documentada en `pipeline.py`. Se
    fuerza el escenario con dos hilos reales conduciendo el mismo
    `TransactionId`: uno se queda bloqueado esperando "row" (ocupado por
    T1), y mientras sigue pendiente, el otro intenta pedir un recurso
    distinto para el mismo `txn_id`."""
    mgr: LockManager[str] = LockManager()
    mgr.acquire(T1, "row", LockMode.EXCLUSIVE)

    registered_as_pending = threading.Event()
    blocked_thread_outcome: list[BaseException | None] = [None]

    def blocked_acquire() -> None:
        registered_as_pending.set()
        try:
            mgr.acquire(T2, "row", LockMode.EXCLUSIVE, timeout=2.0)
        except BaseException as exc:  # noqa: BLE001 - se reporta desde el hilo principal
            blocked_thread_outcome[0] = exc

    thread = threading.Thread(target=blocked_acquire)
    thread.start()
    registered_as_pending.wait(timeout=2.0)
    # Pequeño margen para que el hilo bloqueado termine de encolar su
    # petición antes de que el hilo principal intente la segunda.
    time.sleep(0.05)

    with pytest.raises(LockManagerError):
        mgr.acquire(T2, "other-row", LockMode.SHARED, timeout=0.5)

    mgr.release(T1, "row")  # desbloquea al hilo en espera para poder unirlo
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert blocked_thread_outcome[0] is None  # el acquire original sí se concedió
