"""Tests de propiedades con semilla fija de aleatoriedad (nunca `random`
sin semilla, ver `CLAUDE.md`), verificando invariantes estructurales bajo
cargas de cientos o miles de operaciones:

- `test_sequential_operations_match_a_reference_compatibility_model`:
  miles de operaciones de un único hilo, comparadas contra un modelo de
  referencia independiente (una comprobación de compatibilidad shared/
  exclusive sobre un `dict` plano) -- si `LockManager` concede o deniega
  algo que el modelo no esperaba, el test falla.
- `test_concurrent_workload_never_violates_mutual_exclusion`: hilos reales
  con generadores `random.Random` sembrados de forma determinista,
  golpeando un pool pequeño de recursos con locks shared/exclusive
  (incluyendo deadlocks reales, resueltos por el detector), mientras un
  hilo auditor muestrea el estado y comprueba en cada muestra que ninguna
  transacción tiene un `EXCLUSIVE` a la vez que cualquier otra transacción
  tiene cualquier lock sobre el mismo recurso -- el invariante de
  seguridad que todo el módulo existe para proteger.
"""

from __future__ import annotations

import random
import threading

from lock_manager_deadlock_detector.models import (
    DeadlockAbortError,
    LockManagerError,
    LockMode,
    LockTimeoutError,
    TransactionId,
    modes_compatible,
)
from lock_manager_deadlock_detector.pipeline import LockManager

_SEED = 20260709
_RESOURCES = [f"resource-{i}" for i in range(6)]
_TRANSACTIONS = [TransactionId(i) for i in range(1, 9)]


def test_sequential_operations_match_a_reference_compatibility_model() -> None:
    """Con `timeout=0`, `acquire()` nunca llega a encolarse de verdad: se
    concede al instante o falla al instante (ver docstring de `acquire` en
    `pipeline.py`). Eso hace que, en un único hilo, la decisión de
    conceder o no sea *siempre* una comprobación de compatibilidad pura
    contra los holders actuales -- exactamente lo que replica el modelo de
    referencia de este test, sin ninguna de las reglas de cola FIFO (que
    sólo entran en juego cuando hay contención real entre hilos, cubierta
    por el otro test de este módulo y por `test_deadlock.py`).
    """
    rng = random.Random(_SEED)
    mgr: LockManager[str] = LockManager()
    reference: dict[str, dict[TransactionId, LockMode]] = {}

    for _ in range(5000):
        resource = rng.choice(_RESOURCES)
        txn = rng.choice(_TRANSACTIONS)
        held_by_others = reference.get(resource, {})

        if rng.random() < 0.35 and txn in held_by_others:
            mgr.release(txn, resource)
            del held_by_others[txn]
            if not held_by_others:
                reference.pop(resource, None)
            assert mgr.holds(txn, resource) is None
            continue

        mode = LockMode.SHARED if rng.random() < 0.5 else LockMode.EXCLUSIVE
        already_held = held_by_others.get(txn)
        conflict = any(
            other_txn != txn and not modes_compatible(other_mode, mode)
            for other_txn, other_mode in held_by_others.items()
        )
        should_grant = already_held is LockMode.EXCLUSIVE or already_held is mode or not conflict

        try:
            mgr.acquire(txn, resource, mode, timeout=0)
        except LockTimeoutError:
            assert not should_grant, (
                f"el modelo esperaba conceder {mode} a t{txn} sobre {resource!r}, "
                f"pero LockManager lo denegó (holders={held_by_others})"
            )
            continue

        assert should_grant, (
            f"el modelo esperaba denegar {mode} a t{txn} sobre {resource!r}, "
            f"pero LockManager lo concedió (holders={held_by_others})"
        )
        # un no-op (pedir algo igual o más débil de lo ya tenido) no cambia
        # el modo resultante: EXCLUSIVE ya concedido nunca se "degrada" a SHARED.
        resulting_mode = already_held if already_held is LockMode.EXCLUSIVE else mode
        reference.setdefault(resource, {})[txn] = resulting_mode
        assert mgr.holds(txn, resource) is resulting_mode

    # el estado final del gestor coincide exactamente con el modelo de referencia
    normalized_reference = {res: holders for res, holders in reference.items() if holders}
    assert mgr.snapshot_holders() == normalized_reference
    assert mgr.snapshot_wait_for_graph() == {}  # timeout=0 nunca deja nada encolado


def _assert_mutual_exclusion(holders: dict[str, dict[TransactionId, LockMode]]) -> None:
    for resource, txn_modes in holders.items():
        has_exclusive = any(mode is LockMode.EXCLUSIVE for mode in txn_modes.values())
        assert not (has_exclusive and len(txn_modes) > 1), (
            f"violación de exclusión mutua sobre {resource!r}: {txn_modes}"
        )


def test_concurrent_workload_never_violates_mutual_exclusion() -> None:
    mgr: LockManager[str] = LockManager(default_timeout=2.0)
    stop_auditing = threading.Event()
    violations: list[str] = []

    def auditor() -> None:
        while not stop_auditing.is_set():
            try:
                _assert_mutual_exclusion(mgr.snapshot_holders())
            except AssertionError as exc:
                violations.append(str(exc))
                return

    def worker(thread_index: int, txn: TransactionId) -> None:
        rng = random.Random(_SEED + thread_index)  # semilla determinista por hilo
        held: set[str] = set()
        for _ in range(300):
            if rng.random() < 0.3 and held:
                resource = rng.choice(sorted(held))
                mgr.release(txn, resource)
                held.discard(resource)
                continue

            resource = rng.choice(_RESOURCES)
            mode = LockMode.SHARED if rng.random() < 0.5 else LockMode.EXCLUSIVE
            try:
                mgr.acquire(txn, resource, mode, timeout=0.2)
                held.add(resource)
            except DeadlockAbortError:
                mgr.release_all(txn)
                held.clear()
            except LockTimeoutError:
                continue  # contención pasajera, no es un fallo: se reintenta con otra operación
        mgr.release_all(txn)

    auditor_thread = threading.Thread(target=auditor)
    auditor_thread.start()

    worker_threads = [
        threading.Thread(target=worker, args=(index, txn))
        for index, txn in enumerate(_TRANSACTIONS)
    ]
    for t in worker_threads:
        t.start()
    for t in worker_threads:
        t.join(timeout=60)
    stop_auditing.set()
    auditor_thread.join(timeout=5)

    assert not any(t.is_alive() for t in worker_threads), (
        "una transacción se quedó esperando para siempre (sin deadlock real que lo justifique)"
    )
    assert not auditor_thread.is_alive()
    assert violations == []
    assert mgr.snapshot_wait_for_graph() == {}
    assert mgr.snapshot_holders() == {}  # todas las transacciones liberaron todo al terminar


def test_only_expected_exception_types_are_raised_under_contention() -> None:
    """Sanity check independiente: bajo la misma carga concurrente, ningún
    hilo debe recibir nunca una excepción que no sea una de las tres
    documentadas por `acquire()`/`release()` -- cualquier otra cosa
    (`KeyError`, `AttributeError`...) delataría un bug en el bookkeeping
    interno, no un resultado válido de contención."""
    mgr: LockManager[str] = LockManager(default_timeout=1.0)
    unexpected: list[BaseException] = []
    unexpected_lock = threading.Lock()

    def worker(thread_index: int, txn: TransactionId) -> None:
        rng = random.Random(_SEED + 1000 + thread_index)
        try:
            for _ in range(150):
                resource = rng.choice(_RESOURCES)
                mode = LockMode.SHARED if rng.random() < 0.5 else LockMode.EXCLUSIVE
                try:
                    mgr.acquire(txn, resource, mode, timeout=0.2)
                except (DeadlockAbortError, LockTimeoutError, LockManagerError):
                    pass
                if rng.random() < 0.4:
                    mgr.release_all(txn)
        except BaseException as exc:  # noqa: BLE001 - se reporta desde el hilo principal
            with unexpected_lock:
                unexpected.append(exc)
        finally:
            mgr.release_all(txn)

    threads = [
        threading.Thread(target=worker, args=(index, txn))
        for index, txn in enumerate(_TRANSACTIONS)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not any(t.is_alive() for t in threads)
    assert unexpected == []
