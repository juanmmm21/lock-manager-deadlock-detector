"""CLI de demostración para lock-manager-deadlock-detector.

Subcomandos:

    demo            guion fijo y de un solo hilo que reproduce la
                     compatibilidad shared/shared, la incompatibilidad
                     shared/exclusive (con timeout para no bloquear la CLI)
                     y un upgrade shared -> exclusive en el sitio.
    deadlock-demo    induce, con hilos reales, un deadlock de 2 y otro de
                     3+ transacciones en ciclo (cada una retiene su propio
                     recurso y pide el del siguiente), y muestra que el
                     detector resuelve ambos sin dejar ninguna transacción
                     esperando para siempre.
    benchmark        mide throughput bajo contención: varios hilos piden
                     dos recursos exclusivos en orden aleatorio (sin
                     ordenación consistente entre hilos, la causa clásica
                     de deadlocks en 2PL) y reintentan ante
                     `DeadlockAbortError`/`LockTimeoutError`.
"""

from __future__ import annotations

import argparse
import itertools
import random
import sys
import threading
import time
from collections.abc import Sequence

from lock_manager_deadlock_detector.models import (
    DeadlockAbortError,
    LockManagerError,
    LockMode,
    LockTimeoutError,
    TransactionId,
)
from lock_manager_deadlock_detector.pipeline import LockManager


class _TxnIdAllocator:
    """Contador monotónico thread-safe: en este ecosistema el propio
    llamador (aquí, la CLI; en `nanosql`, el gestor de transacciones real)
    es responsable de asignar `TransactionId`s únicos -- `LockManager` nunca
    los genera, sólo los usa como clave."""

    def __init__(self) -> None:
        self._counter = itertools.count(1)
        self._lock = threading.Lock()

    def next(self) -> TransactionId:
        with self._lock:
            return TransactionId(next(self._counter))


def _run_demo() -> int:
    ids = _TxnIdAllocator()

    print("=== locks compartidos: compatibles entre sí ===")
    mgr: LockManager[str] = LockManager()
    t1, t2 = ids.next(), ids.next()
    mgr.acquire(t1, "row-42", LockMode.SHARED)
    mgr.acquire(t2, "row-42", LockMode.SHARED)
    mode_t1 = mgr.holds(t1, "row-42")
    mode_t2 = mgr.holds(t2, "row-42")
    print(f"  t{t1} shared row-42: {mode_t1.value if mode_t1 else None}")
    print(f"  t{t2} shared row-42: {mode_t2.value if mode_t2 else None}")
    mgr.release_all(t1)
    mgr.release_all(t2)

    print()
    print("=== lock exclusivo: incompatible con cualquier otro lock ===")
    mgr = LockManager()
    t1, t2 = ids.next(), ids.next()
    mgr.acquire(t1, "row-42", LockMode.EXCLUSIVE)
    print(f"  t{t1} exclusive row-42: concedido")
    try:
        mgr.acquire(t2, "row-42", LockMode.EXCLUSIVE, timeout=0.05)
        print(f"  t{t2} exclusive row-42: concedido -- ¡esto no debería pasar!")
        return 1
    except LockTimeoutError:
        print(f"  t{t2} exclusive row-42: timeout esperando a t{t1} (esperado)")
    mgr.release_all(t1)
    mgr.acquire(t2, "row-42", LockMode.EXCLUSIVE)
    print(f"  t{t2} exclusive row-42: concedido tras liberar t{t1}")
    mgr.release_all(t2)

    print()
    print("=== upgrade: de shared a exclusive sin liberar primero ===")
    mgr = LockManager()
    t1 = ids.next()
    mgr.acquire(t1, "row-42", LockMode.SHARED)
    mgr.acquire(t1, "row-42", LockMode.EXCLUSIVE)  # nadie más lo tiene: upgrade inmediato
    mode_t1 = mgr.holds(t1, "row-42")
    print(f"  t{t1} tras upgrade: {mode_t1.value if mode_t1 else None}")
    mgr.release_all(t1)
    return 0


def _cmd_demo(_: argparse.Namespace) -> int:
    return _run_demo()


def _induce_and_resolve_cycle(resources: Sequence[str], ids: _TxnIdAllocator) -> bool:
    """Levanta `len(resources)` hilos que forman deliberadamente un ciclo
    de espera: el hilo `i` retiene `resources[i]` en exclusiva y luego pide
    `resources[(i + 1) % n]`, también en exclusiva -- si todos llegan a
    pedir el siguiente antes de que nadie libere el suyo, el wait-for graph
    cierra un ciclo de tamaño `n`."""
    n = len(resources)
    mgr: LockManager[str] = LockManager(default_timeout=5.0)
    txns = [ids.next() for _ in range(n)]
    barrier = threading.Barrier(n)
    results: dict[TransactionId, str] = {}
    results_lock = threading.Lock()

    def worker(index: int) -> None:
        txn = txns[index]
        own = resources[index]
        nxt = resources[(index + 1) % n]
        mgr.acquire(txn, own, LockMode.EXCLUSIVE)
        barrier.wait()  # las n transacciones ya tienen su recurso antes de pedir el siguiente
        try:
            mgr.acquire(txn, nxt, LockMode.EXCLUSIVE)
            outcome = "completó"
        except DeadlockAbortError:
            outcome = "abortada (víctima del detector de deadlocks)"
        mgr.release_all(txn)
        with results_lock:
            results[txn] = outcome

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    for index, txn in enumerate(txns):
        print(f"  t{txn} (posee {resources[index]}): {results.get(txn, 'sin terminar!')}")

    aborted = sum(1 for outcome in results.values() if outcome.startswith("abortada"))
    completed = sum(1 for outcome in results.values() if outcome == "completó")
    resolved_cleanly = len(results) == n and aborted >= 1 and completed == n - 1
    print(f"  resuelto sin bloqueo permanente: {resolved_cleanly}")
    return resolved_cleanly


def _run_deadlock_demo() -> int:
    ids = _TxnIdAllocator()
    print("=== deadlock de 2 transacciones (recursos cruzados) ===")
    two_ok = _induce_and_resolve_cycle(["account-A", "account-B"], ids)
    print()
    print("=== deadlock de 3 transacciones en ciclo ===")
    three_ok = _induce_and_resolve_cycle(["page-1", "page-2", "page-3"], ids)
    return 0 if (two_ok and three_ok) else 1


def _cmd_deadlock_demo(_: argparse.Namespace) -> int:
    return _run_deadlock_demo()


def _benchmark_one_run(num_threads: int, ops_per_thread: int, num_keys: int) -> None:
    """Cada operación pide dos recursos exclusivos de un pool pequeño, en
    un orden elegido al azar por el propio hilo -- deliberadamente sin
    ordenación consistente entre hilos, la causa clásica de deadlocks bajo
    2PL -- y reintenta con una transacción nueva ante cualquier
    `DeadlockAbortError`/`LockTimeoutError`.

    Se simula un pequeño trabajo (`time.sleep`) entre la primera y la
    segunda adquisición: sin él, la sección crítica es tan corta que dos
    hilos casi nunca se entrelazan lo bastante para formar un ciclo real, y
    el benchmark no mediría nada distinto de la ausencia de contención --
    esto se descubrió al correr el benchmark sin el sleep, con retry_rate
    en 0.00% incluso con sólo 2 claves compartidas entre 16 hilos.
    """
    mgr: LockManager[str] = LockManager(default_timeout=2.0)
    ids = _TxnIdAllocator()
    keys = [f"key-{i}" for i in range(num_keys)]
    retries = 0
    retries_lock = threading.Lock()
    start_barrier = threading.Barrier(num_threads)

    def worker() -> None:
        nonlocal retries
        rng = random.Random()
        start_barrier.wait()
        for _ in range(ops_per_thread):
            while True:
                txn = ids.next()
                a, b = rng.sample(keys, 2)
                try:
                    mgr.acquire(txn, a, LockMode.EXCLUSIVE)
                    time.sleep(0.0005)
                    mgr.acquire(txn, b, LockMode.EXCLUSIVE)
                    mgr.release_all(txn)
                    break
                except (DeadlockAbortError, LockTimeoutError):
                    mgr.release_all(txn)
                    with retries_lock:
                        retries += 1

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    started_at = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - started_at

    total_ops = num_threads * ops_per_thread
    throughput = total_ops / elapsed if elapsed > 0 else float("inf")
    retry_rate = retries / total_ops if total_ops else 0.0
    print(
        f"  keys={num_keys:4d} completed={total_ops:5d} elapsed={elapsed:6.3f}s "
        f"throughput={throughput:8.0f} ops/s retries={retries:5d} retry_rate={retry_rate:6.2%}"
    )


def _cmd_benchmark(args: argparse.Namespace) -> int:
    print(
        f"=== throughput bajo contención: {args.threads} hilos x {args.ops_per_thread} "
        "adquisiciones de 2 recursos exclusivos, orden aleatorio ==="
    )
    for num_keys in args.keys:
        _benchmark_one_run(args.threads, args.ops_per_thread, num_keys)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lock-manager-deadlock-detector")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("demo", help="compatibilidad shared/exclusive y upgrade, de un solo hilo")
    subparsers.add_parser(
        "deadlock-demo", help="induce y resuelve deadlocks de 2 y 3+ transacciones con hilos reales"
    )

    benchmark = subparsers.add_parser(
        "benchmark", help="mide throughput y tasa de reintento bajo contención multi-recurso"
    )
    benchmark.add_argument("--threads", type=int, default=8)
    benchmark.add_argument("--ops-per-thread", type=int, default=500)
    benchmark.add_argument("--keys", type=int, nargs="+", default=[8, 64])

    return parser


_HANDLERS = {
    "demo": _cmd_demo,
    "deadlock-demo": _cmd_deadlock_demo,
    "benchmark": _cmd_benchmark,
}


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _HANDLERS[args.command]
    try:
        return handler(args)
    except LockManagerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
