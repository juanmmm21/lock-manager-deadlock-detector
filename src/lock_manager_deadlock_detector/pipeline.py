"""Núcleo del gestor de locks de dos fases con detección de deadlocks.

`LockManager` concede locks `SHARED`/`EXCLUSIVE` de grano fino sobre
recursos abstractos, mantiene una cola de espera FIFO por recurso y un
wait-for graph global entre transacciones, y detecta y resuelve ciclos
(deadlocks) en cuanto aparecen — con un timeout configurable como red de
seguridad independiente.

Estrategia de sincronización (obligatoria documentarla, ver `CLAUDE.md`
sección D): todo el estado compartido — tabla de locks concedidos, colas de
espera por recurso y wait-for graph — está protegido por un único
`threading.Condition` que envuelve cada operación pública. Es
deliberadamente de grano grueso, no por pereza sino porque este mismo
código es el que mantiene el wait-for graph: si el propio gestor usase
locking de grano fino (un lock por recurso) para proteger su bookkeeping
interno, tendría que resolver su propio problema de ordenación de locks
para evitar deadlocks *del gestor consigo mismo* al mutar varios recursos a
la vez durante la resolución de un ciclo — irónico para un detector de
deadlocks. El locking fino que este módulo *expone* a sus llamadores
(`acquire`/`release` por recurso) es ortogonal a cómo protege su propio
estado interno, y un único `Condition` es simple, correcto y suficiente:
las operaciones son baratas (manipulación de diccionarios y listas
pequeñas), nunca hacen I/O ni se quedan bloqueadas mientras lo mantienen
tomado — el bloqueo de un `acquire()` que debe esperar ocurre liberando el
`Condition` dentro de `wait()`, nunca reteniéndolo.

Restricción de uso: cada `TransactionId` debe ser conducido por un único
hilo a la vez (igual que una transacción real ejecuta sus operaciones de
forma secuencial). Llamar a `acquire()` para el mismo `txn_id` desde dos
hilos concurrentes no está soportado — `acquire()` bloquea el hilo llamante
hasta que la petición se resuelve, así que una transacción nunca tiene más
de una petición pendiente a la vez.
"""

from __future__ import annotations

import threading
import time
from typing import Generic

from .models import (
    DeadlockAbortError,
    LockManagerError,
    LockMode,
    LockNotHeldError,
    LockRequest,
    LockTimeoutError,
    RequestOutcome,
    ResourceKey,
    TransactionId,
    modes_compatible,
)
from .protocols import DeadlockVictimPolicy, YoungestTransactionVictimPolicy


class _Unset:
    """Sentinela para distinguir "no se pasó `timeout`" (usar el valor por
    defecto del gestor) de "se pasó `timeout=None`" (esperar indefinidamente
    incluso si el gestor tiene un `default_timeout` configurado)."""

    __slots__ = ()


_UNSET = _Unset()


class LockManager(Generic[ResourceKey]):
    """Gestor de locks de dos fases (2PL) de grano fino con detección de
    deadlocks por wait-for graph.

    Genérico sobre el tipo de identificador de recurso (`str`, `bytes`,
    tuplas de claves compuestas...): el gestor nunca inspecciona el
    contenido de un recurso, sólo lo usa como clave de diccionario.
    """

    def __init__(
        self,
        default_timeout: float | None = None,
        victim_policy: DeadlockVictimPolicy | None = None,
    ) -> None:
        self._cond = threading.Condition()
        self._default_timeout = default_timeout
        self._victim_policy: DeadlockVictimPolicy = (
            victim_policy or YoungestTransactionVictimPolicy()
        )
        self._holders: dict[ResourceKey, dict[TransactionId, LockMode]] = {}
        self._wait_queue: dict[ResourceKey, list[LockRequest[ResourceKey]]] = {}
        self._wait_for: dict[TransactionId, set[TransactionId]] = {}
        self._pending_by_txn: dict[TransactionId, LockRequest[ResourceKey]] = {}
        self._held_resources: dict[TransactionId, set[ResourceKey]] = {}

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def acquire(
        self,
        txn_id: TransactionId,
        resource: ResourceKey,
        mode: LockMode,
        timeout: float | None | _Unset = _UNSET,
    ) -> None:
        """Adquiere `resource` en modo `mode` para `txn_id`, bloqueando el
        hilo llamante si hace falta esperar.

        Si `txn_id` ya tiene concedido un lock igual o más fuerte
        (`EXCLUSIVE` cubre cualquier petición; `SHARED` cubre una petición
        `SHARED` repetida) sobre `resource`, es un no-op inmediato. Si ya
        tiene `SHARED` y pide `EXCLUSIVE`, es una petición de *upgrade*:
        se trata como cualquier otra petición, pero ignorando el propio
        lock `SHARED` de `txn_id` al comprobar compatibilidad (de lo
        contrario una transacción nunca podría hacer upgrade de su propio
        lock sin liberarlo primero).

        `timeout` en segundos: si se omite, se usa el `default_timeout`
        del gestor (`None` = esperar indefinidamente salvo deadlock). Pasar
        explícitamente `timeout=None` espera indefinidamente aunque el
        gestor tenga un `default_timeout` configurado.

        Lanza `DeadlockAbortError` si esta transacción es elegida víctima
        para romper un ciclo detectado en el wait-for graph, o
        `LockTimeoutError` si expira el timeout antes de conceder el lock.
        """
        effective_timeout = self._default_timeout if isinstance(timeout, _Unset) else timeout
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout

        with self._cond:
            if txn_id in self._pending_by_txn:
                raise LockManagerError(
                    f"la transacción {txn_id} ya tiene una petición de lock pendiente: "
                    "una transacción sólo puede esperar un lock a la vez"
                )
            existing_mode = self._holders.get(resource, {}).get(txn_id)
            if existing_mode is LockMode.EXCLUSIVE or existing_mode is mode:
                return  # ya concedido en un modo igual o más fuerte: no-op

            req: LockRequest[ResourceKey] = LockRequest(resource=resource, txn_id=txn_id, mode=mode)
            self._wait_queue.setdefault(resource, []).append(req)
            self._pending_by_txn[txn_id] = req
            self._try_grant_resource(resource)
            self._recompute_edges(resource)
            if req.outcome is RequestOutcome.PENDING:
                self._detect_and_resolve_deadlock(txn_id)
            self._cond.notify_all()

            while req.outcome is RequestOutcome.PENDING:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    req.outcome = RequestOutcome.TIMED_OUT
                    self._cancel_pending(req)
                    self._cond.notify_all()
                    break
                self._cond.wait(timeout=remaining)

            if req.outcome is RequestOutcome.ABORTED:
                raise DeadlockAbortError(
                    f"la transacción {txn_id} fue abortada para romper un deadlock "
                    f"al esperar el lock {mode.value} sobre {resource!r}"
                )
            if req.outcome is RequestOutcome.TIMED_OUT:
                raise LockTimeoutError(
                    f"la transacción {txn_id} agotó el timeout esperando el lock "
                    f"{mode.value} sobre {resource!r}"
                )

    def release(self, txn_id: TransactionId, resource: ResourceKey) -> None:
        """Libera el lock que `txn_id` tiene concedido sobre `resource`,
        intentando conceder acto seguido el mayor prefijo compatible de la
        cola de espera de ese recurso.
        """
        with self._cond:
            holders = self._holders.get(resource)
            if holders is None or txn_id not in holders:
                raise LockNotHeldError(
                    f"la transacción {txn_id} no tiene ningún lock concedido sobre {resource!r}"
                )
            del holders[txn_id]
            if not holders:
                del self._holders[resource]
            held = self._held_resources.get(txn_id)
            if held is not None:
                held.discard(resource)
                if not held:
                    del self._held_resources[txn_id]
            self._try_grant_resource(resource)
            self._recompute_edges(resource)
            self._cond.notify_all()

    def release_all(self, txn_id: TransactionId) -> int:
        """Libera todos los locks concedidos a `txn_id` (fase de
        contracción de 2PL al terminar una transacción, por commit o
        abort). Devuelve cuántos recursos se liberaron."""
        with self._cond:
            resources: list[ResourceKey] = list(self._held_resources.get(txn_id, ()))
        for resource in resources:
            self.release(txn_id, resource)
        return len(resources)

    def holds(self, txn_id: TransactionId, resource: ResourceKey) -> LockMode | None:
        """Modo en el que `txn_id` tiene concedido `resource`, o `None`."""
        with self._cond:
            return self._holders.get(resource, {}).get(txn_id)

    def snapshot_wait_for_graph(self) -> dict[TransactionId, frozenset[TransactionId]]:
        """Copia inmutable del wait-for graph en este instante, útil para
        depuración y para el subcomando `deadlock-demo` de la CLI."""
        with self._cond:
            return {txn: frozenset(blockers) for txn, blockers in self._wait_for.items()}

    def snapshot_holders(self) -> dict[ResourceKey, dict[TransactionId, LockMode]]:
        """Copia atómica de todos los locks concedidos en este instante,
        recurso por recurso. Pensada para tests de propiedades e
        introspección: al tomarse bajo `self._cond`, es una foto coherente
        de todo el estado a la vez, a diferencia de llamar a `holds()`
        recurso por recurso (que sólo es atómico por llamada individual).
        """
        with self._cond:
            return {resource: dict(holders) for resource, holders in self._holders.items()}

    # ------------------------------------------------------------------
    # Concesión de locks en cola (FIFO con prevención de starvation)
    # ------------------------------------------------------------------

    def _try_grant_resource(self, resource: ResourceKey) -> None:
        """Concede, en orden FIFO, el mayor prefijo de la cola de espera de
        `resource` que sea compatible con lo ya concedido — parando en la
        primera petición incompatible ("no barging": aunque una petición
        más atrás en la cola fuese compatible con los holders actuales, no
        se le deja adelantar a la que la bloquea, para no dejarla en
        inanición indefinidamente).
        """
        queue = self._wait_queue.get(resource)
        if not queue:
            return
        holders = self._holders.setdefault(resource, {})
        while queue and self._grantable(queue[0], holders):
            req = queue.pop(0)
            holders[req.txn_id] = req.mode
            req.outcome = RequestOutcome.GRANTED
            self._pending_by_txn.pop(req.txn_id, None)
            self._held_resources.setdefault(req.txn_id, set()).add(resource)
            # una petición concedida deja de "esperar a" nadie: si no se
            # limpia aquí, `_recompute_edges` no la vería (ya no está en la
            # cola) y dejaría una arista fantasma en el wait-for graph.
            self._wait_for.pop(req.txn_id, None)
        if not queue:
            del self._wait_queue[resource]
        if not holders:
            del self._holders[resource]

    @staticmethod
    def _grantable(req: LockRequest[ResourceKey], holders: dict[TransactionId, LockMode]) -> bool:
        return all(
            other_txn == req.txn_id or modes_compatible(other_mode, req.mode)
            for other_txn, other_mode in holders.items()
        )

    # ------------------------------------------------------------------
    # Wait-for graph
    # ------------------------------------------------------------------

    def _recompute_edges(self, resource: ResourceKey) -> None:
        """Reconstruye las aristas del wait-for graph que pertenecen a las
        peticiones pendientes de `resource`.

        Con la política "no barging" de `_try_grant_resource`, sólo la
        primera petición pendiente de la cola puede estar bloqueada por los
        holders actuales; todas las demás están bloqueadas, transitivamente,
        por esa primera petición (aunque su propio modo fuese compatible
        con los holders) — así que basta una arista directa hacia ella para
        que la búsqueda de ciclos capture correctamente esa dependencia.
        """
        queue = self._wait_queue.get(resource, [])
        for req in queue:
            self._wait_for.pop(req.txn_id, None)
        if not queue:
            return
        holders = self._holders.get(resource, {})
        first = queue[0]
        blockers = {
            txn
            for txn, mode in holders.items()
            if txn != first.txn_id and not modes_compatible(mode, first.mode)
        }
        if blockers:
            self._wait_for[first.txn_id] = blockers
        for later in queue[1:]:
            self._wait_for[later.txn_id] = {first.txn_id}

    def _find_cycle_containing(self, start: TransactionId) -> list[TransactionId] | None:
        """DFS determinista (vecinos visitados en orden ordenado) que busca
        un ciclo en el wait-for graph alcanzable desde `start`. Devuelve
        las transacciones del ciclo en orden, o `None` si no hay ninguno.
        """
        path: list[TransactionId] = []
        on_path: set[TransactionId] = set()
        visited: set[TransactionId] = set()

        def dfs(node: TransactionId) -> list[TransactionId] | None:
            if node in on_path:
                return path[path.index(node) :]
            if node in visited:
                return None
            visited.add(node)
            path.append(node)
            on_path.add(node)
            for neighbor in sorted(self._wait_for.get(node, ())):
                found = dfs(neighbor)
                if found is not None:
                    return found
            path.pop()
            on_path.discard(node)
            return None

        return dfs(start)

    def _detect_and_resolve_deadlock(self, start: TransactionId) -> None:
        """Busca un ciclo que pase por `start` (la transacción que acaba de
        pasar a pendiente) y, si existe, aborta a la víctima elegida por
        `self._victim_policy`.

        Sólo hace falta comprobar ciclos que pasen por `start`: antes de
        esta llamada el grafo era acíclico (invariante mantenido porque
        cualquier ciclo se resuelve en la misma operación que lo crea), y
        la única arista nueva desde la última comprobación es la que
        `_recompute_edges` acaba de añadir para `start` — así que cualquier
        ciclo nuevo tiene que pasar por ella.
        """
        cycle = self._find_cycle_containing(start)
        if cycle is None:
            return
        victim = self._victim_policy.choose_victim(cycle)
        victim_request = self._pending_by_txn.get(victim)
        if victim_request is None:
            return
        victim_request.outcome = RequestOutcome.ABORTED
        self._cancel_pending(victim_request)

    def _cancel_pending(self, req: LockRequest[ResourceKey]) -> None:
        """Retira `req` de la cola de espera y del wait-for graph (ya sea
        porque fue elegida víctima de un deadlock o porque su timeout
        expiró), e intenta conceder lo que haya quedado desbloqueado."""
        queue = self._wait_queue.get(req.resource)
        if queue is not None and req in queue:
            queue.remove(req)
            if not queue:
                del self._wait_queue[req.resource]
        if self._pending_by_txn.get(req.txn_id) is req:
            del self._pending_by_txn[req.txn_id]
        self._wait_for.pop(req.txn_id, None)
        self._try_grant_resource(req.resource)
        self._recompute_edges(req.resource)
