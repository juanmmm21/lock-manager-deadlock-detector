"""Tipos de dominio del gestor de locks de dos fases con detección de deadlocks.

Este módulo gestiona locks sobre identificadores de recurso *abstractos*:
nunca importa `bplus-tree-storage-engine`, `lsm-tree-engine` ni
`mvcc-transaction-manager` (ver `protocols.py` para la razón de diseño). El
recurso bloqueado es cualquier valor hashable — típicamente un `str` o
`bytes` que identifique una fila o página en el motor de storage activo —
por eso `LockManager` es genérico sobre `ResourceKey` en vez de fijar un
tipo concreto como hacía `RowId` en `mvcc-transaction-manager`.
"""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass, field
from enum import Enum
from typing import Generic, NewType, TypeVar

TransactionId = NewType("TransactionId", int)
"""Identificador de transacción, asignado de forma monotónica por el
llamador (o por `nanosql` en la integración real). El criterio de selección
de víctima en un deadlock (ver `protocols.YoungestTransactionVictimPolicy`)
depende de que ids más altos signifiquen transacciones más recientes."""

ResourceKey = TypeVar("ResourceKey", bound=Hashable)
"""Tipo del identificador de recurso sobre el que se piden locks. Cualquier
valor hashable sirve (`str`, `bytes`, tuplas de claves compuestas...); el
gestor nunca inspecciona su contenido, sólo lo usa como clave de diccionario."""


class LockMode(Enum):
    """Modo de un lock sobre un recurso."""

    SHARED = "shared"
    """Compatible con cualquier número de otros locks `SHARED` sobre el
    mismo recurso, pero no con ningún `EXCLUSIVE`."""

    EXCLUSIVE = "exclusive"
    """Incompatible con cualquier otro lock (`SHARED` o `EXCLUSIVE`) que no
    pertenezca a la misma transacción."""


_COMPATIBILITY: dict[tuple[LockMode, LockMode], bool] = {
    (LockMode.SHARED, LockMode.SHARED): True,
    (LockMode.SHARED, LockMode.EXCLUSIVE): False,
    (LockMode.EXCLUSIVE, LockMode.SHARED): False,
    (LockMode.EXCLUSIVE, LockMode.EXCLUSIVE): False,
}


def modes_compatible(held: LockMode, requested: LockMode) -> bool:
    """Regla de compatibilidad clásica de locking de dos fases: sólo dos
    locks `SHARED` sobre el mismo recurso pueden coexistir."""
    return _COMPATIBILITY[(held, requested)]


class RequestOutcome(Enum):
    """Resultado (o estado en curso) de una `LockRequest`."""

    PENDING = "pending"
    GRANTED = "granted"
    ABORTED = "aborted"
    """La transacción fue elegida como víctima de un deadlock detectado en
    el wait-for graph y debe abortar (ver `DeadlockAbortError`)."""
    TIMED_OUT = "timed_out"
    """El timeout configurado expiró antes de que se pudiese conceder el
    lock, sin que necesariamente exista un deadlock real (ver
    `LockTimeoutError`)."""


@dataclass(slots=True, eq=False)
class LockRequest(Generic[ResourceKey]):
    """Una petición de lock, concedida o en espera.

    `eq=False` es deliberado: dos peticiones distintas de la misma
    transacción sobre el mismo recurso con el mismo modo no deben
    considerarse "iguales" a efectos de `list.remove`/pertenencia a un
    `set` — cada objeto se identifica por su identidad, nunca por sus
    campos, porque `outcome` se muta in-place mientras la petición vive en
    las colas de espera.
    """

    resource: ResourceKey
    txn_id: TransactionId
    mode: LockMode
    outcome: RequestOutcome = field(default=RequestOutcome.PENDING)


class LockManagerError(Exception):
    """Error base de todo el módulo."""


class DeadlockAbortError(LockManagerError):
    """La transacción fue elegida como víctima para romper un ciclo
    detectado en el wait-for graph (ver `protocols.DeadlockVictimPolicy`).
    La acción correcta del llamador es deshacer el trabajo de la
    transacción y, si tiene sentido, reintentarla desde cero."""


class LockTimeoutError(LockManagerError):
    """El timeout configurado (por llamada o el valor por defecto del
    `LockManager`) expiró antes de conceder el lock. Es un mecanismo
    independiente de la detección por grafo: sirve de red de seguridad
    cuando detectar el ciclo sale caro, o simplemente ante contención alta
    sin deadlock real."""


class LockNotHeldError(LockManagerError):
    """Se intentó liberar un lock que la transacción no tiene concedido
    sobre ese recurso (o el recurso no existe)."""
