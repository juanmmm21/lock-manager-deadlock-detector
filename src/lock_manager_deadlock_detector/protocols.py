"""Política de selección de víctima ante un deadlock detectado.

El `Definition of Done` de este subproyecto exige elegir y documentar un
criterio explícito y determinista para decidir qué transacción abortar
cuando `pipeline.LockManager` detecta un ciclo en el wait-for graph. En vez
de dejar esa decisión hardcodeada dentro de `LockManager`, se expone como
un `Protocol` pluggable — exactamente el mismo patrón que usa el módulo
hermano `mvcc-transaction-manager` con `RowStore` — para que la política por
defecto pueda sustituirse (por ejemplo, por una que use un coste de trabajo
real en lugar de la edad de la transacción) sin tocar la lógica de
detección de ciclos.

Este módulo no importa código de ningún otro subproyecto del ecosistema: la
integración real ocurre siempre dentro de `nanosql`.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from .models import TransactionId


@runtime_checkable
class DeadlockVictimPolicy(Protocol):
    """Elige, de entre las transacciones que forman un ciclo detectado en
    el wait-for graph, cuál debe abortarse para romperlo.

    Abortar exactamente una transacción del ciclo basta: como el ciclo es
    un circuito cerrado de dependencias "espera a", eliminar cualquiera de
    sus vértices (y, con él, todas sus aristas salientes) lo rompe.
    """

    def choose_victim(self, cycle: Sequence[TransactionId]) -> TransactionId:
        """`cycle` contiene, en orden, las transacciones que forman el
        ciclo (sin repetir el primer elemento al final). Debe devolver
        siempre un elemento de `cycle`."""
        ...


class YoungestTransactionVictimPolicy:
    """Política por defecto: aborta la transacción más joven del ciclo
    (el `TransactionId` más alto).

    Criterio elegido y su justificación:

    - **Determinista sin estado adicional:** los ids de transacción ya son
      monotónicamente crecientes por construcción (se asignan en `begin`),
      así que "más joven" es una comparación O(1) sobre información que el
      gestor ya tiene — no hace falta instrumentar cada transacción con un
      contador de trabajo realizado ni un reloj de pared.
    - **Minimiza trabajo desperdiciado en expectativa:** una transacción
      más joven ha tenido, en promedio, menos tiempo para acumular lecturas
      y escrituras que abortar y rehacer que una transacción más vieja
      implicada en el mismo ciclo.
    - **Evita inanición (starvation) de transacciones antiguas:** si en su
      lugar se abortase sistemáticamente a la transacción más vieja de cada
      ciclo, una transacción de larga duración podría no terminar nunca
      bajo contención sostenida, reabortada una y otra vez apenas más
      vieja que la siguiente oleada de transacciones jóvenes con las que
      choca. Preferir siempre a la más joven como víctima es el mismo
      principio que protege a `wait-die`/`wound-wait` en la literatura
      clásica de control de concurrencia pesimista.

    La alternativa mencionada en el `AGENTS.md` de este subproyecto ("la de
    menor coste") exigiría que el llamador reportase al `LockManager` una
    métrica de trabajo realizado por transacción — información que este
    módulo, deliberadamente agnóstico del contenido de las transacciones,
    no tiene por qué conocer. Se documenta aquí como alternativa válida y
    se deja abierta vía este mismo `Protocol` para quien la necesite.
    """

    def choose_victim(self, cycle: Sequence[TransactionId]) -> TransactionId:
        if not cycle:
            raise ValueError("no se puede elegir una víctima de un ciclo vacío")
        return max(cycle)
