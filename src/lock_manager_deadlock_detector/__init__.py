"""API pública de `lock-manager-deadlock-detector`."""

from .models import (
    DeadlockAbortError,
    LockManagerError,
    LockMode,
    LockNotHeldError,
    LockRequest,
    LockTimeoutError,
    RequestOutcome,
    TransactionId,
)
from .pipeline import LockManager
from .protocols import DeadlockVictimPolicy, YoungestTransactionVictimPolicy

__all__ = [
    "DeadlockAbortError",
    "DeadlockVictimPolicy",
    "LockManager",
    "LockManagerError",
    "LockMode",
    "LockNotHeldError",
    "LockRequest",
    "LockTimeoutError",
    "RequestOutcome",
    "TransactionId",
    "YoungestTransactionVictimPolicy",
]
