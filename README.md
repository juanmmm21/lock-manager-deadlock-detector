# lock-manager-deadlock-detector

**Component 5 of 11 (Transactions & Concurrency, 2 of 2)** in the [`strata-database-engine`](https://github.com/juanmmm21/strata-database-engine) ecosystem.
Repo: [`github.com/juanmmm21/lock-manager-deadlock-detector`](https://github.com/juanmmm21/lock-manager-deadlock-detector)

A from-scratch, fine-grained two-phase locking (2PL) manager, written in Python. It grants `SHARED`/`EXCLUSIVE` locks over abstract resource identifiers, queues conflicting requests per resource, maintains a global wait-for graph between transactions, and detects and resolves deadlocks the moment a cycle closes — with a configurable timeout as an independent safety net.

---

## What it is and what problem it solves

Pessimistic concurrency control makes a transaction wait, rather than let it race: before touching a resource, it must hold a lock on it, and that lock is only released once the transaction ends. That guarantees no dirty reads or lost updates ever happen — but it introduces a new failure mode optimistic schemes don't have. If transaction T1 holds a lock on A and waits for B, while T2 holds B and waits for A, neither will ever make progress: a deadlock. A real lock manager has to detect this and break it, not just hope it never happens.

This project implements exactly that: a lock manager that tracks who is waiting for whom in a wait-for graph, runs cycle detection every time a new wait edge would be added, and aborts one transaction — chosen by an explicit, documented, deterministic policy — the instant a cycle appears. A configurable timeout runs alongside it as an independent, complementary mechanism: useful when graph-based detection would be too expensive to run on every wait, or simply as a safety net against any wait that drags on regardless of cause.

## Role in `strata-database-engine`

```text
write-ahead-log-recovery ──┬─▶ bplus-tree-storage-engine ─┐
                            └─▶ lsm-tree-engine ────────────┤
                                                             ▼
                                          ┌────────────────────────────────┐
                                          │ lock-manager-deadlock-detector │  (this repo)
                                          │  shared/exclusive locks         │
                                          │  wait-for graph · deadlocks     │
                                          └───────────────┬─────────────────┘
                                                           │ resource ids only
                                                           ▼
                                                        nanosql
```

This repo does **not** import `bplus-tree-storage-engine`, `lsm-tree-engine` or `mvcc-transaction-manager`. It locks over abstract, hashable resource identifiers (a row id, a page id, a composite key — anything the caller decides to use as a key), never over a concrete storage structure; the real integration happens only inside `nanosql`. It is a sibling, alternative concurrency-control strategy to [`mvcc-transaction-manager`](https://github.com/juanmmm21/mvcc-transaction-manager): that project resolves concurrency **optimistically** (snapshot isolation, conflicts detected at commit time); this one is the **pessimistic** counterpart (blocking two-phase locking, conflicts prevented up front). `nanosql` picks one or the other per configuration — the two are alternatives, not layers on top of each other.

## Goal / skills demonstrated

- Classic two-phase locking (2PL) with a full shared/exclusive compatibility matrix, including in-place lock upgrades (`SHARED` → `EXCLUSIVE`) without releasing first.
- A FIFO, no-barging wait queue per resource: a request that could technically be granted is still made to wait behind an earlier, incompatible one, so a steady stream of compatible requests can never starve a blocked writer indefinitely.
- A wait-for graph maintained incrementally — not rebuilt from scratch — on every acquire, grant, release, timeout and abort.
- Deadlock detection via cycle-finding DFS, run exactly when a cycle could first appear (not on a timer), with a deterministic, documented victim-selection policy exposed as a pluggable `Protocol`.
- A timeout mechanism that is provably independent of graph-based detection — real deadlocks resolve in microseconds via the graph, never by waiting out the clock (see the [Benchmarks](#benchmarks) and the dedicated timeout test suite).
- Concurrency correctness demonstrated with real OS threads deliberately driven into 2-, 3- and larger-cycle deadlocks, not simulated single-threaded interleaving.

## How it works

### Compatibility and lock upgrades

Two lock modes, one compatibility rule: any number of `SHARED` locks on the same resource may coexist; an `EXCLUSIVE` lock is incompatible with anything else, from any other transaction. If a transaction already holds `SHARED` on a resource and requests `EXCLUSIVE`, that is an *upgrade*: it is treated like any other request, except the transaction's own existing lock is ignored when checking for conflicts (otherwise a transaction could never upgrade its own lock without releasing it first, which would itself introduce needless deadlocks). Two transactions that both hold `SHARED` and both request `EXCLUSIVE` at the same time form the canonical 2-transaction deadlock case, and the detector resolves it exactly like any other cycle.

### FIFO queueing without barging

Each resource has its own wait queue. When a lock is released (or a request added), the manager walks the queue from the front and grants the longest prefix of requests compatible with what is already held — stopping at the first incompatible one. Critically, it does **not** skip over that blocked request to grant a later, compatible one: a queue of `[EXCLUSIVE, SHARED]` leaves the `SHARED` request waiting even though it could technically coexist with the current holders, because letting it go first would leave the `EXCLUSIVE` request perpetually re-blocked by a steady stream of readers. This is the "no barging" policy, and it is what keeps every request's wait bounded.

### The wait-for graph

The graph only ever needs one edge type: a request that is blocked points to whatever is directly responsible for blocking it. Thanks to the no-barging queueing rule above, that is cheap to maintain — for any resource's queue, only the request at the front can be blocked by *holders*; everything behind it is blocked, transitively, by that front request, regardless of whether its own mode would otherwise be compatible. So the graph is rebuilt in O(queue length) per resource on every mutation, not O(n²) over all transactions.

### Deadlock detection and the victim policy

A cycle can only be created by the single new edge just added when a request becomes pending — the graph is an invariant-maintained DAG at every other moment, because any cycle is resolved in the same operation that creates it. So detection never needs a full graph scan: a single DFS from the transaction that just started waiting is enough to find the one cycle that could exist, if any.

The victim-selection policy is a pluggable `Protocol` (`protocols.DeadlockVictimPolicy`), with a documented default: **`YoungestTransactionVictimPolicy`** aborts the transaction with the highest `TransactionId` in the cycle (the most recently started). This is a deliberate choice, not an arbitrary one:

- It needs no extra bookkeeping — transaction ids are already monotonically assigned by the caller at `begin()`-time, so "youngest" is a free O(1) comparison.
- It minimizes expected wasted work — a younger transaction has, on average, done less than an older one caught in the same cycle.
- It avoids starving long-lived transactions — always sacrificing the youngest, rather than the oldest, is the same principle behind `wait-die`/`wound-wait` in the classical pessimistic concurrency control literature.

The alternative named in this project's design brief ("the one with the least work done") would require the caller to report a cost metric per transaction — information this module deliberately does not know, since it stays agnostic to what a transaction actually does. That alternative remains available to any caller by implementing the same `Protocol`.

### Timeout: independent, not a fallback disguised as the same code path

`acquire()` accepts a `timeout` (and `LockManager` a `default_timeout` applied when a call doesn't override it). This is checked entirely separately from cycle detection: a deadlock is caught the instant its cycle closes, well before any timeout could fire, and a timeout can fire on a wait that was never a deadlock at all — just heavy, non-cyclic contention. The test suite proves the two are decoupled by configuring a generous multi-second `default_timeout` and confirming a real deadlock still resolves in milliseconds.

## Architecture

```text
src/lock_manager_deadlock_detector/
├── __init__.py     # re-exported public API
├── models.py       # TransactionId, ResourceKey (generic), LockMode, RequestOutcome,
│                    # LockRequest, exception hierarchy
├── protocols.py     # DeadlockVictimPolicy + YoungestTransactionVictimPolicy (default)
├── pipeline.py         # LockManager: acquire/release, queueing, wait-for graph, detection
└── __main__.py         # demonstration CLI (demo / deadlock-demo / benchmark)
```

- **`models.py`** — `TransactionId` (a `NewType[int]`), a `ResourceKey` `TypeVar` bound to `Hashable` (so `LockManager` is generic over the resource identifier type — `str`, `bytes`, composite tuples, anything hashable), `LockMode`/`RequestOutcome` enums, the mutable `LockRequest` dataclass, and the exception hierarchy (`LockManagerError` → `DeadlockAbortError` / `LockTimeoutError` / `LockNotHeldError`).
- **`protocols.py`** — `DeadlockVictimPolicy`, the single pluggable coupling point for victim selection, and its documented default implementation.
- **`pipeline.py`** — all the locking logic: `acquire`/`release`/`release_all`, FIFO no-barging grant, wait-for graph maintenance, cycle detection and resolution, timeout handling.
- **`__main__.py`** — CLI with a single-threaded `demo` (compatibility and upgrade walkthrough), a real-threads `deadlock-demo` (induces and resolves 2- and 3-transaction cycles live), and a contention `benchmark`.

**Concurrency:** `LockManager` guards all of its internal bookkeeping — granted locks, per-resource wait queues, and the wait-for graph — with a single `threading.Condition` wrapping every public method. This is deliberate, not an oversight: the class that maintains a wait-for graph is a poor candidate for fine-grained internal locking, since resolving a cycle can require mutating several resources' bookkeeping at once, and getting that locking order wrong would mean a deadlock detector deadlocking on itself. The fine-grained locking this module *exposes* to callers (`acquire`/`release` scoped per resource) is orthogonal to how it protects its own state, and a single `Condition` is simple, correct, and cheap enough in practice — every internal operation is dictionary/list bookkeeping, never I/O, and the only blocking wait releases the `Condition` inside `wait()`, never holds it. See the module docstring in `pipeline.py` for the full rationale.

**Usage constraint:** each `TransactionId` must be driven by a single thread at a time — the same way a real transaction executes its operations sequentially. `acquire()` blocks the calling thread until the request resolves, so a transaction never has more than one pending request; calling `acquire()` for the same `txn_id` concurrently from two threads is unsupported and raises `LockManagerError`.

## Requirements and installation

- Python `>=3.11`

```bash
git clone https://github.com/juanmmm21/lock-manager-deadlock-detector.git
cd lock-manager-deadlock-detector
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"  # or: pip install -e . pytest mypy ruff
```

## Usage

### CLI

```bash
# single-threaded walkthrough of shared/exclusive compatibility and lock upgrades
python -m lock_manager_deadlock_detector demo

# induces and resolves a real 2-transaction and a real 3-transaction cycle deadlock
python -m lock_manager_deadlock_detector deadlock-demo

# throughput and deadlock-retry rate under contention, across key-space sizes
python -m lock_manager_deadlock_detector benchmark --threads 16 --ops-per-thread 200 --keys 2 8 32 128
```

### Programmatic usage

```python
from lock_manager_deadlock_detector import (
    DeadlockAbortError,
    LockManager,
    LockMode,
    TransactionId,
)

mgr: LockManager[str] = LockManager(default_timeout=5.0)

t1, t2 = TransactionId(1), TransactionId(2)
mgr.acquire(t1, "account-A", LockMode.EXCLUSIVE)
mgr.acquire(t2, "account-B", LockMode.EXCLUSIVE)

# from another thread driving t1: mgr.acquire(t1, "account-B", LockMode.EXCLUSIVE)
# from another thread driving t2: mgr.acquire(t2, "account-A", LockMode.EXCLUSIVE)
# -- one of the two raises DeadlockAbortError; the caller releases and can retry.

try:
    mgr.acquire(t2, "account-A", LockMode.EXCLUSIVE)
except DeadlockAbortError:
    mgr.release_all(t2)  # 2PL shrink phase: drop everything this transaction held
    # ... retry t2 from scratch with a fresh TransactionId
```

## Data format / interface exposed to `nanosql`

The integration contract with the rest of the ecosystem is the public API of `LockManager`, generic over any hashable resource identifier:

```python
class LockManager(Generic[ResourceKey]):
    def acquire(self, txn_id: TransactionId, resource: ResourceKey, mode: LockMode,
                timeout: float | None = ...) -> None: ...
    def release(self, txn_id: TransactionId, resource: ResourceKey) -> None: ...
    def release_all(self, txn_id: TransactionId) -> int: ...
    def holds(self, txn_id: TransactionId, resource: ResourceKey) -> LockMode | None: ...
```

`nanosql` (or any storage engine adapter) drives this by choosing what a "resource" is — a row id, a page id, a composite key — and calling `acquire`/`release_all` around its own transaction lifecycle. `LockManager` never inspects the resource's contents, so it composes with `bplus-tree-storage-engine` or `lsm-tree-engine` without either of them needing to know this module exists. The pluggable victim policy (`protocols.DeadlockVictimPolicy`) is the one other extension point: a caller with real per-transaction cost information can supply a custom policy instead of the default youngest-transaction rule.

## Development

```bash
pytest
ruff check .
ruff format --check .
mypy --strict src/
```

The test suite (35 tests) covers: shared/exclusive compatibility and in-place lock upgrades, FIFO no-barging queueing (a later compatible request cannot jump a blocked earlier one); the timeout mechanism (per-call override, manager-level default, explicit `timeout=None` overriding that default, and full cleanup of a timed-out request so it leaves no trace); deadlocks induced deliberately with real OS threads — a classic 2-transaction cross-resource cycle, a 2-transaction lock-upgrade cycle, a 3-transaction cycle, and parametrized larger cycles (4, 5, 8 transactions) — all verified to resolve with exactly one aborted victim and every other transaction completing, never a permanent hang; a negative case proving a non-cyclic wait chain never triggers a false deadlock; and two independent property-based tests with a fixed seed — one comparing 5,000 sequential operations against a reference compatibility model built from scratch, the other running real concurrent threads with per-thread deterministic RNGs (including real deadlocks resolved live) while an auditor thread continuously samples state and asserts the mutual-exclusion invariant never breaks.

One genuine bug surfaced by this test suite before publishing, worth naming rather than hiding: the first version of `_try_grant_resource` removed a newly-granted request from its queue but never cleared its wait-for graph edge, and `_recompute_edges` only walks requests still present in the queue — so a request granted in the same pass that unblocked it kept a stale, already-satisfied edge. It was caught by the `snapshot_wait_for_graph() == {}` assertion at the end of the deadlock tests, not by inspection.

## Benchmarks

Measured with the `benchmark` subcommand: 16 threads, 200 iterations each, every iteration acquiring two `EXCLUSIVE` locks from a shared key pool **in random order** — no consistent lock-ordering convention between threads, the classical root cause of 2PL deadlocks — with a short simulated hold ("does work") between the two acquisitions, and a client-side retry with a fresh transaction on `DeadlockAbortError`/`LockTimeoutError`:

| Key-space size | Completed | Throughput | Retries | Retry rate |
|---|---|---|---|---|
| 2 keys | 3,200 | ~1,060 ops/s | 3,291 | 102.84% |
| 8 keys | 3,200 | ~2,270 ops/s | 1,985 | 62.03% |
| 32 keys | 3,200 | ~11,350 ops/s | 91 | 2.84% |
| 128 keys | 3,200 | ~20,660 ops/s | 7 | 0.22% |

The retry rate is not a measurement of detector overhead — it is a direct read of the underlying birthday-paradox-style contention: with only 2 shared keys, essentially every pair of concurrent operations conflicts and a large fraction end up in a genuine deadlock cycle that must be resolved and retried, exactly as it would with any lock manager, real or textbook. What this benchmark actually demonstrates is that the detector keeps up: even at a >100% retry rate (multiple retries per successful operation), no thread ever hangs, and throughput recovers close to linearly as soon as the key-space is wide enough to make random two-resource acquisitions rarely collide. A first version of this benchmark, without the simulated hold between acquisitions, measured a 0.00% retry rate even at 2 keys and 16 threads — the critical section was so short that two threads essentially never interleaved inside it; the hold was added specifically to make the benchmark measure something real instead of an artifact of how fast Python can call two back-to-back dictionary lookups.

## Troubleshooting

- **`DeadlockAbortError` on `acquire()`:** this transaction was chosen as the victim to break a cycle detected in the wait-for graph. This is not a bug to work around — call `release_all()` to drop whatever it held (the 2PL shrink phase) and retry the transaction from scratch with a fresh `TransactionId`, the same pattern used in the "Programmatic usage" example and the benchmark.
- **`LockTimeoutError` on `acquire()`:** the configured timeout (per-call or the manager's `default_timeout`) elapsed before the lock could be granted. This can happen with or without a real deadlock present — it is an independent safety net, not evidence of one. The remedy is the same: release what was acquired so far and retry.
- **`LockNotHeldError` on `release()`:** the transaction does not currently hold a lock on that resource — either it was never acquired, or it was already released. Every `release()` must be paired with a prior successful `acquire()`.
- **A transaction seems stuck on `acquire()` far longer than expected, no exception raised:** check whether a `default_timeout` was configured — with `timeout=None` (explicit or as the manager default), `acquire()` waits indefinitely unless a real deadlock forms around it. A wait chain that is not cyclic (T3 waits for T2, T2 waits for T1) is not a deadlock and will not be aborted; it resolves on its own once the head of the chain releases.
- **`LockManagerError` mentioning "ya tiene una petición de lock pendiente":** the same `TransactionId` was used to call `acquire()` concurrently from two different threads. This is unsupported by design — see the usage constraint in [Architecture](#architecture) — each transaction must be driven by a single thread at a time.

## License

MIT — see [`LICENSE`](./LICENSE).
