# Durable Incremental Ledgers

## Safety Contract

The Rust state and intent stores append a typed, SHA-256 checked delta and call
`sync_all` before returning a successful mutation. Prepared intents and strategy
ownership still become durable before any exchange request. ACK/unknown results
are also synchronous journal commits, not fire-and-forget background writes.
Quantity/cell capacity, ownership, deduplication, and state validation remain.
Only an unchanged, successfully validated pair of concrete file-store revisions
can reuse its cross-ledger ownership proof. Generic/custom stores cannot opt in.

## Files and Recovery

Legacy JSON is loaded without modification. The first mutation creates a private
`<file>.wal` and atomically publishes a versioned checkpoint. A separate private
`<file>.wal.lock` fences the writer while allowing read-only concurrent inspection.
Each frame has a bounded payload, a checksummed header, a payload checksum, and
strict consecutive base/next revisions. Checkpoints bind the log identity and kind.

Loading replays the checkpoint and complete frames, then validates the recovered
state. An incomplete EOF frame is ignored without modifying evidence. Only the
next exclusive writer may remove this incomplete tail. Missing logs, complete
corrupt frames, identity/revision conflicts, and stale writers fail closed.
After an ambiguous append/sync error the writer is poisoned; recovery must replay
and reconcile deterministic client IDs instead of blindly sending again.

## Full Checkpoints

After 64 commits, or on the first commit after 30 seconds, one background worker
captures an immutable state and atomically writes a complete JSON checkpoint.
No new checkpoint is started while that store's previous worker is active.
The writer lease outlives every checkpoint worker, including during shutdown.
A failed checkpoint is logged; the durable journal is retained and remains the
source of truth. Quiet strategies need no timer: their complete state is durable
in the WAL even if no later commit triggers a checkpoint.

The first migration is deliberately synchronous and expensive. Measure it
separately from warm commits. Background checkpoints still consume CPU and disk
bandwidth; removing them from the synchronous call stack is not zero resource cost.

## Inspection and Backups

Raw JSON is a periodic checkpoint, NOT necessarily the current state. Use the
Rust binary, including on read-only mounts:

```text
grid-trading-server --export-strategy-file /data/run/strategy.json
grid-trading-server --export-intent-file /data/run/intents.json
grid-trading-server --validate-strategy-file /data/run/strategy.json
```

Exports read and validate through the same replaying loaders without starting an
HTTP listener, changing files, acquiring the writer lease, or contacting exchanges.
Independent live exports are not a cross-ledger atomic backup. Use a filesystem
snapshot or a quiescent runtime for a consistent backup, and include both JSON
files and both WALs. Do not copy only the JSON checkpoints.

## Compatibility and Retention

Old binaries reject the new checkpoint envelope. A rollback to a pre-WAL binary
requires a quiescent, validated, full-state export of BOTH ledgers, preserving all
original files as evidence. Never restore an older backup into a live strategy,
delete a live WAL, or run the legacy Python repair scripts against this format.

This revision retains committed WAL history, avoiding unsafe online rotation
races with readers and checkpoints. Monitor WAL disk growth and checkpoint errors;
checkpointing improves replay time but does not shrink WAL files. Retention or
generation-based compaction is a separate migration, not an implicit deletion of
live evidence. Disk pressure is not permission to skip durable writes.

## Measurement Scope

The ignored Rust persistence benchmark runs only against a copied offline fixture
via `GRID_BENCH_STRATEGY_FILE`. It compares complete JSON persistence with warm
WAL persistence on the same history, excluding successor construction/validation
equally from both warm measurements. Its timestamp-only transitions isolate the
persistence cost, not full fill-accounting cost. These numbers are not exchange
fill-to-order latency or a production latency guarantee.

### Offline Result, 2026-10-21

Windows GNU debug build; copied QUSDT state with 4,691 orders / 10,965,542 bytes;
20 warm mutations, no concurrent Cargo build during the accepted run:

| Persistence Only | Complete JSON | Incremental WAL |
| --- | ---: | ---: |
| Median | 1,038.99 ms | 46.13 ms |
| Mean | 1,429.36 ms | 83.64 ms |
| Maximum | 3,253.21 ms | 427.65 ms |
| Bytes Written, 20 Mutations | 219,311,560 | 190,080 |

Written bytes fell 99.913%; median persistence time fell approximately 95.56%.
Cold migration including validation took 11.80 seconds in this debug environment.
The WAL still has nonzero fsync tail latency. Do not transplant these absolute
Windows timings to Linux production or advertise them as fill-to-order latency.
The input fixture is private, git-ignored, and not shipped with source or images.
