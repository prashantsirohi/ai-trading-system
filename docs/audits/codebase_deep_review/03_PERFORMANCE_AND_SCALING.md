# Performance and scaling assessment

- **Purpose:** Assess current hot paths, scaling boundaries, query strategy, and required performance benchmarks.
- **Audience:** Platform, data, API, and frontend maintainers.
- **Last verified:** 2026-07-13
- **Source of truth:** Repository measurements and cited runtime paths; live-store query timing was not performed.

---

## Verdict

The system is adequate for one operator and the present daily batch, but its dominant cost model is repeated full-store scans plus in-memory pandas materialization. The first scaling boundary is not CPU; it is DuckDB coordination. Multiple independently locked connections contend for the same files while API background work, heartbeats, pipeline stages, and operator diagnostics overlap.

## Measured baseline

| Measure | Observed result | Interpretation |
|---|---:|---|
| Python package size | 127,211 lines | Large modular monolith; optimization must be profiled by stage |
| Frontend source | 25,032 lines | Reasonable, but several page-level components are oversized |
| OHLCV DuckDB | 3.85 GB | Full scans and broad DataFrame loads are already material |
| Control-plane DuckDB | 812 MB | Registry/event/task traffic deserves explicit retention and indexes |
| Execution DuckDB | 3.7 MB | Small today; correctness is more urgent than scale |
| Candidate tracker DuckDB | 4.7 MB | Small today |
| Targeted safety tests | 56 passed in 4.95 s | Fast enough for a required pre-merge lane |
| Frontend production build | 2.31 s | Build is fast; delivered main JS is large |
| Frontend main bundle | 1,518 kB minified / 423 kB gzip | Route-level splitting is warranted |

The live OHLCV store could not be profiled read-only because another Python process held a conflicting DuckDB lock. No lock was disturbed. That failure is itself operational evidence for AUD-003; it also means this review does not claim query-level timings against the live store.

## Confirmed hot paths

| Area | Evidence | Scaling effect | Priority |
|---|---|---|---|
| Ranking input | Separate catalog scans for latest market data, returns, volume, SMA/high, and delivery | Repeated I/O, repeated conversion, and inconsistent snapshots | P1 correctness, P2 speed |
| Feature store | `features/feature_store.py` is 2,410 lines and combines persistence, transformations, and reads | High memory pressure and difficult query tuning | P2 |
| Registry/control plane | Per-instance mutexes around separate DuckDB connections | No cross-instance or cross-process writer serialization | P1 |
| Publishing | Large dashboard builder plus fallback recomputation | Retry cost and hidden upstream work | P1/P2 |
| API read paths | Several routes fetch broad result sets and shape responses in Python | Latency and memory grow with history | P2 |
| pandas loops | 103 `iterrows` sites and 99 `pd.concat` sites repository-wide | Potential quadratic or row-wise work; each site needs profiling | P2/P3 |
| Browser app | Single 1.5 MB main JS chunk | Slower first load and parse on operator machines | P2 |

Counts identify review targets, not automatic defects. Vectorization should only replace row loops after semantic and timing tests.

## Database and query plan

### Immediate

1. Introduce one process-wide writer service per DuckDB file and an inter-process ownership protocol. API requests enqueue writes; readers use short-lived read-only connections where supported.
2. Make ranking load one point-in-time input snapshot per exchange/date. Persist or pass that snapshot through factor computation rather than rescanning the catalog.
3. Add bounded pagination and explicit column projection to API history/list endpoints.
4. Capture query duration, rows scanned/returned, and peak RSS per pipeline stage. Do not log SQL parameters containing secrets.
5. Define retention and compaction for task events, registry attempts, logs, and derived read models.

### Index and layout candidates to benchmark

DuckDB may choose scans despite indexes, so these are benchmark candidates rather than unconditional prescriptions:

- catalog lookup keys `(exchange, symbol, timestamp)` and date-partitioned Parquet pruning;
- registry retrieval keys `(run_id, stage_name, attempt_no, status)`;
- execution lookup keys `(correlation_id)`, `(client_order_id)`, and open-stop queries;
- task/event queries `(task_id, created_at)`;
- materialized latest-row or as-of views where profiling proves repeated `arg_max` cost.

Every candidate needs `EXPLAIN ANALYZE` on production-shaped temporary copies and a write-amplification measurement.

## Caching and materialization

Safe cache keys must include exchange, effective date, source snapshot identity, feature/rank contract version, and trust state. A date-only cache is unsafe because late corrections can change data for the same session. Cache immutable intermediate frames inside one run first; cross-run caching should be content-addressed and lineage-registered.

Useful materializations:

- one canonical adjusted OHLCV session snapshot;
- a point-in-time rank input frame containing price, returns, volume, delivery, and membership as of the same cutoff;
- bounded dashboard read models built by their owning upstream stages;
- compact API summaries separate from full artifacts.

Do not cache broker state, preview results, or mutable execution decisions without freshness and idempotency controls.

## Parallelism boundaries

Parallelize CPU-heavy, read-only symbol partitions only after one immutable input snapshot is established. Serialize writes by store and merge worker outputs deterministically. The seven feature substages are natural DAG nodes, but dependencies and artifact contracts must be explicit before concurrent execution. Publish delivery can be asynchronous after local artifact registration; fundamental recomputation does not belong in that retry path.

## Ten-times scale forecast

At roughly 10x symbols/history, the current design is expected to fail first through:

1. longer exclusive DuckDB lock windows and more API/pipeline contention;
2. repeated multi-gigabyte scans in ranking and feature stages;
3. DataFrame peak-memory multiplication during joins and concatenation;
4. unbounded registry/event/read-model growth;
5. API JSON serialization of broad histories;
6. longer single-run recovery because stage outputs are not uniformly atomic and independently reusable.

The recommended target remains a modular monolith with explicit store ownership and artifact contracts. A distributed service split is not justified until profiling shows an isolated workload whose independent scaling exceeds the coordination cost.

## Benchmark suite required

Create a generated-schema, non-market synthetic benchmark fixture only for computational performance tests; it must never be accepted by production trust gates or used as a market-data canary.

| Benchmark | Dataset shapes | Metrics | Acceptance intent |
|---|---|---|---|
| Point-in-time rank | 500/2,000/5,000 symbols × 1/5/10 years | wall time, RSS, scans, factor parity | near-linear symbol scaling; no future rows |
| Feature build | each substage independently | rows/s, RSS, artifact size | locate dominant transformations |
| Registry contention | 1/4/16 concurrent tasks | p50/p95 write latency, lock errors | zero lock errors; bounded p95 |
| API history | 1k/100k/1m records | p50/p95 latency, response bytes | pagination and stable memory |
| Execution batch | 10/100/1,000 candidates | time, duplicate submissions, policy parity | deterministic and idempotent |
| Frontend | cold route loads | transferred/parsed bytes, interaction time | main entry substantially below current size |

Run a real-data read-only canary only in an operator-approved maintenance window or on a copied snapshot. Never work around a live lock by terminating its owner.
