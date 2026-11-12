# Critical review: this project as a DE-at-scale deliverable

*Written in the voice of a DE-at-scale professor sitting at the viva table,
grading against what the course actually asks for and what industry would
expect of a mid-career DE. Written with the current branch
`feature/pipeline-2-avinash` open and the three recent improvements
(Kafka -> bronze, Spark path, Great Expectations) committed.*

---

## 1. What a DE-at-scale project is expected to demonstrate

A graded DE-at-scale project is not a "can you write pandas code?" exercise.
It is a demonstration that the student understands **the full life-cycle of a
batch + streaming analytics system**, with explicit reasoning about:

1. **Volume** — the pipeline must ingest something that is uncomfortable for
   a single process. "A few hundred MB" does not count. Grader's rule of
   thumb: bronze on disk >= 10 GB, raw input >= 50-100 GB uncompressed.
2. **Velocity** — a streaming path that actually lands data, not just pushes
   to a broker. If the write-side is wired and the read-side is a `TODO`,
   that is a half-built system.
3. **Variety + schema contract** — multiple sources (batch + stream +
   reference), one canonical downstream schema, projection explicit at the
   boundary, with a schema registry or at least typed DDL in Git.
4. **Distributed execution** — at least one stage that runs on a cluster
   engine (Spark / Flink / Beam). Standing up a Spark container that no
   code touches is cosmetic, not distributed.
5. **Dimensional / vault modeling** — business keys, surrogate keys,
   history, slowly-changing attributes. Not just "flat parquet + a BI tool".
6. **Orchestration** — Airflow / Dagster / Prefect DAGs with declared
   dependencies, retries, SLAs, catchup semantics, idempotent tasks.
7. **Observability** — producer metrics, consumer lag, task duration,
   throughput, freshness, each surfaced on dashboards somebody could use
   on-call. Not just "we have Grafana running".
8. **Data quality** — expectations encoded, validated per run, failures
   block downstream. A `TODO great_expectations` is a shortcut.
9. **Reproducibility + hygiene** — pinned dependencies, idempotent runs,
   secrets out of Git, CI green, a single command that reproduces the
   showcase.
10. **A credible performance story** — numbers for throughput, latency,
    scale-out efficiency, resource utilisation. Not just "it runs on my
    laptop".

Graded on a 100-point rubric that is typical for the course, the above
roughly weight: volume 15, velocity 15, schema/modeling 15, distributed 15,
orchestration 10, observability 10, data quality 10, performance story 5,
hygiene 5.

---

## 2. Where this project is strong

| Dimension | Status | Why it's strong |
| --- | --- | --- |
| **Medallion layout** | Solid | `data/bronze/` → `data/silver/vault/` → `data/gold/marts/` with explicit `date=/hour=` partitioning and typed conversion at the bronze seam (`DTYPES` in `screpper.py`, `bronze_flightlists.py`). |
| **Data Vault 2.0 raw vault** | Solid | Hubs/links/sats with `load_dt`, business-key canonicalisation, deterministic MD5 hash keys (`etl4/vault/schema.py`). This is what a vault purist would want to see. |
| **Postgres sink via COPY FROM STDIN** | Solid | Correct for scale; no row-at-a-time inserts. `fact_trajectory_enriched` lands at 4.4 M rows in ~2.7 min. |
| **Orchestration (post-fix)** | Solid | Four Airflow DAGs, each with a single responsibility (zenodo monthly, live stream drain, daily incremental, nightly vault rebuild). Chained explicitly; no fan-out hacks. |
| **Streaming end-to-end (now)** | Solid | `live_producer` -> `adsb.raw` Kafka topic -> `kafka_bronze_consumer` -> bronze parquet. Offsets commit after flush (at-least-once). Flushes on batch-size, idle timeout, or SIGTERM. |
| **Observability surface** | Reasonable | Prometheus counters/histograms on the producer, node-exporter host metrics, five Grafana dashboards provisioned at container start, `ingest_runs` + `pipeline_runs` + `eval_results` + `fact_experiment_result` tables for pipeline telemetry. |
| **CI + test suite** | Reasonable | 20 invariants pinned in `etl4/tests/test_pipeline.py`; Python 3.14 local + 3.11 CI; `.gitattributes` patched after the airports.csv LFS pointer bit CI. |
| **Hygiene** | Reasonable | `.env` gitignored; pinned `requirements.txt`; no secrets in history; `CLAUDE.md` untracked; data directories gitignored; no Claude attribution on commits. |

---

## 3. Lagging tails (where points are still being left on the table)

### 3.1 Volume is below the DE-at-scale bar (-5 to -8)

Bronze is **~0.45 GB today** (154 MB raw_states + 304 MB flightlists). The
DE-at-scale expectation is tenfold higher. The daily_batch incremental path
accretes ~12 k state vectors per poll (~650 KB/day) — fine as a
demonstration of incremental loading, but it does not move the volume
needle. The 10 GB scale-up (Zenodo 2019-2023 flightlists, ~8.7 GB
compressed) is **in flight today** and should be reported alongside the
viva deliverables, with throughput numbers for vault/marts/sink at that
scale.

### 3.2 Spark is still 95% decorative (-3 to -5)

`apache/spark:3.5.3` master + two workers run in Docker Compose. Before
today, nothing used them. After today, `raw_vault.py --engine spark`
routes `sat_state_observation` through `groupBy(icao24).applyInPandas`.
That is **one stage out of ten**, and the UDF is still `applyInPandas`
rather than native Spark SQL / Spark DataFrames, so it does not stress
Catalyst or Tungsten. A real DE-at-scale answer:

- Promote `bench/run_matrix.py` (the 12-cell noise × method matrix) to
  Spark `foreachBatch` or `applyInPandas` so all 12 cells run in parallel
  on the cluster instead of sequentially on one laptop CPU.
- Promote the `postgres_sink` `COPY` to a `spark.write.jdbc` with
  partition hints so the write parallelises across executors.
- Turn the Kafka consumer into a Spark Structured Streaming sink with
  a checkpoint directory. Our `confluent-kafka` consumer is fine for
  the showcase, but Spark Structured Streaming is what graders want
  to see for a "kafka -> lake" claim because it owns exactly-once with
  `trigger(availableNow=True)` out of the box.

### 3.3 Data quality is thin (-2 to -3)

Five expectations per table across five tables is the floor of what
Great Expectations rewards, not the ceiling. Missing today:

- **Freshness / recency expectations.** Nothing fails when the daily_batch
  partition is older than 24 h. A DE-at-scale professor will ask where
  the `data_quality_runs` row with `last_partition_lag_minutes > 90`
  goes; the answer today is "nowhere".
- **Referential integrity across vault <-> marts.** No expectation asserts
  that every `fact_trajectory_enriched.hk_aircraft` appears in
  `dim_aircraft`. Join-fanout bugs would slip through.
- **Column-distribution drift.** No expectation on the ratio of
  `baroaltitude` non-null, or the share of rows with `onground=true`, or
  the icao24 cardinality per day. A grader looking for anomaly detection
  will not find it.
- **Per-run quarantine.** On a failure today the DAG just errors. There is
  no dead-letter for the offending rows. The producer has a DLQ topic
  (`adsb.dlq`) but nothing on the consumer side mirrors it.

### 3.4 The matrix is embarrassingly parallel but runs sequentially (-2)

`bench/run_matrix.py` runs 3 methods × 4 noise profiles = 12 cells. They
have no cross-cell state. Wall-time on a laptop is ~90 min. This should be
trivial fan-out on Spark (`foreachBatch`) or even `multiprocessing.Pool`
with `maxtasksperchild=1`. It is not. The code waits for each cell
serially.

### 3.5 Lineage / column-level traceability is absent (-2)

OpenLineage, Marquez, or dbt-style column lineage would let a reviewer see
how `fact_trajectory_enriched.typecode` traces back to `flightlist.typecode`
via `sat_aircraft_identity` and `dim_aircraft`. Today lineage lives in
ADRs and the author's head. For DE-at-scale, that is a gap.

### 3.6 No cost / scale-out story (-2)

There is no benchmark showing "30 M rows takes 2× the wall time 10 M rows
takes". There is no single-node-vs-cluster comparison. There is no profile
showing CPU-vs-IO-vs-Postgres-write breakdown. Without that, the
"at-scale" claim is unsupported.

### 3.7 No schema registry / contract versioning (-1)

Kafka records are JSON with implicit shape. If the producer changes a
field name tomorrow, every consumer breaks silently. For a Kafka-fronted
system, a professor will ask for Schema Registry (Avro) or, failing that,
JSON Schema validated at the broker boundary. Neither exists.

---

## 4. Shortcuts still in the tree

| # | Shortcut | Where | Risk |
| --- | --- | --- | --- |
| 1 | `applyInPandas` over proper Spark DataFrame / Dataset | `etl4/vault/raw_vault_spark.py` | Catalyst can't optimise; the whole group is deserialised to pandas. Works today; does not scale beyond 50 M rows per icao24. |
| 2 | Postgres sink uses `DROP TABLE ... CREATE TABLE ... COPY` instead of upsert | `etl4/vault/postgres_sink.py` | A failed sink leaves Postgres empty mid-flight. Should be `COPY to staging, BEGIN, swap, COMMIT`. |
| 3 | Great Expectations library import is best-effort; falls back to a bespoke pandas evaluator when GE isn't installed | `etl4/qa/validate.py` | Defensible (GE wheels for 3.14 not out), but at viva the professor will point out that "the suite ran against the fallback evaluator, not GE". Pin to a Python version that does ship GE wheels. |
| 4 | `pyopensky` lives in `requirements.txt` but is only imported by dormant Trino code | `etl4/requirements.txt`, `etl4/ingest/ingest_opensky_day.py` | Dead dependency. Silently makes the environment harder to reproduce. |
| 5 | Dashboard 03 reads `fact_experiment_result`; nothing enforces the "latest run per (noise, method)" invariant at write-time — it's done in a `DISTINCT ON` view | `etl4/run_e2e.py stage_eval_mart` | A second concurrent writer could violate. Not a problem today; a landmine tomorrow. |
| 6 | Kafka messages are JSON; no Schema Registry, no Avro | `etl4/ingest/live_producer.py` | See §3.7. |
| 7 | `confluent-kafka` consumer flushes offsets after every batch; parquet is append-per-flush, not compacted | `etl4/ingest/kafka_bronze_consumer.py` | Many small files. A nightly compaction step (`spark.read.parquet(...).repartition(N).write.parquet`) is missing. |
| 8 | `matrix.csv` is the only crash-safety mechanism; no checkpoint for the 90-min matrix | `etl4/bench/run_matrix.py` | A crash at cell 11/12 restarts from scratch unless you parse `matrix.csv` by hand. |
| 9 | Local Docker Spark cluster is fed from a single-driver `pyspark.sql.SparkSession.builder.master(local[*])` in the default code path | `etl4/vault/raw_vault_spark.py` | The cluster's two workers are unused unless `SPARK_MASTER=spark://localhost:7077` is set. Document the env var or default to the cluster. |
| 10 | Airflow BashOperators use `cd /opt/project && python -m ...` | every DAG | `BashOperator` hides exceptions inside task logs; a `PythonOperator` with proper `on_failure_callback` is what a graded answer looks like. |
| 11 | Tests do not run GE suites | `etl4/tests/test_pipeline.py` | 20 invariants cover algorithms; zero cover the data-quality contract. |
| 12 | `data/bronze/flightlists/v23.00/` is a single-version folder. No way to compare `v23.00` vs `v24.00` side by side; the new version just overwrites paths | `etl4/vault/bronze_flightlists.py` | Immutable bronze is the whole point; this detail is fine but a professor will ask. |

---

## 5. If this project were graded today

Expected rubric (100 points):

| Dimension | Weight | Score |
| --- | --- | --- |
| Volume | 15 | 7  (scale-up in flight; previously 4) |
| Velocity | 15 | 11 (Kafka -> bronze now wired; previously 5) |
| Variety / schema contract | 15 | 10 |
| Distributed execution | 15 | 7  (one stage on Spark; previously 1) |
| Modeling (Vault + marts) | 10 | 10 |
| Orchestration | 10 | 8 |
| Observability | 10 | 7 |
| Data quality | 10 | 7  (GE suite shipped; previously 2) |
| Performance story | 5  | 2 |
| Hygiene | 5  | 5 |

**Total: 74 / 100 today. Pre-improvements this was ~61.**

For an A grade (>= 85), the credible lifts are:
1. Complete the 10 GB scale-up and publish throughput numbers (+5).
2. Push the matrix fan-out to Spark (+4).
3. Add freshness + referential + distribution-drift expectations (+3).
4. Write up a scale-out comparison with at least two data points (+3).

---

## 6. Final verdict for the viva

The project passes the "serious DE-at-scale coursework" bar. It is above
the median of what gets submitted. It is not yet an exemplar.

The three improvements committed today (Kafka -> bronze, Spark path, GE
suite) close the three largest *architectural* gaps — the system is no
longer a half-built claim about streaming, distribution, and quality.
What remains is primarily **volume and performance evidence**, both of
which are mechanical rather than design work.

A credible defence at the viva:

> "The design covers the full life-cycle. The Kafka path lands in the
> lake, the quality suite runs on every daily load, the state-observation
> satellite build is distributed when the cluster is available, and the
> scale-up is in flight. I know where my gaps are: matrix fan-out on
> Spark, freshness expectations, and a published scale-out curve."

That is what a senior data engineer sounds like. Say that.
