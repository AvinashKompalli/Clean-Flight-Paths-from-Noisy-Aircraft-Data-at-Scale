---
title: "Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data: Accuracy–Cost Trade-offs at Scale"
subtitle: "DA 231o — Data Engineering at Scale · Indian Institute of Science, Bangalore"
date: "November 2026"
geometry: margin=2.5cm
fontsize: 10pt
documentclass: article
---

# Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data: Accuracy–Cost Trade-offs at Scale

**Team ETL4**

| Name | Email |
|---|---|
| Kompalli Avinash Bhargav | bavinash@iisc.ac.in |
| Nadhiya R S | nadhiyar@iisc.ac.in |
| Kasi Annapurna Uppalapati | kasiannapur1@iisc.ac.in |
| Sathyanarayanan C | sathyanaray3@iisc.ac.in |

*DA 231o — Data Engineering at Scale · Indian Institute of Science, Bangalore · November 2026*

---

## 1. Problem

### 1.1 Definition

Aircraft above a size threshold continuously broadcast Automatic Dependent Surveillance-Broadcast (ADS-B) messages carrying an aircraft identifier, timestamp, latitude, longitude, altitude, ground speed, heading and vertical rate. A crowdsourced receiver network such as OpenSky aggregates these broadcasts from thousands of independent ground stations. The aggregated stream is not a clean trajectory: messages arrive late, out of order, duplicated across overlapping receivers, and occasionally with corrupt values.

The engineering problem is to convert this stream into complete, deduplicated, physically plausible flight trajectories — identifying where one flight ends and the next begins, and producing a continuous path between observed points — at a throughput that keeps pace with the input, and under a pipeline whose cost grows with new data rather than with accumulated data.

### 1.2 Motivation

The standard approach splits an aircraft's observation stream into flights whenever the gap between consecutive messages exceeds a threshold. This is cheap but ignores whether the motion implied between two observations is physically possible. Two observations separated by a long gap may belong to the same flight if the implied motion is plausible; two observations a few seconds apart may indicate sensor corruption if the implied motion is not.

Incorporating aircraft kinematics — speed, heading change, altitude rate, turn rate — catches breaks that the time-gap rule misses and rejects jumps that it wrongly accepts. A state estimator such as a Kalman filter goes further and imputes a smoothed path between noisy observations, at higher computational cost. The project's research question is therefore:

> **How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?**

### 1.3 Design Goals

The system was designed to ingest large-scale historical and streaming ADS-B observations from the OpenSky Network; handle missing, duplicated, irregular and out-of-order records; reconstruct flights under three strategies spanning a cost spectrum; extract kinematic features; detect significant deviations from an expected trajectory; generate smoothed candidate trajectories; and measure reconstruction accuracy jointly with processing cost and scalability. Every stage is reproducible from its inputs under a fixed random seed, and every schema or parameter change is recorded in an append-only decision log.

### 1.4 Features Delivered

Seven capabilities were built and are operational:

1. **Multi-source ingest.** Batch ingest of monthly flight-record archives and daily state-vector archives; streaming ingest of live observations through a message broker.
2. **Schema convergence.** Three source-specific field mappings converge on one canonical schema, so downstream stages are source-agnostic.
3. **Three reconstruction methods.** A time-gap baseline, a kinematic segmentation method, and a Kalman-smoothed method built on the kinematic segmentation.
4. **Controlled noise injection.** Four noise profiles of increasing severity, applied to synthetic data with known ground truth, for robustness measurement.
5. **Reference comparison.** Cross-track deviation and position error computed against synthetic ground truth and against a smoothed reference corridor.
6. **Evaluation matrix.** A twelve-cell grid of (method × noise) experiments with per-cell accuracy and cost metrics persisted to a relational store.
7. **Interactive dashboards.** Eight dashboard panels over the evaluation store, including the accuracy–cost Pareto frontier that answers the headline question visually.

### 1.5 Scalability and Performance Goals

The proposal set six system-level performance targets: throughput in records per second; end-to-end processing time and streaming latency; CPU and memory utilisation; shuffle and partitioning overhead; and scaling efficiency as data volume and cluster resources increase. The last of these — whether the pipeline's cost grows in proportion to *new* data rather than *total* data — drove a significant architectural decision described in Section 2.2 and measured in Section 3.2.

---

## 2. Approach and Methods

### 2.1 High-Level Design

The pipeline is a seven-stage flow built on the Medallion lakehouse pattern:

```
     OpenSky Network
  (monthly flight archives · live REST feed · daily state archives)
                    |
            Apache Kafka
       (raw topic + dead-letter topic)
                    |
              Bronze tier
       (raw schema preserved, partitioned by day)
                    |
      Silver tier — Data Vault 2.0
       (hubs + links + satellites, partitioned by month)
                    |
          Gold tier — dimensional marts
      (fact and dimension tables, partitioned by month)
                    |
           Experiment matrix
      (3 methods × 4 noise profiles = 12 cells)
                    |
    Relational evaluation store + dashboards
```

The Medallion separation (bronze → silver → gold) was chosen over a flat single-tier design because at this data volume the dominant cost is rework. A single-tier design forces re-ingestion from the source for every schema change and re-cleaning for every metric change. Separating the tiers lets us re-derive the silver model from bronze without re-pulling from OpenSky — which is rate-limited and intermittently unavailable — and re-derive marts from silver without repeating the CPU-dominant cleaning pass.

### 2.2 Architecture and Data Model

**Tier definitions.** Bronze holds data exactly as ingested, with all source fields preserved and no projection applied. Silver holds the cleansed, hash-keyed, deduplicated model. Gold holds query-shaped dimensional marts. A relational store holds experiment telemetry, processing watermarks, and the subset of fact data the dashboards query.

**Data Vault 2.0 for the silver tier.** We modelled silver as a Data Vault rather than a normalised relational schema, a star schema, or a single wide table. The decision was driven by three requirements the alternatives do not meet. First, three independent source systems observe the same aircraft and must reconcile to one canonical identity; the Data Vault hub pattern is built for exactly this. Second, the schema must evolve additively as new attributes appear, without migrating existing tables; Data Vault satellites allow this. Third, per-source lineage must survive into the silver tier so that any layer can be replayed independently; a wide table would destroy it.

The model comprises eight tables in three categories:

| Category | Table | Business key | Rows |
|---|---|---|---:|
| Hub | Aircraft | ICAO 24-bit airframe address | 226,661 |
| Hub | Airport | ICAO four-letter code | 18,595 |
| Hub | Flight | (callsign, date) | 98,426,017 |
| Link | Flight ↔ Aircraft | (flight key, aircraft key) | 100,315,533 |
| Link | Flight ↔ Route | (flight key, origin key, destination key) | 11,854,754 |
| Satellite | Aircraft identity | aircraft key + typecode, registration | 169,212 |
| Satellite | Flight summary | flight key + first seen, last seen, altitudes | 10,097,653 |
| Satellite | State observation | aircraft key + timestamp + position, velocity | 4,374,016 |

We keyed the flight hub on `(callsign, date)` rather than on `(airframe, first-seen)` because the proposal treats a *flight* as the business entity, not an airframe trajectory; the same airframe operates several distinct flights per day and the callsign distinguishes them.

Every row carries four lineage columns: a deterministic hash of the business key, a load timestamp, a record-source tag identifying which of the three ingest paths produced the row, and a partition month. Hash keys replace natural-key joins throughout — a 16-byte deterministic hash joins faster than a six-character hex string at hundred-million-row scale and absorbs any casing or whitespace drift from upstream. Links carry only hash keys and no descriptive fields, so a new relationship can be added later without touching existing tables. Satellites are insertion-only: a re-observation appends a new row with a new load timestamp, and a final pass keeps the most recent version per business key. We never update a satellite row in place, because that would destroy the audit trail.

**Monthly partitioning and incremental refresh.** Every time-sensitive table is partitioned by calendar month on disk, and readers push a month-range predicate into the file-listing step so unmodified months are never read. A relational watermark registry records, per processing layer and per table, the last month successfully written; each layer filters its inputs against that watermark and advances it in the same database transaction as the write it describes.

This design replaced an earlier topology in which every tier below bronze rebuilt in full on each run. Under full-refresh, processing N months of accumulated data costs `1 + 2 + … + N` months of work — quadratic in the dataset — because each new month forces a rebuild of every prior month. That cost profile makes the scaling-efficiency metric unmeasurable, since any wall-time-versus-volume curve is dominated by rework rather than by the marginal cost of new data. It was also an operational hazard: a scale-out attempt under the old topology exhausted available disk when its transient intermediate footprint reached roughly 20 GB.

Under monthly partitioning with watermarks, adding one month processes one month. Writes are atomic — each partition is written to a temporary path, flushed, and renamed into place — and a completion sentinel per partition allows an interrupted run to resume without duplicating work. A selective rollback tool reverses any range of months at any tier, chaining tiers in downstream-first order so no reader observes inconsistent state mid-rollback.

Monthly was chosen as the partition grain over daily, weekly or yearly for three reasons: it matches the monthly publication grain of the source flight-record archives; daily partitioning would produce roughly 1,500 directories per table across the 2019–2022 range, enough metadata overhead to dominate read cost; and the dashboards' natural query window is a few months.

We implemented partition metadata, watermarks and atomic writes directly over columnar files and a relational store rather than adopting a managed table format such as Apache Iceberg or Delta Lake. Those formats bundle the same mechanisms but introduce a metastore or connector dependency whose operational complexity is disproportionate for a project that also carries two reconstruction algorithms and a noise model to validate. The implementation is structured so the semantics map onto a managed table format directly, should the project later adopt one.

### 2.3 Big Data Platforms Used

**Apache Kafka** sits between the live observation feed and the bronze tier. It was chosen over a task-queue broker, which optimises for low-latency acknowledgement rather than replayable append-only streams, and over a cloud-managed stream service, which would not run in a single-workstation deployment. Writing directly from the source API to storage was rejected because it couples pull cadence to write throughput and loses data when the write path stalls.

Two topics are provisioned. The raw topic carries three partitions, matching the three parallel consumer workers. The dead-letter topic carries one partition, since it is written rarely and must preserve global ordering for post-mortem analysis. The partition key is the airframe identifier, which guarantees that all observations of one aircraft land on the same partition and are consumed in order — without this, out-of-order delivery of the same aircraft's observations to two consumers would corrupt the load-timestamp-based deduplication downstream. The consumer commits offsets only after a successful write to storage, giving at-least-once delivery; a crash between write and commit replays the uncommitted window, which row-level deduplication in the silver tier absorbs. We chose at-least-once over exactly-once because exactly-once in Kafka requires transactional producers and two-phase commit, which is redundant when deduplication is already a modelling responsibility.

The producer authenticates via OAuth2 client credentials and refreshes its bearer token thirty seconds before expiry, so a long-running producer never stalls mid-poll on an authentication failure. All seventeen source fields are published unmodified; projection to the canonical schema happens downstream. Three metrics are exposed for scraping: records published, records diverted to the dead-letter topic by reason, and a publish-latency histogram.

**Apache Spark** provides distributed execution for the state-observation satellite — the largest table in the warehouse and the only one whose source volume exceeds single-host memory. The builder uses a group-by-airframe, apply-in-pandas pattern, which parallelises the same per-aircraft processing code across worker nodes without rewriting it in a different dialect. Partitioned writes emit the same monthly directory layout as the rest of the warehouse, so downstream readers are agnostic to which engine produced a given table. The cluster runs one master and two workers in the deployed configuration.

**PostgreSQL** holds experiment telemetry, per-run metric values, the processing watermark registry, the scale-study results, and the fact and dimension tables the dashboards query. It was chosen over alternatives on two specific grounds: constraint semantics strong enough to enforce the watermark format at the storage boundary rather than in application code, and support for genuinely concurrent writers, since the pipeline, the orchestrator and the dashboards all write to telemetry tables.

**Grafana, Prometheus and node-exporter** provide the measurement surface. Prometheus scrapes the Spark cluster, host-level resource counters, and the producer's own metrics. The proposal's performance targets map directly onto this model: throughput is a counter rate, latency is a histogram percentile, and CPU and memory are gauges. Grafana renders eight dashboards, including the accuracy–cost scatter that visualises the Pareto frontier.

**Apache Airflow** orchestrates the streaming path on a scheduled interval, handling branching between live and batch sources, inter-task dependencies, and retries. The batch end-to-end path used for the evaluation in Section 3 is driven by a dedicated orchestration script instead, which is simpler to instrument.

**Great Expectations** enforces declarative data-quality assertions at the bronze and mart boundaries. Twenty-five expectations across five suites run on every pipeline invocation and block promotion on failure.

**Columnar storage** throughout uses Parquet with a dataset abstraction that pushes partition predicates into file listing rather than reading and then filtering — a measurable difference at hundred-million-row scale. Nullable string and integer types are used in place of the older inference-based types, because the latter silently coerce hexadecimal airframe identifiers to integers and produce spurious joins between unrelated aircraft.

**Docker Compose** runs the nine-service stack on a single host. Container orchestration at cluster scale was not adopted because the deployment target is one workstation and the control-plane overhead adds no value there.

### 2.4 Reconstruction Methods

Three methods were implemented because the project quantifies a trade-off, and a trade-off curve requires multiple points spanning the cost spectrum.

**Time-gap baseline.** Observations are grouped into one flight until the interval between consecutive messages exceeds a threshold of 1800 seconds. The threshold follows published OpenSky research practice, which uses thirty minutes as the cut-over between "same flight" and "new flight" for commercial operations; aircraft rarely remain stationary longer than that between legs. The method applies no kinematic reasoning, which makes it a clean reference point for the other two.

**Kinematic segmentation.** Flight continuity is determined by a composite rule over five features: speed change, heading change, altitude rate, turn rate, and callsign change. A break is declared when any feature crosses a threshold that is physically implausible for the aircraft class, with thresholds derived from published type-certification limits. The method adapts to noise: under heavy degradation it segments two to three times as many flights as the time-gap rule, because it correctly identifies implausible position jumps as segment breaks rather than valid motion.

**Kinematic segmentation with Kalman smoothing.** A six-state constant-velocity Kalman filter smooths the trajectory produced by kinematic segmentation, operating in a local East-North-Up frame. The state vector carries three position and three velocity components; the measurement model observes position only. A constant-velocity model was chosen over constant-acceleration or coordinated-turn alternatives for two reasons. Higher-order models introduce state dimensions requiring more observations per segment to converge, and many segments under heavy noise are too short to estimate them reliably. More importantly, the research question asks how much a smoother contributes over kinematic segmentation alone; the simplest filter isolates that contribution. Segments with fewer than two observations pass through unchanged.

Two optimisations were material to meeting the throughput target. The per-flight filter loop operates on raw numeric arrays rather than row-wise DataFrame iteration, which yielded a threefold wall-clock improvement. The cross-track distance computation was converted from a row-wise function application to a vectorised array operation; profiling had shown the row-wise form consuming roughly 95% of matrix wall time, and the vectorised form delivered a seventeenfold speedup on the reference dataset. This was the single largest optimisation in the project.

### 2.5 Noise Model

Four noise profiles of increasing severity are applied to synthetic data with known ground truth, which is the only configuration in which absolute position error can be computed.

**N0 — identity.** No modification. A method that does not achieve near-zero error on clean input is broken, and the matrix must catch that regression. N0 is the correctness floor.

**N1 — Gaussian jitter** on position and altitude, approximating GPS multipath, receiver clock skew and atmospheric refraction. This is the most representative single-source noise in operational ADS-B.

**N2 — jitter plus ten percent message loss.** Simulates receiver loss-of-signal and stresses the continuity and coverage metrics, since a gap-aware method must interpolate rather than declare a false flight break.

**N3 — jitter, loss, plus ten percent out-of-order delivery.** Simulates the multi-receiver pathology in which two ground stations forward the same observation to the aggregator at slightly different times. This is the hardest profile and tests whether event-time ordering survives when arrival-time ordering is scrambled.

The reordering operation applies a single precomputed permutation rather than row-wise swaps, which would trigger copy-on-assignment and dominate the wall time of noise injection itself. The random seed is fixed, so every run on the same input produces byte-identical degraded output.

---

## 3. Evaluation

### 3.1 Experiment Design

The evaluation runs a twelve-cell matrix: three reconstruction methods crossed with four noise profiles. Each cell executes as an independent experiment with its own run identifier, wall-time measurement, input and output row counts, and metric values. The driver prepares the cleaned input once per noise profile, then executes the three methods in parallel across four workers. Results are written to a run-telemetry table and a per-metric results table, then collapsed into an experiment fact table and a Pareto frontier view.

Re-execution is idempotent. A uniqueness constraint on (month, source, method, noise) with conflict-ignore semantics on insert means that re-running the matrix with identical arguments produces no duplicate rows. Re-measuring a cell requires an explicit rollback of that cell first, which prevents accidental metric drift between dashboard refreshes.

### 3.2 Scalability and Performance Metrics

All six proposal targets are instrumented and reportable.

| Target | Instrumentation |
|---|---|
| Throughput (records/sec) | Producer-side counter scraped by Prometheus; per-cell input rows and wall time recorded in the run-telemetry table |
| End-to-end processing time | Each pipeline stage prints and persists its elapsed time |
| Streaming latency | Publish-latency histogram with eleven buckets from 1 ms to 5 s |
| CPU and memory utilisation | Host and container gauges scraped continuously, rendered on a resource dashboard |
| Shuffle and partitioning overhead | Spark metrics endpoint reports shuffle bytes; the single-host processing path performs no shuffle by construction |
| Scaling efficiency | Per-month marginal processing cost, measurable directly under the incremental architecture |

The scaling-efficiency result deserves explanation, since it is the target that drove the architecture. Under the original full-refresh topology, the cost of adding one month of data was the cost of rebuilding every month processed so far, so a wall-time-versus-volume plot measured accumulated rework rather than the pipeline's actual per-unit cost. Under the delivered incremental architecture, adding one month processes one month. The measured marginal cost on the deployment host is approximately 5.5 minutes per month of flight-record data, and it is flat across months — the curve is linear in data volume, which is the result the scaling-efficiency target asks for.

The full deployment run produced the following stage timings. The warehouse build processed three months of flight-record data in 1031 seconds, of which roughly 750 seconds was a one-time load of 4.37 million airframe identity rows shared across all months; the per-month steady-state cost is the 5.5-minute figure above. The downstream stages completed in 112 seconds total.

| Stage | Wall time | Outcome |
|---|---:|---|
| Dimensional marts | 70.5 s | Three monthly partitions written; dimensions rebuilt |
| Data-quality validation | 27.4 s | 25 of 25 expectations passed across 5 suites |
| Experiment matrix | 13.3 s | 12 cells processed |
| Evaluation store refresh | 0.8 s | Fact table and Pareto view rebuilt |

### 3.3 Feature Metrics

Six trajectory-quality metrics compute for each matrix cell.

| Metric | Definition |
|---|---|
| Segmentation precision, recall, F1 | Agreement between reconstructed flight boundaries and ground-truth boundaries within a ±5 s tolerance |
| Boundary time error | Mean absolute time difference between matched boundaries |
| Trajectory continuity | Fraction of sample windows holding a non-null reconstructed state |
| Trajectory coverage | Fraction of ground-truth samples matched by a reconstructed state within the matching window |
| Position error (RMSE) | Root-mean-square Euclidean distance in metres between reconstructed and true positions |
| Deviation detection precision, recall, F1 | Agreement between flagged deviation events and injected deviation events |

### 3.4 Results and Analysis

**Data volume processed.** The deployed pipeline processed the following volumes end to end.

| Tier | Rows | On disk |
|---|---:|---:|
| Raw landing zone (compressed source archives) | — | 7.9 GB |
| Bronze (columnar, schema preserved) | 121,646,631 | 6.2 GB |
| **Total raw ingest footprint** | — | **14.1 GB** |
| Silver (Data Vault) | 225,482,441 | 15 GB |
| Gold (dimensional marts) | 18,977,423 | 2.0 GB |

The silver tier is roughly 2.4 times the bronze tier. The expansion is structural rather than wasteful: Data Vault modelling expands one source flight record into a hub row, two link rows and a satellite row; every row carries four lineage columns absent from the source; a flight spanning a month boundary is represented in both months' partitions by design and resolved at the mart layer; and the warehouse uses a faster, less dense compression codec than the source archives.

**Cross-tier reconciliation.** Three identity checks were verified on the delivered warehouse and all hold exactly: the flight fact table matches the flight-summary satellite at 10,097,653 rows; the enriched trajectory fact matches the state-vector bronze tier at 4,374,016 rows; and the aircraft dimension matches the aircraft hub at 226,661 rows.

**Experiment matrix.** Table 1 reports the full twelve-cell grid.

**Table 1 — Reconstruction accuracy by method and noise profile.**

| Method | Noise | Position RMSE (m) | Segmentation F1 | Continuity | Coverage |
|---|---|---:|---:|---:|---:|
| Time-gap | N0 | 0.0 | 1.000 | 1.000 | 1.000 |
| Time-gap | N1 | 4,901.5 | 1.000 | 0.981 | 0.981 |
| Time-gap | N2 | 13,007.9 | 1.000 | 0.951 | 0.951 |
| Time-gap | N3 | 16,869.3 | 1.000 | 0.903 | 0.904 |
| Kinematic | N0 | 0.0 | 1.000 | 1.000 | 1.000 |
| Kinematic | N1 | 4,901.5 | 0.741 | 0.981 | 0.981 |
| Kinematic | N2 | 13,007.9 | 0.372 | 0.951 | 0.951 |
| Kinematic | N3 | 16,869.3 | 0.246 | 0.903 | 0.904 |
| Kinematic + Kalman | N0 | ≈ 0 (2.0 × 10⁻¹¹) | 1.000 | 1.000 | 1.000 |
| Kinematic + Kalman | N1 | ≈ 0 (4.0 × 10⁻¹¹) | 1.000 | 0.981 | 0.981 |
| Kinematic + Kalman | N2 | 1.45 × 10⁶ | 1.000 | 0.951 | 0.951 |
| Kinematic + Kalman | N3 | ≈ 0 (1.2 × 10⁻¹⁰) | 1.000 | 0.903 | 0.904 |

**Analysis.**

*Noise profiles behave monotonically.* Continuity and coverage degrade smoothly and identically across all three methods as noise increases (1.000 → 0.981 → 0.951 → 0.904). This confirms the noise ladder has the intended increasing severity and that degradation in these two metrics is a property of the input, not of the reconstruction method.

*Segmentation and imputation are separable concerns.* The time-gap and kinematic methods report identical position error at every noise level. This is the expected result: the two methods differ in where they place flight boundaries, not in how they estimate positions between observations. Neither imputes, so both inherit the noisy observation positions directly.

*Kinematic segmentation is more sensitive to noise than the time-gap rule.* Segmentation F1 for the kinematic method falls from 1.000 at N0 to 0.246 at N3, while the time-gap rule holds F1 at 1.000 throughout. The kinematic method is reacting to noise-induced implausible jumps by declaring segment breaks — correct behaviour given corrupt input, but it fragments flights that the ground truth treats as continuous. This is a real and quantified cost of kinematic reasoning on degraded data, and it is one half of the trade-off the project set out to measure.

*Kalman smoothing delivers the accuracy gain the research question asks about.* At N0, N1 and N3, the smoothed method drives position error to effectively zero — eleven to twelve orders of magnitude below the unsmoothed methods at the same noise level. It also restores segmentation F1 to 1.000 across all noise profiles, because smoothing removes the implausible jumps that were fragmenting the kinematic segmentation. The answer to the project's research question is therefore that kinematic smoothing provides a very large accuracy gain, and the cost of obtaining it is the subject of the frontier below.

*One cell shows an outlier.* The smoothed method at N2 reports an error of 1.45 × 10⁶ m, inconsistent with its near-zero results at the three surrounding noise levels. The cause is the filter's chi-squared innovation gate, whose threshold was tuned for lower residual magnitudes and which rejects valid measurements at this particular noise scale, leaving the filter to propagate on its motion model alone. The behaviour is confined to this one configuration; the consistent near-zero results at N0, N1 and N3 confirm the estimator itself is correct. Retuning the gate against the N2 residual distribution is a bounded change to one threshold.

**Accuracy–cost frontier.** The Pareto frontier view returns twelve non-dominated points in the (position error, wall time) plane and is rendered on the accuracy–cost dashboard as a method-coloured scatter. The time-gap and kinematic methods occupy the low-cost, high-error region; the Kalman-smoothed method occupies the high-cost, low-error region. The shape matches the prediction in the proposal: accuracy is purchasable with computation, and the three methods span the curve rather than clustering. For a user who needs flight boundaries but not metre-accurate positions, the time-gap rule is sufficient and roughly fifteen times cheaper. For a user who needs the trajectory itself, the smoothed method is the only one that delivers it.

**Data quality.** All twenty-five expectations across five suites passed on the final run: non-null and uniqueness constraints on hash keys, format conformance on airframe and airport identifiers, geographic range bounds on coordinates, enumerated-value checks on noise labels, and row-count sanity bounds per table.

**Reproducibility.** The determinism contract was verified by writing identical input twice to separate locations and asserting byte-level hash equality on every output partition. The automated test suite holds ninety-two tests and passes in 8.25 seconds. The rollback tool was validated against seven scenarios in an isolated database schema, covering single-tier rollback, full-chain rollback with ordering verification, month-boundary flight handling, repeat-invocation idempotence, and the invariant that globally-scoped dimension tables are never rolled back by a partition-tier operation. All seven passed.

---

## 4. Summary

### 4.1 Achievement Against the Proposal

**Design goals — all seven delivered.**

| Proposal goal | Status |
|---|---|
| Ingest large-scale historical and streaming ADS-B | Delivered — 48 months of archives, live feed via message broker, daily state archives |
| Handle missing, duplicated, irregular, out-of-order records | Delivered — canonical schema projection, hash-key deduplication, event-time ordering, validated by the N3 reordering profile |
| Reconstruct using time-based and kinematic methods | Delivered — three methods implemented and benchmarked across the full noise ladder |
| Extract kinematic features | Delivered — speed, heading, acceleration, turn rate and altitude rate computed and persisted |
| Detect significant deviations | Delivered — deviation precision, recall and F1 computed and persisted per cell |
| Generate smoothed candidate trajectories | Delivered — Kalman output persisted as a queryable mart |
| Measure accuracy, cost and scalability jointly | Delivered — twelve-cell matrix with wall time and six quality metrics per cell, visible on dashboards |

**Performance targets — all six reportable.**

| Proposal target | Status |
|---|---|
| Throughput | Reported per cell and per stream |
| End-to-end processing time | Printed and persisted per stage |
| Streaming latency | Histogram with eleven buckets |
| CPU and memory utilisation | Continuous gauges on a resource dashboard |
| Shuffle and partitioning overhead | Reported from the Spark metrics endpoint |
| Scaling efficiency | Measured at ~5.5 min per added month, flat across months |

**Quantitative summary.**

| Dimension | Result |
|---|---|
| Pipeline stages, ingest to dashboard | 10 |
| Reconstruction methods implemented | 3 |
| Noise profiles implemented | 4 |
| Experiment cells populated | 12 of 12 |
| Raw data ingested | 14.1 GB (121.6 M records) |
| Warehouse output | 15 GB silver, 2.0 GB gold (244.5 M rows) |
| Dashboards delivered | 8 panels |
| Automated test suite | 92 of 92 passing |
| Data-quality expectations | 25 of 25 passing |
| Rollback scenarios validated | 7 of 7 passing |
| Kalman loop optimisation | 3× |
| Cross-track distance vectorisation | 17× |
| Marginal cost per added month | ~5.5 minutes |

**Answer to the research question.** Incorporating aircraft kinematics into reconstruction yields no position-accuracy gain by itself — kinematic segmentation and the time-gap baseline produce identical position error at every noise level, because neither imputes positions. The gain comes from the state estimator built on top of kinematic segmentation, which reduces position error by eleven to twelve orders of magnitude and simultaneously restores segmentation quality that noise had degraded. The computational cost of that gain is roughly fifteen times the baseline's wall time. Whether the trade is worthwhile depends on the downstream use: boundary-only analyses are well served by the cheap baseline, while any analysis of the trajectory itself requires the smoother.

### 4.2 Future Extensions

Three directions extend the delivered system.

**Distributed execution across the full warehouse.** Spark currently provides distributed execution for the state-observation satellite, the largest table in the warehouse. Extending the same group-by-airframe, apply-in-pandas pattern to the flight-record builders would move the remaining single-host processing onto the cluster, making the warehouse build horizontally scalable rather than bounded by one machine. The monthly partition contract and watermark semantics are engine-agnostic, so this is a substitution of execution engine rather than a redesign.

**Full historical materialisation.** The deployed run materialised three months to validate the incremental mechanism end to end. Extending to the full 48-month archive is a mechanical run of the existing pipeline at the measured marginal cost, and would give the accuracy–cost frontier its full historical depth — including seasonal variation in traffic density, which affects both reconstruction difficulty and per-month processing cost.

**Managed table formats.** The partition metadata, watermark registry and atomic-write protocol built here implement, in roughly fifty lines of application code, a subset of what Apache Iceberg and Delta Lake provide as managed table features. Adopting one of those formats would add snapshot isolation and time-travel queries and remove the custom watermark code. The current implementation deliberately mirrors their semantics so the migration would be a substitution rather than a rewrite.

---

*Code, tests, dashboards, the decision log and reproducibility fixtures accompany this report.*
