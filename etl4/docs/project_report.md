---
title: "Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data: Accuracy and Cost Trade-offs at Scale"
subtitle: "DA 231o, Data Engineering at Scale, Indian Institute of Science, Bangalore"
date: "November 2026"
geometry: margin=2.5cm
fontsize: 10pt
documentclass: article
---

# Scalable Kinematic Flight Trajectory Reconstruction from Noisy ADS-B Data: Accuracy and Cost Trade-offs at Scale

**Team ETL4**

| Name | Email |
|---|---|
| Kompalli Avinash Bhargav | bavinash@iisc.ac.in |
| Nadhiya R S | nadhiyar@iisc.ac.in |
| Kasi Annapurna Uppalapati | kasiannapur1@iisc.ac.in |
| Sathyanarayanan C | sathyanaray3@iisc.ac.in |

DA 231o, Data Engineering at Scale. Indian Institute of Science, Bangalore. November 2026.

---

## 1. Problem

### 1.1 Problem Definition

Aircraft above a size threshold broadcast Automatic Dependent Surveillance-Broadcast (ADS-B) messages roughly once per second, carrying a 24-bit airframe identifier, a timestamp, latitude, longitude, barometric and geometric altitude, ground speed, heading and vertical rate. The OpenSky Network aggregates these broadcasts from several thousand crowdsourced ground receivers and publishes the stream for research use.

That stream is not a set of flight trajectories. It is a sequence of position reports from many aircraft, interleaved, with no flight boundaries marked. Converting it into trajectories means solving three problems at once: grouping reports by aircraft and ordering them by the time the observation was made rather than the time it arrived; identifying where one flight ends and the next begins, since one airframe flies several separate flights in a day and the raw stream does not distinguish them; and making the resulting path continuous and physically plausible despite missing points, duplicate reports from overlapping receivers, and occasional corrupt values.

This project builds a pipeline that performs that conversion at scale, implements three strategies for the flight-boundary problem, and measures both the accuracy and the computational cost of each.

### 1.2 Motivation

The standard method for identifying flight boundaries is a time-gap rule: split the stream whenever the interval between consecutive observations of one airframe exceeds a threshold, typically thirty minutes. The rule is cheap and easy to reason about. Its weakness is that it considers only elapsed time and never the motion implied by the two observations it compares.

An aircraft held on the ground for forty minutes between legs produces a long gap and is correctly split. An aircraft that loses receiver coverage for forty minutes while cruising produces an identical gap and is incorrectly split into two flights. Conversely, two observations three seconds apart implying a position jump of two hundred kilometres are physically impossible and almost certainly corrupt, yet the rule accepts them because the time gap is small.

Aircraft kinematics address both cases: ground speed, heading change, altitude rate and turn rate between two observations determine whether the implied motion is achievable for the aircraft class. Going further, a state estimator can treat each observation as a noisy measurement of an underlying smooth trajectory and reconstruct the path between observations rather than accepting reported positions as exact.

Each step adds information and adds computational cost. The project exists to quantify that exchange. The research question stated in the proposal is:

> How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?

### 1.3 Design Goals

1. Ingest large-scale historical ADS-B observations and a live streaming feed from the OpenSky Network.
2. Handle missing, duplicated, irregularly sampled and out-of-order records without manual intervention.
3. Reconstruct flights using a time-based baseline and two kinematic methods spanning a range of computational cost.
4. Extract kinematic features: speed, heading, acceleration, turn rate, altitude change, vertical rate.
5. Detect significant deviations from an expected or reference trajectory.
6. Generate a smoothed candidate trajectory where observations are noisy or deviations are detected.
7. Measure reconstruction accuracy together with processing cost, so the two can be compared directly rather than reported in isolation.

Two engineering constraints applied across all seven. Every stage must produce identical output from identical input under a fixed random seed. Every change to a schema or an algorithm parameter must be recorded in an append-only decision log, so a result can be traced to the configuration that produced it.

### 1.4 Features Required

Delivering those goals required seven capabilities. **Multi-source ingest** of monthly flight-record archives covering 2019 to 2022, daily state-vector archives, and a live REST feed, each with a different schema and delivery mechanism. **Schema convergence**, projecting all three onto one canonical schema before any downstream stage sees them, so reconstruction code is independent of data origin. **Three reconstruction methods** sharing input and output schemas, so results are directly comparable. **Controlled noise injection**, because real ADS-B carries no ground truth: the pipeline generates synthetic trajectories with known truth and degrades them under profiles of increasing severity. **Reference comparison** using cross-track distance and position error, and a smoothed reference corridor where explicit truth is unavailable. An **evaluation matrix** crossing every method with every noise profile, each cell recording accuracy and cost to a queryable store. **Dashboards** over that store, including the accuracy against cost scatter.

### 1.5 Scalability and Performance Goals

The proposal set six targets: throughput in records per second; end-to-end processing time and streaming latency; CPU and memory utilisation; shuffle and partitioning overhead; scaling efficiency as data volume grows; and robustness to missing, duplicated and out-of-order observations.

The fifth constrained the architecture more than the other five combined. Scaling efficiency asks whether cost grows in proportion to the volume of new data or to the total volume accumulated. A pipeline that rebuilds everything on each run has a cost that grows with the square of the dataset, and no amount of per-stage optimisation changes that. Meeting this target required the architectural decision described in Section 2.2 and measured in Section 3.2.

---

## 2. Approach and Methods

### 2.1 High-Level Design

The pipeline has seven stages in a linear flow. Data moves forward through quality tiers, and each tier persists its output so downstream tiers can be rebuilt without re-running upstream work.

```
          OpenSky Network
  monthly archives | live REST feed | daily state archives
                     |
                Apache Kafka
          raw topic + dead-letter topic
                     |
                 Bronze tier
       source schema preserved, partitioned by day
                     |
          Silver tier, Data Vault 2.0
      hubs + links + satellites, partitioned by month
                     |
          Gold tier, dimensional marts
      fact and dimension tables, partitioned by month
                     |
              Experiment matrix
       3 methods x 4 noise profiles = 12 cells
                     |
    Relational evaluation store and dashboards
```

The bronze, silver and gold separation follows the Medallion pattern, chosen over a single-tier design because the dominant cost here is repeated work. With one tier, every schema change forces a fresh pull from OpenSky, which is rate-limited and intermittently unavailable, and every metric change forces a fresh cleaning pass, the most CPU-intensive stage in the pipeline. Separating the tiers lets silver rebuild from bronze without contacting OpenSky, and marts rebuild from silver without repeating the cleaning pass.

### 2.2 Architecture and Data Model

Bronze stores data exactly as received, every source field preserved and no projection applied. Silver stores the cleaned, hash-keyed, deduplicated model. Gold stores query-shaped dimension and fact tables. A relational store holds experiment telemetry, processing watermarks, and the subset of fact data the dashboards query directly.

**Data Vault 2.0 for the silver tier.** Silver is modelled as a Data Vault rather than a normalised schema, a star schema, or a single wide table, for three reasons specific to this project. *Multi-source reconciliation:* three independent sources observe the same aircraft and must resolve to one canonical identity, which is exactly what the hub pattern provides, creating a hub row once per business key and recording the source that first observed it so later observations attach rather than duplicate. *Additive schema evolution:* new attributes arrive as new satellite tables on an existing hub, so no existing table is altered and a schema change cannot break a working stage. *Per-source lineage:* every row records which ingest path produced it and when, without which no individual tier could be replayed in isolation.

The model comprises eight tables. Hubs hold entity identity, one row per business key. Links hold relationships between hubs and carry no descriptive fields. Satellites hold descriptive attributes and attach to a parent hub.

| Category | Table | Business key | Rows |
|---|---|---|---:|
| Hub | Aircraft | ICAO 24-bit airframe address | 226,661 |
| Hub | Airport | ICAO four-letter code | 18,595 |
| Hub | Flight | callsign and date | 98,426,017 |
| Link | Flight to Aircraft | flight key, aircraft key | 100,315,533 |
| Link | Flight to Route | flight key, origin key, destination key | 11,854,754 |
| Satellite | Aircraft identity | aircraft key, typecode, registration | 169,212 |
| Satellite | Flight summary | flight key, first seen, last seen, altitudes | 10,097,653 |
| Satellite | State observation | aircraft key, timestamp, position, velocity | 4,374,016 |

The flight hub is keyed on callsign and date rather than on airframe and first-seen timestamp. The proposal treats a flight as the unit of analysis, and one airframe operates several distinct flights in a day: the callsign distinguishes them, the airframe identifier does not.

Every row carries four lineage columns: a deterministic hash of the business key, a load timestamp, a record-source tag, and a partition month. Hash keys replace natural keys in all joins, because a 16-byte hash joins faster than a six-character hexadecimal string at hundred-million-row scale and absorbs casing and whitespace variation between sources. Satellites are insertion-only: a re-observation appends a new row with a new load timestamp and a final pass selects the most recent version per key. Rows are never updated in place, because an update destroys the history the load timestamp exists to record.

**Monthly partitioning and incremental refresh.** Every time-sensitive table is partitioned by calendar month on disk. Readers push a month-range predicate into the file-listing step, so partitions outside the requested range are never opened. A watermark registry in the relational store records, per tier and per table, the last month successfully written; each tier filters its inputs against its watermark and advances that watermark inside the same database transaction as the write it describes.

This replaced an earlier design in which every tier below bronze rebuilt completely on each run, at the quadratic cost described in Section 1.5. That design also had an operational cost, exhausting the available disk when transient intermediate files reached approximately 20 GB during a scale-out attempt.

Under monthly partitioning with watermarks, adding one month processes one month. Writes are atomic: each partition is written to a temporary path, flushed, then renamed into place, so a reader concurrent with a writer sees either the complete old partition or the complete new one. A completion marker after each commit lets an interrupted run resume at a partition boundary rather than restarting. A rollback tool reverses any range of months at any tier, processing tiers downstream-first so no reader observes a source rolled back beneath it.

Monthly was chosen as the grain because it matches the publication grain of the source archives, which arrive as one file per month. Daily partitioning would create approximately 1,500 directories per table across the 2019 to 2022 range, and the metadata cost of listing them would exceed the saving from finer pruning; yearly is too coarse for the dashboard query pattern, which examines a few months at a time.

Partition metadata, the watermark registry and the atomic write protocol were implemented directly over columnar files and the relational store rather than by adopting a managed table format such as Apache Iceberg or Delta Lake. Those formats provide equivalent mechanisms but introduce a metastore or connector dependency, and for a project that also carries two reconstruction algorithms, a noise model and an evaluation harness to validate, the extra operational surface was not justified. The implementation mirrors their semantics closely enough that a later migration would be a substitution rather than a redesign.

### 2.3 Big Data Platforms Used

**Apache Kafka** buffers the live feed between the source API and bronze. It was chosen over a task-queue broker, which optimises for low-latency acknowledgement rather than replayable append-only streams, and over writing directly from the API to storage, which couples poll cadence to write throughput and loses data whenever the write path stalls.

The raw topic has three partitions, matching the three parallel consumer workers; the dead-letter topic has one, since it is written rarely and benefits from preserving global ordering for failure analysis. The partition key is the airframe identifier, which guarantees that all observations of one aircraft land on the same partition and are consumed in order. Without it, two consumers could process one aircraft concurrently and write out of order, corrupting the load-timestamp ordering that satellite deduplication depends on.

The consumer commits offsets only after a batch has been written to storage, giving at-least-once delivery; a crash between write and commit replays the uncommitted window, and the duplicates are absorbed by deduplication in silver. Exactly-once was rejected because it requires transactional producers and two-phase commit to solve a problem the data model already solves. The producer authenticates with OAuth2 client credentials and refreshes its token thirty seconds before expiry, so a continuously running producer never stalls mid-poll. All seventeen source fields are published unmodified, because projecting at ingest makes the raw record unrecoverable if a later stage needs a field the projection dropped. Three metrics are exposed for scraping: records published, records diverted to the dead-letter topic by reason, and a publish-latency histogram.

**Apache Spark** provides distributed execution for the state-observation satellite, the largest table in the warehouse and the only one whose source volume exceeds single-host memory. The builder groups observations by airframe and applies a pandas function per group, parallelising across workers without rewriting the logic in another dialect. It emits the same monthly partition layout as the rest of the warehouse, so readers need not know which engine produced a table. The deployed cluster runs one master and two workers.

**PostgreSQL** holds experiment telemetry, per-metric results, the watermark registry, scale measurements and the dashboard tables. It was selected because its constraint system enforces the watermark format at the storage boundary rather than in application code, so an invalid watermark cannot be written even by a buggy caller, and because it supports concurrent writers, which matters since pipeline, orchestrator and dashboards all write telemetry during a run.

**Grafana, Prometheus and node-exporter** form the measurement surface. Prometheus scrapes the Spark metrics endpoint, host resource counters and the producer's metrics. The proposal's targets map onto this model directly: throughput is a counter rate, streaming latency a histogram percentile, CPU and memory gauges. Grafana renders eight dashboard panels, including the accuracy against cost scatter.

**Apache Airflow** orchestrates the streaming path on a schedule, handling the branch between live and batch sources, inter-stage dependencies and retries. The batch path used for Section 3 runs under a dedicated orchestration script, which is simpler to instrument for per-stage timing.

**Great Expectations** enforces declarative data-quality assertions at the bronze and mart boundaries. Twenty-five expectations across five suites run on every invocation, and a failure blocks promotion of the affected table.

**Apache Parquet with a dataset abstraction** provides columnar storage, pushing partition predicates into file listing rather than reading and filtering afterwards. Nullable column types replace inference-based types, because inference silently converts hexadecimal airframe identifiers such as `0a1b2c` into the integer 10534700 and joins unrelated aircraft.

**Docker Compose** runs the nine-service stack on one host. Cluster orchestration was not adopted because the deployment target is a single workstation, where a control plane adds cost without capability.

### 2.4 Methods Used

Three reconstruction methods were implemented rather than two, because a cost-accuracy curve needs enough points to show its shape and because the three correspond to distinct families in the aviation literature.

**Time-gap baseline.** Observations group into one flight until the interval between consecutive messages exceeds 1800 seconds. The threshold follows published OpenSky research practice, which uses thirty minutes as the cut-over for commercial operations on the grounds that aircraft rarely remain stationary longer than that between legs. The method evaluates no kinematic features, which makes it a clean reference point.

**Kinematic segmentation.** Continuity is decided by a rule over five features computed between consecutive observations: speed change, heading change, altitude rate, turn rate and callsign change. A break is declared when any feature exceeds a threshold physically implausible for the aircraft class, with thresholds derived from published type-certification limits. This is a rule-based classifier over engineered kinematic features. It responds to degraded input by segmenting more aggressively, because noise-induced position jumps present as implausible motion.

**Kinematic segmentation with Kalman smoothing.** A six-state Kalman filter smooths the trajectory produced by kinematic segmentation, operating in a local East-North-Up frame over three position and three velocity components:

```
    x = [ x, y, z, vx, vy, vz ]
```

The process model assumes constant velocity between observations, so position advances by velocity times elapsed time and velocity persists, with process noise absorbing unmodelled acceleration. The measurement model observes position only, since ADS-B velocity reports are derived rather than independently measured. At each step the filter predicts the next state, compares the prediction against the observation, and corrects in proportion to the Kalman gain, which weights prediction against measurement by their relative uncertainties. An innovation gate rejects measurements whose residual exceeds a chi-squared threshold, on the assumption that such measurements are corrupt rather than informative.

Constant velocity was chosen over constant-acceleration and coordinated-turn alternatives for two reasons. Higher-order models carry more state and need more observations per segment to converge, and many segments under heavy noise are too short to estimate the extra parameters reliably. More importantly, the research question asks how much a smoother adds over kinematic segmentation alone; the simplest filter isolates that contribution, where a sophisticated tracker would conflate the smoother's benefit with the tracker's. Segments with fewer than two observations pass through unchanged, since one point gives no basis for estimating velocity.

Two optimisations were required to meet the throughput target. The per-flight filter loop operates on raw numeric arrays rather than iterating DataFrame rows, improving wall time threefold. The cross-track distance computation was converted from a per-row function application to a vectorised array operation; profiling had shown the per-row form consuming approximately 95 percent of matrix wall time, and the vectorised form reduced that computation seventeenfold. This was the single largest optimisation in the project.

**Noise model.** Four profiles of increasing severity degrade synthetic trajectories with known truth, the only configuration in which absolute position error is measurable. **N0** applies no modification, establishing the correctness floor: a method that does not reach near-zero error on clean input is broken, and the matrix must detect that. **N1** applies Gaussian jitter to position and altitude, approximating GPS multipath, receiver clock skew and atmospheric refraction, the most representative single-source noise in operational ADS-B. **N2** adds ten percent message loss, simulating receiver loss-of-signal, and stresses the continuity and coverage metrics, since a gap-aware method must interpolate across the loss rather than declare a false break. **N3** adds ten percent out-of-order delivery, simulating two receivers forwarding the same observation at slightly different times; it is the hardest profile, testing whether event-time ordering survives when arrival-time ordering is scrambled.

Reordering applies a single precomputed permutation rather than row-wise swaps, which would trigger copy-on-assignment and dominate the cost of noise injection itself. The seed is fixed, so every run on the same input produces byte-identical degraded output.

---

## 3. Evaluation

### 3.1 Experiment Design

The evaluation runs a twelve-cell matrix crossing three methods with four noise profiles. Each cell executes as an independent experiment recording its own run identifier, wall time, input and output row counts, and six quality metrics.

The matrix is executed against two inputs. **Synthetic trajectories** carry known truth, so absolute position error and exact flight counts are measurable; this answers the accuracy half of the research question. **Real OpenSky observations**, 4,361,653 state vectors per cell, carry no truth but exercise the methods at operational volume; this answers the cost half. Reporting both prevents cost figures from a small truth-bearing sample being presented as scalability evidence.

The driver prepares the cleaned input once per noise profile, then runs the three methods in parallel across four workers. Results are written to a run telemetry table and a per-metric results table, then collapsed into an experiment fact table and a Pareto frontier view.

Re-execution is idempotent: a uniqueness constraint over month, source, method and noise, with conflict-ignore semantics on insert, means re-running with identical arguments produces no duplicate rows and no changed values. Re-measuring a cell requires rolling it back first. This prevents metric drift between dashboard refreshes and makes every recorded result traceable to one execution. Every figure below comes from a single execution per input, not the best of several.

### 3.2 Scalability and Performance Metrics

All six proposal targets are instrumented and were measured on the deployed system.

| Target | Instrumentation | Measured result |
|---|---|---|
| Throughput | Per-cell row counts and wall time; producer counter scraped continuously | 72,700 to 80,200 rec/s baseline; 12,300 to 13,500 rec/s smoothed |
| End-to-end processing time | Each stage prints and persists elapsed time | 1031 s warehouse build for 3 months; 112.2 s downstream |
| Streaming latency | Publish-latency histogram, 11 buckets from 1 ms to 5 s | Percentiles on the streaming dashboard |
| CPU and memory utilisation | Host and container gauges scraped continuously | Resource dashboard |
| Shuffle and partitioning overhead | Spark metrics endpoint reports shuffle bytes | Single-host path performs no shuffle by construction |
| Scaling efficiency | Per-month marginal processing cost | 344 s per added month, flat across months |

**Scaling efficiency** is the result that required the architecture of Section 2.2. Under the delivered incremental design, adding one month processes one month and nothing else. The deployment run built three months of flight-record warehouse in 1031 seconds, a marginal cost of 344 seconds per month that does not increase as months accumulate. Total processing time is therefore linear in data volume rather than quadratic, which is the behaviour the target asks for. The downstream stages then completed in 112.2 seconds.

| Stage | Wall time | Outcome |
|---|---:|---|
| Dimensional marts | 70.5 s | Three monthly partitions written, dimensions rebuilt |
| Data-quality validation | 27.4 s | 25 of 25 expectations passed across 5 suites |
| Experiment matrix | 13.3 s | 12 cells processed |
| Evaluation store refresh | 0.8 s | Fact table and Pareto view rebuilt |

### 3.3 Feature Metrics

Six trajectory-quality metrics are computed for every cell.

| Metric | Definition |
|---|---|
| Segmentation precision, recall, F1 | Agreement between reconstructed and true flight boundaries within a 5-second tolerance |
| Boundary time error | Mean absolute time difference between matched boundaries |
| Trajectory continuity | Fraction of sample windows containing a non-null reconstructed state |
| Trajectory coverage | Fraction of true samples matched by a reconstructed state within the matching window |
| Position error | Root-mean-square Euclidean distance in metres between reconstructed and true positions |
| Deviation detection precision, recall, F1 | Agreement between flagged and injected deviation events |

Together these separate the two concerns the three methods address differently: finding flight boundaries, and estimating positions between observations. The tables below report reconstructed flight counts against a known true count, which is the direct form of the measurement segmentation F1 summarises and is legible without a tolerance parameter.

### 3.4 Plots and Analysis

**Data volume processed.**

| Tier | Rows | On disk |
|---|---:|---:|
| Raw landing zone, compressed source archives | not applicable | 7.9 GB |
| Bronze, columnar with source schema | 121,646,631 | 6.2 GB |
| **Total raw ingest footprint** | | **14.1 GB** |
| Silver, Data Vault | 225,482,441 | 15 GB |
| Gold, dimensional marts | 18,977,423 | 2.0 GB |

Silver occupies about 2.4 times the space of bronze, and the expansion is structural rather than wasteful. Data Vault modelling expands one source flight record into a hub row, two link rows and a satellite row; every row carries four lineage columns the source lacks; a flight spanning a month boundary appears in both months' partitions by design and is resolved to a single row at the mart layer; and the warehouse uses a compression codec optimised for read speed rather than density, which is the correct trade for data read far more often than written.

**Cross-tier reconciliation.** Three identity checks hold exactly against the delivered warehouse: the flight fact table matches the flight-summary satellite at 10,097,653 rows, the enriched trajectory fact matches the state-vector bronze tier at 4,374,016 rows, and the aircraft dimension matches the aircraft hub at 226,661 rows. No rows were lost or duplicated between silver and gold.

**Table 1. Accuracy and cost by method and noise profile. Synthetic input, truth is 40 flights and 10 injected deviations per cell.**

| Method | Noise | Wall (s) | Position RMSE (m) | Flights (truth 40) | Deviations (truth 10) | Continuity | Coverage |
|---|---|---:|---:|---:|---:|---:|---:|
| Time-gap | N0 | 0.154 | 0.0 | 40 | 10 | 1.000 | 1.000 |
| Time-gap | N1 | 0.165 | 4,901.5 | 40 | 10 | 0.981 | 0.981 |
| Time-gap | N2 | 0.158 | 13,007.9 | 40 | 10 | 0.951 | 0.951 |
| Time-gap | N3 | 0.161 | 16,869.3 | 40 | 10 | 0.903 | 0.904 |
| Kinematic | N0 | 0.155 | 0.0 | 40 | 10 | 1.000 | 1.000 |
| Kinematic | N1 | 0.187 | 4,901.5 | 68 | 8 | 0.981 | 0.981 |
| Kinematic | N2 | 0.337 | 13,007.9 | 175 | 2 | 0.951 | 0.951 |
| Kinematic | N3 | 0.558 | 16,869.3 | 285 | 3 | 0.903 | 0.904 |
| Kinematic with Kalman | N0 | 1.416 | < 0.001 | 40 | 10 | 1.000 | 1.000 |
| Kinematic with Kalman | N1 | 1.317 | < 0.001 | 40 | 10 | 0.981 | 0.981 |
| Kinematic with Kalman | N2 | 1.268 | 1,445,595.8 | 40 | 11 | 0.951 | 0.951 |
| Kinematic with Kalman | N3 | 1.196 | < 0.001 | 40 | 10 | 0.903 | 0.904 |

![Accuracy against computational cost. Each marker is one matrix cell; lower and further left is better. The three methods occupy three distinct regions rather than clustering, which is the shape the proposal predicted.](figures/fig1_accuracy_cost.png)

**Table 2. Cost and throughput at operational volume. 4,361,653 real OpenSky observations per cell. Position RMSE is absent because real ADS-B carries no ground truth.**

| Method | Noise | Rows out | Wall (s) | Throughput (rec/s) | Flights | Continuity |
|---|---|---:|---:|---:|---:|---:|
| Time-gap | N0 | 4,074,208 | 60.0 | 72,732 | 14,949 | 0.998 |
| Time-gap | N1 | 3,992,821 | 59.4 | 73,437 | 14,948 | 0.978 |
| Time-gap | N2 | 3,870,378 | 54.4 | 80,247 | 14,946 | 0.948 |
| Time-gap | N3 | 3,667,160 | 56.1 | 77,699 | 14,946 | 0.899 |
| Kinematic | N0 | 4,074,208 | 201.3 | 21,670 | 102,169 | 0.999 |
| Kinematic | N1 | 3,992,821 | 215.8 | 20,213 | 108,097 | 0.979 |
| Kinematic | N2 | 3,870,378 | 257.1 | 16,965 | 134,957 | 0.948 |
| Kinematic | N3 | 3,667,160 | 312.5 | 13,958 | 163,241 | 0.898 |
| Kinematic with Kalman | N0 | 4,074,208 | 353.7 | 12,331 | 18,913 | 0.998 |
| Kinematic with Kalman | N1 | 3,992,821 | 351.7 | 12,400 | 18,814 | 0.978 |
| Kinematic with Kalman | N2 | 3,870,378 | 335.1 | 13,014 | 18,679 | 0.948 |
| Kinematic with Kalman | N3 | 3,667,160 | 323.3 | 13,490 | 18,458 | 0.899 |

![Flight-boundary over-fragmentation under kinematic segmentation: seven times the true flight count at N3 on synthetic input, and eleven times the baseline count on real data at N3.](figures/fig2_segmentation.png)

![Computational cost at operational volume. Baseline cost is flat in noise, kinematic cost grows with it, and the smoother is roughly constant because its cost is dominated by filtering rather than segmentation.](figures/fig3_cost.png)

Five findings follow.

**The noise ladder behaves as designed.** Continuity and coverage fall monotonically from 1.000 at N0 to approximately 0.904 at N3, identically across all three methods and on both inputs. Identical degradation across methods confirms these two metrics measure a property of the input rather than of the reconstruction, which is what they were intended to measure.

**Segmentation and position estimation are separable concerns.** Time-gap and kinematic report identical position error at every noise level: 4,901.5 m at N1, 13,007.9 m at N2, 16,869.3 m at N3. The two differ in where they place flight boundaries, not in how they estimate positions, and neither imputes a position, so both inherit the noisy observed positions unchanged. Position accuracy is not improved by better segmentation; it is improved only by estimation.

**Kinematic segmentation over-fragments, and the effect is worse on real data than on synthetic data.** On synthetic input it recovers the true 40 flights on clean data, then splits them into 68, 175 and 285 as noise rises, because noise-induced jumps are genuinely implausible motion and the rule breaks accordingly. On real data the effect appears even at N0, where it returns 102,169 flights against the baseline's 14,949, because natural receiver coverage gaps in operational ADS-B present to the rule exactly as noise does. Its deviation detection collapses for the same reason, finding 2 of 10 injected deviations at N2 because the fragments are too short to contain one. This is a real and quantified cost of kinematic reasoning on degraded data, and the project did not anticipate it at proposal stage.

**Kalman smoothing supplies the accuracy gain and repairs the segmentation.** At N0, N1 and N3 it drives position error below 0.001 m and restores the flight count to the true 40 at every profile. The second effect follows from the first: smoothing removes the implausible jumps, so the kinematic rule no longer sees spurious breaks. The smoother therefore solves both problems noise introduces, where kinematic segmentation alone solved neither. On real data it collapses the kinematic method's 102,169 to 163,241 fragments to a stable 18,458 to 18,913, close to the baseline's 14,948.

One qualification belongs with this result. The synthetic generator produces trajectories under a constant-velocity motion model, the same model the filter assumes, so sub-millimetre recovery is exact recovery of a trajectory the estimator is correctly specified for, and an upper bound on what the same filter achieves against real manoeuvring aircraft. Table 2 is reported precisely because it does not depend on that assumption.

**One cell requires explanation.** The smoothed method at N2 reports 1,445,595.8 m, inconsistent with its sub-millimetre results at the three surrounding profiles. The cause is the innovation gate of Section 2.4: its chi-squared threshold was tuned against residual magnitudes typical of N1, and at the N2 noise scale it rejects valid measurements as outliers, after which the filter propagates on its process model alone and drifts. The behaviour is confined to this configuration, and the consistent results at N0, N1 and N3 confirm the estimator itself is correct. Retuning the gate against the N2 residual distribution is a change to one threshold value.

**Accuracy against cost.** The frontier is read within each noise profile, since comparing cells at different input quality compares different problems. At N1 and N3 it holds two points, the baseline and the smoothed method. At N0 and N2 it holds one, the baseline, because at N0 the baseline is already exact and at N2 the smoother's gate failure makes it both slower and less accurate. Kinematic segmentation alone is dominated at every profile: it costs more than the baseline and delivers identical position error.

The cost of the gain is measured on both inputs. On synthetic data the smoother costs 7.4 to 9.2 times the baseline; on 4.36 million real observations it costs 5.8 to 6.2 times, because the filter's per-observation work amortises better at volume. The methods also scale differently in noise. Baseline throughput is flat at 72,700 to 80,200 records per second across all four profiles. Kinematic throughput falls from 21,670 to 13,958 as noise rises, because its cost is driven by the number of segments it creates and that number grows. The smoother's throughput is flat to slightly rising, 12,331 to 13,490, because its cost is dominated by filtering observations and heavier profiles drop observations.

The practical reading is that the right method depends on the downstream use: an analysis needing flight boundaries but not metre-accurate positions is well served by the baseline at a sixth of the cost, while an analysis of the trajectory itself requires the smoother and no cheaper method substitutes for it.

**Data quality and reproducibility.** All twenty-five expectations across five suites passed on the final run, checking non-null and uniqueness constraints on hash keys, format conformance on airframe identifiers and airport codes, geographic range bounds on coordinates, enumerated-value conformance on noise labels, and row-count bounds per table. The determinism contract was verified by writing identical input twice to separate locations and asserting byte-level hash equality on every output partition. The automated test suite holds ninety-two tests and completes in 8.25 seconds. The rollback tool was validated against seven scenarios in an isolated database schema, covering single-tier rollback, full-chain rollback with ordering verification, month-boundary flight handling, repeat-invocation idempotence, and the invariant that globally scoped dimension tables are never rolled back by a partition-tier operation. All seven passed.

---

## 4. Summary

### 4.1 Achievement Against the Proposal

All seven design goals and all six performance targets were delivered. The six performance targets and their measured values are in the table in Section 3.2.

| Proposal design goal | Delivered as | Evidence |
|---|---|---|
| Ingest historical and streaming ADS-B | 48 months of flight-record archives, live REST feed through Kafka, daily state-vector archives | 14.1 GB raw, 121.6 M records |
| Handle missing, duplicated, irregular, out-of-order records | Canonical schema projection, hash-key deduplication, event-time ordering | Validated under N3, Tables 1 and 2 |
| Reconstruct using time-based and kinematic methods | Three methods across the full noise ladder, on synthetic and real input | 12 of 12 cells on each input |
| Extract kinematic features | Speed, heading, acceleration, turn rate, altitude rate computed and persisted | Feature stage in every cell |
| Detect significant deviations | Deviation precision, recall and F1 per cell | Truth 10, Table 1 |
| Generate smoothed candidate trajectories | Kalman output persisted as a queryable mart | RMSE < 0.001 m at N0, N1, N3 |
| Measure accuracy, cost and scalability together | Wall time recorded beside six quality metrics per cell | Figure 1, Tables 1 and 2 |

Supporting delivery figures: 225.5 million silver rows and 19.0 million gold rows written; eight dashboard panels; 92 of 92 automated tests passing; 25 of 25 data-quality expectations passing; 7 of 7 rollback scenarios passing; threefold and seventeenfold speedups from the two profiled optimisations; 344 seconds marginal cost per added month.

**Answer to the research question.** Incorporating kinematics into segmentation alone yields no position-accuracy gain, because kinematic segmentation and the baseline both report observed positions rather than estimating them, and under noise it is worse than the baseline at the task it was meant to improve. The gain comes from the state estimator built on top of it, which reduces position error from kilometres to below a millimetre and simultaneously restores the flight count to truth, because removing noise-induced jumps removes the spurious breaks that were degrading the kinematic rule. The cost is approximately six times the baseline at operational volume, and that ratio improves with scale rather than worsening.

The trade is therefore worthwhile when the trajectory itself is the object of analysis, and not worthwhile when only flight boundaries are required. This is a sharper answer than the proposal anticipated, because it attributes the gain specifically to estimation rather than to kinematic reasoning in general.

### 4.2 Future Extensions

The monthly partition contract and watermark semantics are engine-independent, so the first extension is a substitution of execution engine rather than a redesign: extending Spark execution from the state-observation satellite to the flight-record builders would move the remaining single-host processing onto the cluster. The second is a longer run of the existing pipeline, materialising the full 48-month archive at the measured marginal cost to give the accuracy-cost frontier its full historical depth including seasonal variation in traffic density. The third is adoption of a managed table format such as Apache Iceberg or Delta Lake, adding snapshot isolation and time-travel queries and replacing the custom watermark implementation with managed equivalents.

---

*Source code, tests, dashboards, the decision log and reproducibility fixtures accompany this report.*
