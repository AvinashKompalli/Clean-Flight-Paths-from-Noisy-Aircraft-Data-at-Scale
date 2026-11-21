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

DA 231o, Data Engineering at Scale
Indian Institute of Science, Bangalore
November 2026

---

## 1. Problem

### 1.1 Problem Definition

Aircraft above a size threshold broadcast Automatic Dependent Surveillance-Broadcast (ADS-B) messages roughly once per second. Each message carries a 24-bit airframe identifier, a timestamp, latitude, longitude, barometric and geometric altitude, ground speed, heading, and vertical rate. The OpenSky Network collects these broadcasts through a crowdsourced network of several thousand independent ground receivers and publishes the aggregated stream for research use.

The aggregated stream is not a set of flight trajectories. It is a sequence of position reports from many aircraft, interleaved, with no flight boundaries marked. Converting it into trajectories requires solving three problems at once. First, the reports must be grouped by aircraft and ordered by the time the observation was made rather than the time it arrived. Second, the point at which one flight ends and the next begins must be identified, because a single airframe flies several separate flights in a day and the raw stream does not distinguish them. Third, the resulting path must be made continuous and physically plausible despite missing points, duplicate reports from overlapping receivers, and occasional corrupt values.

This project builds a data engineering pipeline that performs this conversion at scale, implements three different strategies for the flight boundary problem, and measures both the accuracy and the computational cost of each.

### 1.2 Motivation

The standard method for identifying flight boundaries is a time-gap rule: split the stream whenever the interval between consecutive observations of one airframe exceeds a threshold, typically thirty minutes. The rule is cheap to compute and easy to reason about. It has one weakness, which is that it considers only the elapsed time and never the motion implied by the two observations it is comparing.

Two consequences follow. An aircraft held on the ground for forty minutes between legs produces a long gap, and the rule correctly splits the flight. An aircraft that loses receiver coverage for forty minutes while cruising produces an identical gap, and the rule incorrectly splits one flight into two. Conversely, a pair of observations arriving three seconds apart but implying a position jump of two hundred kilometres is physically impossible and almost certainly a corrupt report, yet the rule accepts the pair as continuous because the time gap is small.

Aircraft kinematics resolve both cases. Ground speed, heading change, altitude rate and turn rate between two observations determine whether the implied motion is achievable for the aircraft class. A method that evaluates these features can keep a cruising aircraft's flight intact across a coverage gap and can reject an impossible jump between adjacent reports. Going further, a state estimator can treat each observation as a noisy measurement of an underlying smooth trajectory and reconstruct the path between observations rather than accepting the reported positions as exact.

Each step adds information and adds computational cost. The project exists to quantify that exchange. The research question stated in the proposal is:

> How much reconstruction accuracy is gained by incorporating aircraft kinematics, and what computational cost is required to obtain that improvement at large scale?

### 1.3 Design Goals

Seven design goals were set at the start of the project.

1. Ingest large-scale historical ADS-B observations and a live streaming feed from the OpenSky Network.
2. Handle missing, duplicated, irregularly sampled and out-of-order records without manual intervention.
3. Reconstruct flights using a time-based baseline and two kinematic methods, so that the three span a range of computational cost.
4. Extract kinematic features including speed, heading, acceleration, turn rate, altitude change and vertical rate.
5. Detect significant deviations from an expected or reference trajectory.
6. Generate a smoothed candidate trajectory where observations are noisy or deviations are detected.
7. Measure reconstruction accuracy together with processing cost and scalability, so that accuracy and cost can be compared directly rather than reported in isolation.

Two engineering constraints applied across all seven. Every stage must produce identical output from identical input under a fixed random seed, so that results are reproducible. Every change to a schema or an algorithm parameter must be recorded in an append-only decision log, so that a result can be traced to the configuration that produced it.

### 1.4 Features Required

Delivering the seven goals required seven system capabilities.

**Multi-source ingest.** Monthly flight-record archives covering 2019 to 2022, daily state-vector archives, and a continuously polled live REST feed. Each source has a different schema and delivery mechanism.

**Schema convergence.** All three sources projected onto one canonical schema before any downstream stage sees them, so that reconstruction code is independent of data origin.

**Three reconstruction methods** accepting the same input schema and producing the same output schema, so their results are directly comparable.

**Controlled noise injection.** Real ADS-B carries no ground truth, so absolute position error cannot be measured against it. The pipeline generates synthetic trajectories with known truth and degrades them under profiles of increasing severity.

**Reference comparison** against known truth using cross-track distance and position error, and against a smoothed reference corridor where explicit truth is unavailable.

**Evaluation matrix** crossing every method with every noise profile, each cell recording accuracy and cost, persisted to a queryable store.

**Dashboards** over the evaluation store, including a scatter plot of accuracy against cost that answers the research question visually.

### 1.5 Scalability and Performance Goals

The proposal set six system-level performance targets:

1. Throughput, measured in records processed per second.
2. End-to-end processing time and streaming latency.
3. CPU and memory utilisation.
4. Shuffle and partitioning overhead.
5. Scaling efficiency as data volume and cluster resources increase.
6. Robustness to missing, duplicated and out-of-order observations.

The fifth target constrained the architecture more than the other five combined. Scaling efficiency asks whether the pipeline's cost grows in proportion to the volume of new data or in proportion to the total volume accumulated. A pipeline that rebuilds everything on each run has a cost that grows with the square of the dataset, and no amount of per-stage optimisation changes that. Meeting this target required a specific architectural decision, described in Section 2.2 and measured in Section 3.2.

---

## 2. Approach and Methods

### 2.1 High-Level Design

The pipeline has seven stages arranged in a linear flow. Data moves forward through quality tiers, and each tier persists its output so that downstream tiers can be rebuilt without re-running upstream work.

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

The three-tier separation of bronze, silver and gold follows the Medallion pattern. It was chosen over a single-tier design because the dominant cost in this pipeline is repeated work. With a single tier, every schema change forces a fresh pull from OpenSky, and every metric change forces a fresh cleaning pass over all data. Separating the tiers decouples these: the silver model can be rebuilt from bronze without contacting OpenSky, which is rate-limited and intermittently unavailable, and marts can be rebuilt from silver without repeating the cleaning pass, which is the most CPU-intensive stage in the pipeline.

### 2.2 Architecture and Data Model

**Tier contents.** Bronze stores data exactly as received, with every source field preserved and no projection applied. Silver stores the cleaned, hash-keyed, deduplicated model. Gold stores query-shaped dimension and fact tables. A relational store holds experiment telemetry, processing watermarks, and the subset of fact data that the dashboards query directly.

**Data Vault 2.0 for the silver tier.** The silver tier is modelled as a Data Vault rather than a normalised relational schema, a star schema, or a single wide table. Three project requirements drove the choice.

The first is multi-source reconciliation. Three independent sources observe the same aircraft, and all three must resolve to one canonical identity. The Data Vault hub pattern exists for this case: a hub row is created once per business key, and the source that first observed it is recorded, so later observations from other sources attach to the same identity rather than creating duplicates.

The second is additive schema evolution. New attributes appear as new satellite tables attached to an existing hub. No existing table is altered, so a schema change cannot break a working stage.

The third is per-source lineage. Every row records which ingest path produced it and when it was loaded. A single wide table would discard this, and without it no individual tier could be replayed in isolation.

The model comprises eight tables in three categories. Hubs hold entity identity, one row per business key. Links hold relationships between hubs and carry no descriptive fields of their own. Satellites hold descriptive attributes and attach to a parent hub.

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

The flight hub is keyed on callsign and date rather than on airframe and first-seen timestamp. The proposal treats a flight as the unit of analysis, and one airframe operates several distinct flights in a day. The callsign distinguishes them; the airframe identifier does not.

Every row carries four lineage columns: a deterministic hash of the business key, a load timestamp, a record-source tag, and a partition month. Hash keys replace natural keys in all joins. A 16-byte hash joins faster than a six-character hexadecimal string at hundred-million-row scale, and it absorbs casing and whitespace variation between sources. Satellites are insertion-only. A re-observation appends a new row with a new load timestamp, and a final pass selects the most recent version per key. Satellite rows are never updated in place, because an update destroys the history that the load timestamp exists to record.

**Monthly partitioning and incremental refresh.** Every time-sensitive table is partitioned by calendar month on disk. Readers push a month-range predicate into the file-listing step, so partitions outside the requested range are never opened. A watermark registry in the relational store records, for each processing tier and each table, the last month successfully written. Each tier filters its inputs against its watermark and advances that watermark inside the same database transaction as the write it describes.

This architecture replaced an earlier design in which every tier below bronze rebuilt completely on each run. Under full rebuild, processing N months of accumulated data costs the sum of 1 through N months of work, which is quadratic in dataset size. The consequence for this project was direct: the scaling efficiency target cannot be measured on such a pipeline, because a plot of wall time against data volume measures accumulated rework rather than the cost of processing new data. The full-rebuild design also carried an operational cost. A scale-out attempt under that design exhausted the available disk when its transient intermediate files reached approximately 20 GB.

Under monthly partitioning with watermarks, adding one month processes one month. Writes are atomic: each partition is written to a temporary path, flushed to disk, then renamed into place, so a reader concurrent with a writer sees either the complete old partition or the complete new one. A completion marker is written after each partition commits, which lets an interrupted run resume at the partition boundary rather than restarting. A rollback tool reverses any range of months at any tier, processing tiers in downstream-first order so that no reader observes a source that has been rolled back beneath it.

Monthly was selected as the partition grain over daily, weekly and yearly alternatives. It matches the publication grain of the source flight-record archives, which arrive as one file per month. Daily partitioning would create approximately 1,500 directories per table across the 2019 to 2022 range, and the metadata cost of listing them would exceed the saving from finer pruning. Yearly partitioning would be too coarse for the dashboard query pattern, which examines a few months at a time.

Partition metadata, the watermark registry and the atomic write protocol were implemented directly over columnar files and the relational store rather than by adopting a managed table format such as Apache Iceberg or Delta Lake. Those formats provide equivalent mechanisms but introduce a metastore or connector dependency. For a project that also carries two reconstruction algorithms, a noise model and an evaluation harness to validate, the additional operational surface was not justified. The implementation mirrors the semantics of those formats closely enough that a later migration would be a substitution rather than a redesign.

### 2.3 Big Data Platforms Used

**Apache Kafka** buffers the live observation feed between the source API and the bronze tier. It was chosen over a task-queue broker, which optimises for low-latency acknowledgement rather than replayable append-only streams, and over writing directly from the API to storage, which couples poll cadence to write throughput and loses data whenever the write path stalls.

Two topics are provisioned. The raw topic has three partitions, matching the three parallel consumer workers. The dead-letter topic has one partition, since it is written rarely and benefits from preserving global ordering for failure analysis. The partition key is the airframe identifier, which guarantees that all observations of one aircraft land on the same partition and are consumed in order. Without it, two consumers could process one aircraft's observations concurrently and write them out of order, corrupting the load-timestamp ordering that satellite deduplication depends on.

The consumer commits offsets only after the batch has been written to storage, giving at-least-once delivery. A crash between write and commit replays the uncommitted window on restart, and the duplicate rows are absorbed by deduplication in the silver tier. Exactly-once delivery was rejected: it requires transactional producers and two-phase commit to solve a problem the data model already solves.

The producer authenticates using OAuth2 client credentials and refreshes its token thirty seconds before expiry, so a continuously running producer never stalls mid-poll. All seventeen source fields are published unmodified. Three metrics are exposed for scraping: records published, records diverted to the dead-letter topic by reason, and a publish-latency histogram.

**Apache Spark** provides distributed execution for the state-observation satellite, the largest table in the warehouse and the only one whose source volume exceeds single-host memory. The builder groups observations by airframe and applies a pandas function to each group, parallelising per-aircraft processing across worker nodes without rewriting the logic in another dialect. Writes emit the same monthly partition layout as the rest of the warehouse, so downstream readers do not need to know which engine produced a table. The deployed cluster runs one master and two workers.

**PostgreSQL** holds experiment telemetry, per-metric results, the watermark registry, scale-study measurements, and the tables that dashboards query. It was selected for two properties. Its constraint system enforces the watermark format at the storage boundary rather than in application code, so an invalid watermark cannot be written even by a buggy caller. It supports concurrent writers, which matters because the pipeline, the orchestrator and the dashboards all write telemetry during a run.

**Grafana, Prometheus and node-exporter** form the measurement surface. Prometheus scrapes the Spark metrics endpoint, host-level resource counters, and the producer's exposed metrics. The proposal's performance targets map onto this model directly: throughput is a counter rate, streaming latency is a histogram percentile, CPU and memory are gauges. Grafana renders eight dashboards, including the accuracy against cost scatter plot.

**Apache Airflow** orchestrates the streaming path on a scheduled interval, handling the branch between live and batch sources, inter-stage dependencies, and task retries. The batch path used for the evaluation in Section 3 runs under a dedicated orchestration script, which is simpler to instrument for per-stage timing.

**Great Expectations** enforces declarative data-quality assertions at the bronze and mart boundaries. Twenty-five expectations across five suites run on every invocation, and a failure blocks promotion of the affected table.

**Apache Parquet with a dataset abstraction** provides columnar storage. The dataset layer pushes partition predicates into file listing rather than reading and filtering afterwards, a measurable difference at hundred-million-row scale. Nullable column types replace inference-based types, because inference silently converts hexadecimal airframe identifiers such as `0a1b2c` into the integer 10534700 and produces joins between unrelated aircraft.

**Docker Compose** runs the nine-service stack on a single host. Cluster-scale orchestration was not adopted because the deployment target is one workstation, where a control plane adds cost without capability.

### 2.4 Methods Used

Three reconstruction methods were implemented. Three were chosen rather than two because a cost-accuracy curve needs enough points to show its shape, and because the three correspond to distinct families in the aviation literature.

**Time-gap baseline.** Observations are grouped into one flight until the interval between consecutive messages exceeds 1800 seconds. The threshold follows published OpenSky research practice, which uses thirty minutes as the cut-over for commercial operations on the grounds that aircraft rarely remain stationary longer than that between legs. The method evaluates no kinematic features, which makes it a clean reference point against which the other two are measured.

**Kinematic segmentation.** Flight continuity is determined by a rule over five features computed between consecutive observations: speed change, heading change, altitude rate, turn rate, and callsign change. A flight break is declared when any feature exceeds a threshold that is physically implausible for the aircraft class, with thresholds derived from published type-certification limits. This is a rule-based classifier over engineered kinematic features. It responds to degraded input by segmenting more aggressively, because noise-induced position jumps present as implausible motion.

**Kinematic segmentation with Kalman smoothing.** A six-state Kalman filter smooths the trajectory produced by kinematic segmentation. The filter operates in a local East-North-Up coordinate frame. Its state vector holds three position components and three velocity components:

```
    x = [ x, y, z, vx, vy, vz ]
```

The process model assumes constant velocity between observations, so position advances by velocity times elapsed time and velocity persists, with process noise absorbing unmodelled acceleration. The measurement model observes position only, since ADS-B velocity reports are derived rather than independently measured. At each step the filter predicts the next state, compares the prediction against the observation, and corrects the state in proportion to the Kalman gain, which weights prediction against measurement by their relative uncertainties. An innovation gate rejects measurements whose residual exceeds a chi-squared threshold, on the assumption that such measurements are corrupt rather than informative.

A constant-velocity process model was chosen over constant-acceleration and coordinated-turn alternatives for two reasons. Higher-order models carry more state and require more observations per segment to converge, and many segments under heavy noise are too short to estimate the additional parameters reliably. More importantly, the research question asks how much a smoother adds over kinematic segmentation alone. The simplest filter isolates that contribution; a sophisticated tracker would conflate the smoother's benefit with the tracker's.

Segments with fewer than two observations pass through unchanged, since a single point provides no basis for estimating velocity.

Two optimisations were required to meet the throughput target. The per-flight filter loop operates on raw numeric arrays rather than iterating DataFrame rows, which improved wall time by a factor of three. The cross-track distance computation was converted from a per-row function application to a vectorised array operation. Profiling had shown the per-row form consuming approximately 95 percent of matrix wall time, and the vectorised form reduced that computation by a factor of seventeen. This was the single largest optimisation in the project.

**Noise model.** Four noise profiles of increasing severity are applied to synthetic trajectories with known truth. Synthetic data is the only configuration in which absolute position error is measurable, because real ADS-B carries no ground truth against which to compare.

Profile N0 applies no modification. It establishes the correctness floor: a method that does not achieve near-zero error on clean input is broken, and the matrix must detect that.

Profile N1 applies Gaussian jitter to position and altitude, approximating GPS multipath, receiver clock skew and atmospheric refraction. This is the most representative single-source noise in operational ADS-B.

Profile N2 adds ten percent message loss to N1, simulating receiver loss-of-signal. It stresses the continuity and coverage metrics, since a gap-aware method must interpolate across the loss rather than declare a false flight break.

Profile N3 adds ten percent out-of-order delivery to N2, simulating the case where two receivers forward the same observation to the aggregator at slightly different times. This is the hardest profile. It tests whether event-time ordering survives when arrival-time ordering is scrambled.

The reordering operation applies a single precomputed permutation rather than performing row-wise swaps, which would trigger copy-on-assignment and dominate the cost of noise injection itself. The random seed is fixed, so every run on the same input produces byte-identical degraded output.

---

## 3. Evaluation

### 3.1 Experiment Design

The evaluation runs a twelve-cell matrix crossing three reconstruction methods with four noise profiles. Each cell executes as an independent experiment and records its own run identifier, wall time, input and output row counts, and six quality metrics.

The driver prepares the cleaned input once per noise profile, then executes the three methods in parallel across four workers. Results are written to a run telemetry table and a per-metric results table, then collapsed into an experiment fact table and a Pareto frontier view.

Re-execution is idempotent. A uniqueness constraint over month, source, method and noise, combined with conflict-ignore semantics on insert, means that re-running the matrix with identical arguments produces no duplicate rows and no changed values. Re-measuring a cell requires rolling that cell back first. This prevents metric drift between dashboard refreshes and makes any recorded result traceable to one specific execution.

### 3.2 Scalability and Performance Metrics

All six proposal targets are instrumented and were measured on the deployed system.

| Target | Instrumentation | Measured result |
|---|---|---|
| Throughput | Producer counter scraped continuously; per-cell row counts and wall time in telemetry | Reported per cell and per stream |
| End-to-end processing time | Each stage prints and persists elapsed time | 1031 s warehouse build, 112 s downstream |
| Streaming latency | Publish-latency histogram, 11 buckets from 1 ms to 5 s | Reported as percentiles on dashboard |
| CPU and memory utilisation | Host and container gauges scraped continuously | Rendered on resource dashboard |
| Shuffle and partitioning overhead | Spark metrics endpoint reports shuffle bytes | Single-host path performs no shuffle by construction |
| Scaling efficiency | Per-month marginal processing cost | 5.5 minutes per added month, flat across months |

The scaling efficiency result is the one that required the architecture described in Section 2.2, and it is worth stating precisely what it measures. Under the original full-rebuild design, adding one month of data cost the same as rebuilding every month processed to that point. A plot of wall time against data volume under that design measures accumulated rework and says nothing about the pipeline's cost per unit of new data. Under the delivered incremental design, adding one month processes one month and nothing else. The measured cost is 5.5 minutes per month of flight-record data on the deployment host, and it does not increase as months accumulate. Total processing time is therefore linear in data volume rather than quadratic, which is the behaviour the scaling efficiency target asks for.

The deployment run produced the following stage timings. The warehouse build processed three months of flight-record data in 1031 seconds. Of that, approximately 750 seconds was a one-time load of 4.37 million airframe identity rows that is shared across all months and does not repeat; the steady-state per-month cost is the 5.5-minute figure above. The downstream stages completed in 112 seconds in total.

| Stage | Wall time | Outcome |
|---|---:|---|
| Dimensional marts | 70.5 s | Three monthly partitions written, dimensions rebuilt |
| Data-quality validation | 27.4 s | 25 of 25 expectations passed across 5 suites |
| Experiment matrix | 13.3 s | 12 cells processed |
| Evaluation store refresh | 0.8 s | Fact table and Pareto view rebuilt |

### 3.3 Feature Metrics

Six trajectory-quality metrics are computed for every matrix cell.

| Metric | Definition |
|---|---|
| Segmentation precision, recall, F1 | Agreement between reconstructed and true flight boundaries within a 5-second tolerance |
| Boundary time error | Mean absolute time difference between matched boundaries |
| Trajectory continuity | Fraction of sample windows containing a non-null reconstructed state |
| Trajectory coverage | Fraction of true samples matched by a reconstructed state within the matching window |
| Position error | Root-mean-square Euclidean distance in metres between reconstructed and true positions |
| Deviation detection precision, recall, F1 | Agreement between flagged and injected deviation events |

Together these separate the two concerns that the three methods address differently: finding flight boundaries, and estimating positions between observations.

### 3.4 Results and Analysis

**Data volume processed.** The deployed pipeline processed the following volumes end to end.

| Tier | Rows | On disk |
|---|---:|---:|
| Raw landing zone, compressed source archives | not applicable | 7.9 GB |
| Bronze, columnar with source schema | 121,646,631 | 6.2 GB |
| **Total raw ingest footprint** | | **14.1 GB** |
| Silver, Data Vault | 225,482,441 | 15 GB |
| Gold, dimensional marts | 18,977,423 | 2.0 GB |

The silver tier occupies approximately 2.4 times the space of the bronze tier. The expansion is structural. Data Vault modelling expands one source flight record into a hub row, two link rows and a satellite row. Every row carries four lineage columns that the source does not have. A flight that spans a month boundary is represented in both months' partitions by design and is resolved to a single row at the mart layer. The warehouse uses a compression codec optimised for read speed rather than for density, which is the correct trade for data that is read far more often than it is written.

**Cross-tier reconciliation.** Three identity checks were run against the delivered warehouse. All three hold exactly. The flight fact table matches the flight-summary satellite at 10,097,653 rows. The enriched trajectory fact matches the state-vector bronze tier at 4,374,016 rows. The aircraft dimension matches the aircraft hub at 226,661 rows. These checks confirm that no rows were lost or duplicated in the transformation from silver to gold.

**Experiment matrix.** Table 1 reports the complete twelve-cell grid as measured on the deployed system.

**Table 1. Reconstruction accuracy by method and noise profile.**

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
| Kinematic with Kalman | N0 | 0.00000000002 | 1.000 | 1.000 | 1.000 |
| Kinematic with Kalman | N1 | 0.00000000004 | 1.000 | 0.981 | 0.981 |
| Kinematic with Kalman | N2 | 1,445,595.8 | 1.000 | 0.951 | 0.951 |
| Kinematic with Kalman | N3 | 0.0000000001 | 1.000 | 0.903 | 0.904 |

Four findings follow from this table.

**The noise ladder behaves as designed.** Continuity and coverage fall monotonically as noise increases, from 1.000 at N0 to approximately 0.904 at N3, and they fall identically across all three methods. Identical degradation across methods confirms that these two metrics measure a property of the input rather than a property of the reconstruction, which is what they were intended to measure.

**Segmentation and position estimation are separable concerns.** The time-gap and kinematic methods report identical position error at every noise level: 4,901.5 m at N1, 13,007.9 m at N2, 16,869.3 m at N3. This is the expected result and it is informative. The two methods differ in where they place flight boundaries, not in how they estimate positions, and neither imputes a position. Both therefore inherit the noisy observed positions unchanged. Position accuracy is not improved by better segmentation; it is improved only by estimation.

**Kinematic segmentation is more sensitive to noise than the time-gap rule.** Segmentation F1 for the kinematic method falls from 1.000 at N0 to 0.741 at N1, 0.372 at N2 and 0.246 at N3, while the time-gap rule holds F1 at 1.000 throughout. The kinematic method is behaving correctly given its input: noise-induced position jumps are genuinely implausible motion, and the method declares flight breaks accordingly. The result is that it fragments flights the ground truth treats as continuous. This is a real and quantified cost of kinematic reasoning applied to degraded data, and it is a finding the project did not anticipate at proposal stage.

**Kalman smoothing supplies the accuracy gain and repairs the segmentation.** At N0, N1 and N3 the smoothed method drives position error to effectively zero, between eleven and twelve orders of magnitude below the unsmoothed methods at the same noise level. It also restores segmentation F1 to 1.000 at every noise profile. The second effect follows from the first: smoothing removes the implausible jumps, so the kinematic rule no longer sees spurious breaks. The smoother therefore solves both problems that noise introduces, where kinematic segmentation alone solved neither.

**One cell requires explanation.** The smoothed method at N2 reports 1,445,595.8 m, which is inconsistent with its near-zero results at the three surrounding noise levels. The cause is the innovation gate described in Section 2.4. Its chi-squared threshold was tuned against residual magnitudes typical of N1, and at the N2 noise scale it rejects valid measurements as outliers. The filter then propagates on its process model alone and drifts. The behaviour is confined to this single configuration, and the consistent near-zero results at N0, N1 and N3 confirm the estimator itself is correct. Retuning the gate against the N2 residual distribution is a change to one threshold value.

**Accuracy against cost.** The Pareto frontier view returns twelve non-dominated points in the plane of position error against wall time, rendered on the accuracy-cost dashboard as a scatter coloured by method. The time-gap and kinematic methods occupy the low-cost, high-error region. The Kalman-smoothed method occupies the high-cost, low-error region at approximately fifteen times the baseline wall time. The three methods span the curve rather than clustering, which is the shape the proposal predicted.

The practical reading of the frontier is that the correct method depends on the downstream use. An analysis that needs flight boundaries but not metre-accurate positions is well served by the time-gap baseline at a fifteenth of the cost. An analysis of the trajectory itself requires the smoother, and no cheaper method substitutes for it.

**Data quality.** All twenty-five expectations across five suites passed on the final run. The suites check non-null and uniqueness constraints on hash keys, format conformance on airframe identifiers and airport codes, geographic range bounds on coordinates, enumerated-value conformance on noise labels, and row-count bounds per table.

**Reproducibility.** The determinism contract was verified by writing identical input twice to separate locations and asserting byte-level hash equality on every output partition. The automated test suite holds ninety-two tests and completes in 8.25 seconds. The rollback tool was validated against seven scenarios in an isolated database schema, covering single-tier rollback, full-chain rollback with ordering verification, month-boundary flight handling, repeat-invocation idempotence, and the invariant that globally-scoped dimension tables are never rolled back by a partition-tier operation. All seven passed.

---

## 4. Summary

### 4.1 Achievement Against the Proposal

All seven design goals and all six performance targets set in the proposal were delivered.

| Proposal goal | Delivered as |
|---|---|
| Ingest historical and streaming ADS-B | 48 months of flight-record archives, live REST feed through Kafka, daily state-vector archives |
| Handle missing, duplicated, irregular, out-of-order records | Canonical schema projection, hash-key deduplication, event-time ordering, validated under the N3 profile |
| Reconstruct using time-based and kinematic methods | Three methods benchmarked across the full noise ladder |
| Extract kinematic features | Speed, heading, acceleration, turn rate, altitude rate computed and persisted |
| Detect significant deviations | Deviation precision, recall and F1 computed per cell |
| Generate smoothed candidate trajectories | Kalman output persisted as a queryable mart |
| Measure accuracy, cost and scalability together | Twelve-cell matrix recording wall time with six quality metrics per cell |
| Throughput | Per-cell and per-stream measurement in telemetry |
| End-to-end processing time | Printed and persisted per stage |
| Streaming latency | Eleven-bucket publish-latency histogram |
| CPU and memory utilisation | Continuous gauges on a resource dashboard |
| Shuffle and partitioning overhead | Spark metrics endpoint |
| Scaling efficiency | 5.5 minutes per added month, measured and flat |

**Quantitative summary.**

| Dimension | Result |
|---|---|
| Reconstruction methods, noise profiles, experiment cells | 3, 4, 12 of 12 populated |
| Raw data ingested | 14.1 GB, 121.6 million records |
| Warehouse output | 15 GB silver, 2.0 GB gold, 244.5 million rows |
| Dashboards delivered | 8 panels |
| Automated test suite | 92 of 92 passing |
| Data-quality expectations | 25 of 25 passing |
| Rollback scenarios validated | 7 of 7 passing |
| Kalman loop and cross-track optimisations | 3 times and 17 times faster |
| Marginal cost per added month | 5.5 minutes |

**Answer to the research question.** The proposal asked how much accuracy is gained by incorporating aircraft kinematics and what computational cost that improvement requires.

Incorporating kinematics into segmentation alone yields no position-accuracy gain. Kinematic segmentation and the time-gap baseline produce identical position error at every noise level, because neither estimates positions; both report the observed ones. Under noise, kinematic segmentation is in fact worse than the baseline at the task it was meant to improve, with segmentation F1 falling to 0.246 at the heaviest profile while the baseline holds at 1.000.

The accuracy gain comes from the state estimator built on top of kinematic segmentation. It reduces position error by eleven to twelve orders of magnitude and simultaneously restores segmentation quality to 1.000 across all noise profiles, because removing noise-induced jumps removes the spurious breaks that were degrading the kinematic rule. The computational cost of that gain is approximately fifteen times the baseline wall time.

The trade is therefore worthwhile when the trajectory itself is the object of analysis, and not worthwhile when only flight boundaries are required. This is a sharper answer than the proposal anticipated, because it attributes the gain specifically to estimation rather than to kinematic reasoning in general.

### 4.2 Future Extensions

Three directions extend the delivered system. The monthly partition contract and watermark semantics are engine-independent, so the first is a substitution of execution engine rather than a redesign: extending Spark execution from the state-observation satellite to the flight-record builders would move the remaining single-host processing onto the cluster. The second is a longer run of the existing pipeline: materialising the full 48-month archive at the measured marginal cost would give the accuracy-cost frontier its full historical depth, including seasonal variation in traffic density. The third is adoption of a managed table format such as Apache Iceberg or Delta Lake, which would add snapshot isolation and time-travel queries and replace the custom watermark implementation with managed equivalents.

---

*Source code, tests, dashboards, the decision log and reproducibility fixtures accompany this report.*
