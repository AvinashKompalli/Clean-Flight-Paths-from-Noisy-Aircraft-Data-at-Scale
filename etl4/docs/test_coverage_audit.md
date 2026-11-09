# Test Coverage Audit — `plan.md` §18 vs `etl4/tests/`

## 1. Intent

`plan.md` §18 is the conformance checklist; `etl4/tests/test_pipeline.py` + `test_smoke.py` are the enforcement. This file maps one to the other. Maintenance rule: §18 and §2 change together; a new test or a new edge case updates both sides in the same commit. Audit only — no new tests drafted here.

## 2. Coverage matrix

Verdicts: **covered** = a test exists whose assertion would fail if the invariant were violated; **partial** = a test exercises the code path but does not assert the specific invariant; **uncovered** = no test touches it.

### 2.1. Ingest

| Edge case (§18) | Test | Verdict |
|---|---|---|
| 17 raw REST fields preserved in order | — | uncovered |
| OAuth2 token refresh at 30-s margin | — | uncovered |
| 401 → one retry, then DLQ | — | uncovered |
| JSON serialisation failure → DLQ | — | uncovered |
| Replay `--speed 0` sleeps nothing | — | uncovered |
| `poll(0)` on every produce | — | uncovered |
| S3 download idempotent | — | uncovered |
| Dry-run prints total size | — | uncovered |
| `.csv.gz` decompressed explicitly | — | uncovered |
| Nested `.csv.tar` / `.csv.zip` handled | — | uncovered |

### 2.2. Clean

| Edge case | Test | Verdict |
|---|---|---|
| `icao24` is `string`, never `object`/`Int64` | pinned in `project_raw`, not asserted | partial |
| Missing lat/lon dropped, reason logged | `test_clean_removes_null_positions` | covered |
| Out-of-range lat/lon/alt rejected, distinct reasons | `test_clean_range_check_rejects_bad_lat` | partial |
| Dedup on `(icao24, time)`, keep first | `test_clean_drops_exact_duplicates` | partial |
| Position-jump flag set, row NOT dropped | `test_kinematic_breaks_on_position_jump` | partial |
| Rejected rows returned separately, same schema | `test_clean_*` tuple-unpack | partial |
| `BooleanDtype` on `onground` preserved | — | uncovered |

### 2.3. Order and features

| Edge case | Test | Verdict |
|---|---|---|
| Sort `(icao24, time)` stable | — | uncovered |
| Monotonicity check raises on inversion | — | uncovered |
| `dt == 0` on first row per `icao24` | `test_features_dt_is_positive_within_aircraft` | partial |
| Turn-rate wrapped ±180° before /dt | — | uncovered |
| Heading null → turn-rate null | — | uncovered |
| No `.apply()` per-row in features | — | uncovered |

### 2.4. Reconstruct

| Edge case | Test | Verdict |
|---|---|---|
| `.fillna(True).astype(bool)` before cumsum | — | uncovered |
| `.fillna(0).astype("int64")` before string | — | uncovered |
| Callsign break only on non-null change | — | uncovered |
| Kalman loop numpy, not iterrows | — | uncovered |
| Kalman ENU origin per-flight | — | uncovered |
| Kalman <2 rows handled | — | uncovered |
| `flight_id` = `<icao24>_<seg>` | `test_time_gap_assigns_flight_ids` | partial |

### 2.5. Reference and deviations

| Edge case | Test | Verdict |
|---|---|---|
| Cross-track vectorised, no `df.apply` | `test_cross_track_distance_zero_on_path` | partial |
| Airport >100 km from endpoint → skip | — | uncovered |
| Origin == destination → great-circle | — | uncovered |
| Deviations min-duration 60 s RLE filter | `test_deviation_detects_offset_segment` | partial |
| Residual detection only when Kalman present | same test (flag False) | partial |

### 2.6. Correction

| Edge case | Test | Verdict |
|---|---|---|
| Only rows above residual replaced | — | uncovered |
| Short-gap interp ≥10 s and <60 s only | — | uncovered |
| Longer gaps left unfilled | — | uncovered |
| Synthetic rows flagged, truth ignores them | — | uncovered |

### 2.7. Evaluation

| Edge case | Test | Verdict |
|---|---|---|
| RMSE nearest-in-time, not space | `test_position_rmse_zero_on_identical_frames` | partial |
| Seg-F1 uses 60-s tolerance | `test_segmentation_prf_perfect_match` | partial |
| Coverage ratio NaN on 0 truth rows | — | uncovered |
| Deviation F1 requires >50% overlap | — | uncovered |

### 2.8. Infrastructure

| Edge case | Test | Verdict |
|---|---|---|
| Kafka KRaft (no Zookeeper) | — | uncovered |
| Spark `apache/spark:3.5.3` | — | uncovered |
| Postgres schema pre-created | — | uncovered |
| `prometheus.yml` has producer + node-exporter | — | uncovered |

### 2.9. Experiment matrix

| Edge case | Test | Verdict |
|---|---|---|
| Writes `matrix.csv` after every cell | `test_run_experiment_time_gap_end_to_end` | partial |
| Noise uses `default_rng(seed=42)` | — | uncovered |
| Shuffle single `iloc[perm]` | — | uncovered |
| Duplicates after gap-drop | — | uncovered |
| Jumps `uniform(-2,2)` deg lat and lon | — | uncovered |

### 2.10. Tests and reproducibility

| Edge case | Test | Verdict |
|---|---|---|
| `pytest` green from fresh clone | the suite itself | covered |
| No pandas `PerformanceWarning`/`DeprecationWarning` | — | uncovered |
| `verify_env.py` matches `requirements.txt` | exists, not pytest-enforced | partial |
| Random seed used consistently | `test_kalman_converges_on_straight_line` seed 42 | partial |

**Totals**: covered **2**, partial **14**, uncovered **39** (55 items).

## 3. Gaps — rationale

- **Ingest (10U)**: producer/OAuth2/DLQ/downloader fail only at runtime; sketches — fake OAuth2 returns expired-then-valid to assert retry+DLQ; nested `.csv.tar` fed to `read_states_csv`; dry-run stdout parsed for byte total. New `test_ingest.py`.
- **Clean (1U, 5P)**: headline — `icao24` dtype not asserted; one `dtype == "string"` closes §0.6. `onground` nullable-bool uncovered. Tighten existing `test_clean_*`.
- **Order / features (1P, 5U)**: turn-rate ±180° wrap, null-heading, stable sort, monotonicity all unasserted — missing wrap inflates method B's flight count at polar crossings. Extend `test_features_*`.
- **Reconstruct (1P, 6U)**: two NAType guards (`.fillna(True).astype(bool)` and `.fillna(0).astype("int64")` before cumsum) untested — the exact crash from §17.4 and CLAUDE.md §3.4. Kalman <2-rows and ENU-origin also unasserted. `test_pipeline.py`.
- **Reference / deviations (3P, 2U)**: existing test fires on any event, so 60-s RLE filter unasserted; a 30-s excursion asserting zero events is one line. Airport >100 km skip unasserted.
- **Correction (4U)**: synthetic-row flag is the headline — without it, truth RMSE includes interpolated rows and §19 headline biases. Short-gap boundary at 10-60 s unasserted. New `test_correction.py`.
- **Evaluation (2P, 2U)**: existing RMSE test uses identical frames and never exercises nearest-in-time; coverage-ratio NaN-on-zero-truth unasserted. `test_pipeline.py`.
- **Infrastructure (4U)**: compose-file invariants (`KAFKA_PROCESS_ROLES`, `apache/spark:3.5.3`) belong in a YAML parse test. New `test_infra_static.py`.
- **Experiment matrix (1P, 4U)**: crash-safety write-after-every-cell is load-bearing — a regression wastes a 90-minute run; test kills runner after cell 1, asserts one-row `matrix.csv`. Noise seed, shuffle pattern also unasserted.
- **Reproducibility (1C, 2P, 1U)**: `verify_env.py` not pytest-invoked — two lines flip it to covered. `pytest.ini` warning filter catches pandas regressions.

## 4. Priority ranking — top 5 gaps

1. **Experiment matrix — `matrix.csv` after every cell** (§2.9). A regression wastes a full 90-minute run on any mid-run crash; CLAUDE.md §6 flags this explicitly. Two-line pytest.
2. **Reconstruct — NAType guards before `cumsum`** (§2.4). Regression surfaces as a `TypeError` deep in segmentation under N1-N3 — matrix cell fails silently. `plan.md` §17.4 and CLAUDE.md §3.4 call this out; no test guards it.
3. **Clean — `icao24` is `string` everywhere** (§2.2). The §0.6 cross-join invariant. One `dtype == "string"` assertion per existing `test_clean_*` closes the worst silent-failure mode in the project.
4. **Reference / deviations — 60 s run-length filter** (§2.5). Without it, deviation count becomes noise-sensitive and §19's deviation-F1 acceptance criterion loses meaning.
5. **Correction — synthetic-row flag** (§2.6). Without it, synthetic-truth RMSE in `final_report.md` §8 is biased low; must be fixed before the headline accuracy can be defended at viva.

## 5. Appendix — existing tests inventory

- `test_clean_removes_null_positions` → null lat/lon → `reason == "null_key_or_position"`.
- `test_clean_drops_exact_duplicates` → cleaned length matches dedup on `(icao24,time,lat,lon)`.
- `test_clean_range_check_rejects_bad_lat` → lat=95 → `reason == "out_of_range"`.
- `test_features_dt_is_positive_within_aircraft` → per-aircraft min `dt` ≥ 0.
- `test_features_speed_est_near_broadcast_velocity` → median rel-err `speed_est` vs `velocity` < 0.1.
- `test_time_gap_assigns_flight_ids` → `flight_id` column exists, non-null.
- `test_time_gap_breaks_on_long_gap` → hand-crafted gap → two `flight_id` values.
- `test_kinematic_breaks_on_position_jump` → pre-flagged jump → two `flight_id` values.
- `test_kalman_converges_on_straight_line` → smoothed mean-error from row 10 < 100 m.
- `test_great_circle_endpoints_preserved` → endpoints within 0.01°.
- `test_cross_track_distance_zero_on_path` → on-path point has cross-track < 100 m.
- `test_deviation_detects_offset_segment` → offset flight produces ≥ 1 event.
- `test_segmentation_prf_perfect_match` → identical boundaries → P=R=F1=1.0.
- `test_position_rmse_zero_on_identical_frames` → self-RMSE < 1.0 m.
- `test_continuity_ratio_perfect_on_regular_steps` → perfect 10-s cadence → continuity > 0.95.
- `test_run_experiment_time_gap_end_to_end` → runner returns `rows_out`, `n_flights`, `wall_s`.
- `test_synthetic_generates_valid_bronze` (`test_smoke.py`) → parquet has required columns, 20 aircraft, lat in [-90,90].
