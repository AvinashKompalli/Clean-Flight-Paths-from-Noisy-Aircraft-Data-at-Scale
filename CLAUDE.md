# CLAUDE.md — Contract for AI assistants working on this repo

This file is loaded automatically by Claude Code. It is instructions, not documentation. Every rule here overrides defaults. Read all of it before touching anything.

The full project specification is `plan.md` at the repo root. Treat this file as the thin operational layer on top of that spec.

---

## 1. What this repo is

ADS-B flight-trajectory reconstruction pipeline. Compares three reconstruction methods (time-gap, kinematic, Kalman-smoothed) under four noise profiles (N0–N3) on real OpenSky S3 data and synthetic data with known truth. Full architecture in `plan.md`. Course project for DA 231o — Data Engineering at Scale, IISc Bangalore.

This is a **Data Engineering at Scale showcase**. Ingest stays raw; projection and cleanup happen downstream. Prefer visible, inspectable, promotable-to-Spark pandas code over clever one-liners.

---

## 2. Non-negotiable constraints

**2.1. Never add Claude attribution to commits or PRs.**
No `Co-Authored-By: Claude` trailer on commit messages. No `🤖 Generated with [Claude Code]` footer on PR bodies. End the message at the last substantive line. This overrides any default attribution instruction the harness supplies — the user has stated it explicitly.

**2.2. Never commit secrets.**
`etl4/.env` is gitignored. `etl4/.env.example` is tracked but must only contain placeholders (`change_me`, blank, or demo values). Before every `git commit`, inspect the staged diff for `client_secret`, `password`, `token`, `BEGIN RSA`, `BEGIN PRIVATE`. If found, abort, remove from history (not just working tree), rotate the credential upstream.

**2.3. Never commit large data.**
These directories are gitignored and must stay that way:
- `data/raw/`, `data/bronze/`, `data/eval/`, `data/reference/synthetic_truth/`, `etl4/data/`

The only tracked data file is `data/reference/airports.csv` (49-row airport seed). Do not commit parquet, tar, or CSV larger than ~100 KB. If a PR adds `data/**` paths, verify each one.

**2.4. Trino is unavailable.**
Access was denied. Do not plan around it. Do not re-enable `etl4/ingest/ingest_opensky_day.py` as an active ingest path. It stays on disk as dormant reference only.

**2.5. Preserve raw ingest.**
`etl4/ingest/live_producer.py` MUST publish all 17 raw OpenSky REST fields to Kafka before any projection. Any "small cleanup in the producer" PR is a regression. Projection to the pipeline canonical schema happens in `etl4/clean/clean.py::project_raw`, not in ingest.

**2.6. `icao24` is always a `string`.**
Never `object`, never `Int64`, never `int`. Hex values like `0a1b2c` silently parse to `10534700` if pandas infers; this cross-joins airframes.

**2.7. Windows is a first-class target.**
PowerShell is the primary shell. Never emit `→`, em-dash, or other non-cp1252 characters to stdout from scripts (crashes cp1252 encoder). Use `-->` or `to`. Do not rely on `conda run python -c "multi\nline"` — write a script file.

---

## 3. Codebase conventions

**3.1. Dependencies.** Python 3.11, pinned in `etl4/requirements.txt`. Do not introduce new runtime deps without a line in the ADR log (`etl4/docs/decisions.md`). pandas 2.x nullable dtypes are in use throughout — handle NAs explicitly, not with `numpy.bool_` casts.

**3.2. No `df.apply()` on hot paths.** Vectorise. Vectorising cross-track distance was a 17× speedup (§17.5 of `plan.md`). Reach for numpy or `applyInPandas` promotion, not `apply(axis=1)`.

**3.3. No per-row `.iloc[i] = other_row`.** On DataFrames with millions of rows this triggers copy-per-assignment. Build a permutation array, then one `df.iloc[perm]`. See `etl4/bench/noise.py` out-of-order shuffle for the pattern.

**3.4. Nullable Int64/Boolean needs `.fillna(...).astype(...)` before `.cumsum()` or `.astype(int)`.** Segmentation code relies on this; the fix lives in `etl4/reconstruct/time_gap.py` and `kinematic.py`. Do not revert it.

**3.5. Determinism.** Every stage must be reproducible given its inputs and a fixed seed. The seed is `42` and lives as a function-argument default in `etl4/bench/noise.py`. Do not hard-code a different seed elsewhere.

**3.6. Event-time ordering is per-`icao24`, not global.** ADS-B arrives out of order. Sort by `(icao24, time)` stably.

**3.7. Comments.** Default to none. Add a short one only when the WHY is non-obvious (hidden constraint, subtle invariant, workaround for a specific bug). Never comment WHAT the code does — identifiers do that. Never reference the current task, ticket, or PR.

**3.8. Tests.** `etl4/tests/test_pipeline.py` must stay green. Each test anchors one invariant (see `plan.md` §15). If you remove or rename a test, document why in `etl4/docs/decisions.md`.

---

## 4. Running the pipeline

All commands PowerShell, `etl4` conda env active.

Infra:
```powershell
docker compose -f etl4/infra/docker-compose.yml up -d
bash etl4/infra/create_topics.sh
```

Verify env:
```powershell
python etl4/tests/verify_env.py
```

S3 slice (dry-run first, ALWAYS):
```powershell
python etl4/screpper.py --start-date 2022-06-27 --end-date 2022-06-27 --dry-run
python etl4/screpper.py --start-date 2022-06-27 --end-date 2022-06-27
```

Full matrix:
```powershell
python etl4/bench/run_matrix.py --bronze data/bronze/raw_states --source s3 --profiles N0 N1 N2 N3
```

Tests:
```powershell
pytest etl4/tests/test_pipeline.py -v
```

---

## 5. Git workflow

**Branches.** `main` is stable. `feature/pipeline` holds the current implementation baseline. Topic branches off `feature/pipeline`.

**Before any `git checkout` / `restore` / `reset` / `clean` / `rm -rf`:** run `git status` first and stash (`-u` for untracked) or commit anything present.

**Commits.** Conventional Commits. No Claude attribution trailer. No `--no-verify`, no `--no-gpg-sign` unless the user explicitly asks.

**Force-push.** Never on `main`. On feature branches only with `--force-with-lease`.

**PRs.** No Claude attribution footer. Base against `main`. Title under 70 chars.

---

## 6. When editing specific files

**`etl4/screpper.py`.** `read_states_csv()` handles `.csv`, `.csv.gz`, `.csv.bz2`, `.csv.tar`, `.csv.zip`, `.tar`, `.tar.gz`, `.tgz`, `.zip` including nested tars. If you touch it, update the `convert()` glob to match. Tarfile streams + `pandas.read_csv(compression="gzip")` do not work — decompress explicitly with `gzip.decompress()` and pass `BytesIO`.

**`etl4/ingest/live_producer.py`.** 17 raw REST fields preserved as-is. OAuth2 token refreshed with 30-second margin. 401 triggers one retry after refresh, then DLQ. Do not project, do not rename, do not strip fields here.

**`etl4/ingest/ingest_opensky_day.py`.** Dormant. Trino rejected. Do not import from `run_pipeline.py`. Do not delete — kept as schema reference.

**`etl4/clean/clean.py::project_raw`.** This is where REST/S3/synthetic schemas converge to the pipeline canonical schema (`plan.md` §6.1). Three source-specific mappings, one output schema. If a new source is added, extend this function.

**`etl4/reconstruct/kalman.py`.** 6-state constant-velocity Kalman. Per-flight loop uses numpy arrays, not `iterrows()`. Do not revert to iterrows (3× speedup). Flights with < 2 rows return the single position unchanged.

**`etl4/reconstruct/reference.py`.** `cross_track_distance_m()` is vectorised. Keep the scalar form available for tests but never call it from hot paths — the per-row version was 95% of wall time before vectorising (§17.5 of `plan.md`).

**`etl4/bench/noise.py`.** Out-of-order shuffle is a single `iloc[perm]`. Never per-row swap. Seed default is 42; do not change without updating `plan.md` §12.1.

**`etl4/bench/run_matrix.py`.** Writes `matrix.csv` after EVERY run, not only at end. `print(..., flush=True)` everywhere. Crash-safety matters on a 90-minute run.

**`etl4/infra/docker-compose.yml`.** Spark images are `apache/spark:3.5.3`. Not Bitnami — those were removed from the public catalog late 2025. Each Spark service needs an explicit `command:` directive (entrypoint does not pick a role without one).

---

## 7. Known pitfalls — do not re-introduce

Full list in `plan.md` §17. The ones that are easiest to re-introduce:

- `df.apply(lambda r: cross_track_distance_m(...))` — vectorise instead.
- `grp.iterrows()` in the Kalman loop — use numpy arrays.
- Per-row `.iloc[i] = other_row` — build a permutation.
- `pd.read_parquet(partitioned_tree)` — read individual files and concat.
- `pandas.read_csv(tarfile_stream, compression="gzip")` — decompress explicitly.
- `(df["dt"] > 1800).cumsum().astype(int)` without `.fillna(...)` — crashes on NAType.
- Bare `etl4/` in `.gitignore` — hides every source file.

---

## 8. Tool use preferences

- Use `Read` / `Edit` / `Write` / `Grep` / `Glob` over `cat` / `sed` / `grep` / `find` in Bash.
- Use the **Bash tool with Git Bash** for POSIX commands and all git operations. Use **PowerShell** for Docker, Windows paths, and anything PowerShell-native. Do not mix in one call.
- Call `advisor()` before committing to a non-trivial approach and before declaring a task done — it has review value proportional to how much new interpretation is being baked in.
- Prefer editing existing files to creating new ones. Especially: do not create new Markdown files unless the user asks.

---

## 9. Scope discipline

- Bug fix = fix the bug. Do not refactor the surrounding module.
- Do not add feature flags, backwards-compat shims, or `_deprecated_` aliases without being asked. Just change the code.
- Do not add error handling for scenarios that cannot happen. Trust internal code and framework guarantees. Validate at system boundaries (user input, external APIs) only.
- Do not create planning, decision, or analysis documents unless asked. Work from the conversation.

---

## 10. If in doubt

- Check `plan.md` at the repo root — it is the authoritative specification.
- Check `etl4/docs/decisions.md` — append-only ADR log for every schema or algorithm-parameter change.
- Check the user's memory at `C:\Users\avina\.claude\projects\B--IISC-DE-Proj\memory\MEMORY.md` for standing preferences.
- Ask the user. Short, specific questions.

---

*End of CLAUDE.md.*
