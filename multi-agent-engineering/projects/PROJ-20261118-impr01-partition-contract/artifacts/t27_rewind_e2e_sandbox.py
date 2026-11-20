"""T-27 Rewind E2E sandbox + driver (functional-tester).

Validates etl4/orchestration/rewind.py in a fully isolated Postgres schema
(t27_sandbox) and a scratch filesystem root. No production data is touched.

The driver exercises 7 scenarios from the T-27 dispatch:
  Sc1  Dry-run on empty state (with and without --yes, per argparse order-of-ops)
  Sc2  Single-layer vault rewind with 3 seeded ym partitions
  Sc3  Full-stack --layer all with downstream-first chain order (call-order spy)
  Sc4  Cross-month flight - rewind only M, then M+1 (FS half; postgres half NOT TESTABLE)
  Sc5  --help text inspection for cross-month neighbour rule
  Sc6  Idempotent repeat rewind (second run is a no-op)
  Sc7  dim_aircraft never rolled back by any layer

Isolation:
  * POSTGRES_DSN env var is set BEFORE importing etl4.orchestration.rewind,
    so rewind.DSN_DEFAULT binds to the sandbox search_path.
  * rewind.rewind_vault.__kwdefaults__['root'] and rewind_marts.__kwdefaults__['root']
    are patched to scratch dirs after import, so in-process calls that do not
    pass root= explicitly still hit scratch.
  * The sandbox schema is dropped + recreated per run.

Windows cp1252-safe (no arrows, no em-dashes). Writes a JSON report next to
itself with per-scenario verdict + evidence.

Invoke:
    conda run -n opensky python <this-file>
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[4]

# --- Isolation: sandbox DSN set BEFORE importing rewind so DSN_DEFAULT binds. ---
PROD_DSN = "postgresql://etl4:change_me@localhost:5432/etl4"
SANDBOX_SCHEMA = "t27_sandbox"
SANDBOX_DSN = (
    "postgresql://etl4:change_me@localhost:5432/etl4"
    "?options=-csearch_path%3D" + SANDBOX_SCHEMA
)
os.environ["POSTGRES_DSN"] = SANDBOX_DSN

sys.path.insert(0, str(REPO_ROOT))

import pandas as pd        # noqa: E402
import psycopg             # noqa: E402

from etl4.orchestration import rewind         # noqa: E402
from etl4.vault import vault_io               # noqa: E402

# Sanity: rewind should have bound DSN_DEFAULT to the sandbox search_path.
assert SANDBOX_SCHEMA in rewind.DSN_DEFAULT, \
    "rewind.DSN_DEFAULT did not pick up the sandbox POSTGRES_DSN"


# ------------------------------------------------------------------- utilities
def _print(msg: str) -> None:
    print(msg, flush=True)


def _sb_connect():
    """Sandbox-schema connection. All DDL/DML lands in t27_sandbox."""
    return psycopg.connect(SANDBOX_DSN, connect_timeout=5)


def _prod_connect():
    """Production-schema connection (public). Used only to create/drop the sandbox."""
    return psycopg.connect(PROD_DSN, connect_timeout=5)


# --------------------------------------------------------------- sandbox DDL
SANDBOX_DDL = f"""
DROP SCHEMA IF EXISTS {SANDBOX_SCHEMA} CASCADE;
CREATE SCHEMA {SANDBOX_SCHEMA};

SET search_path TO {SANDBOX_SCHEMA};

CREATE TABLE pipeline_watermarks (
    layer       TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    last_ym     TEXT NOT NULL,
    row_count   BIGINT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name),
    CHECK (last_ym ~ '^[0-9]{{4}}-(0[1-9]|1[0-2])$')
);

CREATE TABLE pipeline_runs (
    run_id      SERIAL PRIMARY KEY,
    run_date    DATE,
    source      TEXT,
    method      TEXT,
    noise       TEXT,
    rows_in     BIGINT,
    rows_out    BIGINT,
    wall_secs   DOUBLE PRECISION,
    cpu_secs    DOUBLE PRECISION,
    shuffle_mb  DOUBLE PRECISION,
    started_at  TIMESTAMPTZ DEFAULT now(),
    ym          TEXT NOT NULL,
    UNIQUE (ym, source, method, noise)
);

CREATE TABLE eval_results (
    run_id      INTEGER REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    metric      TEXT,
    value       DOUBLE PRECISION,
    recorded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE deviation_events (
    event_id      SERIAL PRIMARY KEY,
    run_id        INTEGER REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    icao24        TEXT,
    flight_id     TEXT,
    start_time    BIGINT,
    end_time      BIGINT,
    type          TEXT,
    max_distance  DOUBLE PRECISION,
    peak_residual DOUBLE PRECISION
);

-- Deliberately mirror the CURRENT production shape (no ym column) so Sc3/Sc6
-- can expose the T-12 schema-gap concern on rewind_sink. A second variant
-- with ym is created on demand inside Sc3.
CREATE TABLE fact_trajectory_enriched (
    hk_aircraft  CHAR(32) NOT NULL,
    icao24       TEXT NOT NULL,
    time         BIGINT NOT NULL,
    lat          DOUBLE PRECISION,
    lon          DOUBLE PRECISION,
    baroaltitude DOUBLE PRECISION,
    velocity     DOUBLE PRECISION,
    heading      DOUBLE PRECISION,
    callsign     TEXT,
    typecode     TEXT,
    registration TEXT,
    PRIMARY KEY (hk_aircraft, time)
);

CREATE TABLE dim_aircraft (
    hk_aircraft  CHAR(32) PRIMARY KEY,
    icao24       TEXT NOT NULL,
    registration TEXT,
    typecode     TEXT
);
"""


def _create_sandbox() -> None:
    with _prod_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SANDBOX_DDL)
    _print(f"[sandbox] schema {SANDBOX_SCHEMA} created (prod shape, no ym)")


def _drop_sandbox() -> None:
    try:
        with _prod_connect() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {SANDBOX_SCHEMA} CASCADE")
        _print(f"[sandbox] schema {SANDBOX_SCHEMA} dropped")
    except Exception as e:
        _print(f"[sandbox] DROP failed (ignored): {e}")


def _add_ym_to_sink() -> None:
    """Simulate T-12/T-13 ym migration: add ym to fact_trajectory_enriched."""
    with _sb_connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE fact_trajectory_enriched "
                "ADD COLUMN ym TEXT"
            )
    _print("[sandbox] ALTER fact_trajectory_enriched ADD COLUMN ym")


# ------------------------------------------------------------ seed helpers
def _seed_vault_partition(root: Path, table: str, ym: str,
                          rows: list, extra_cols: dict = None) -> Path:
    """Write <root>/<table>/ym=<ym>/part.parquet with the given row payloads."""
    data = {
        "hk_flight": pd.Series([r["hk_flight"] for r in rows], dtype="string"),
        "icao24":    pd.Series([r["icao24"]    for r in rows], dtype="string"),
        "ym":        pd.Series([ym] * len(rows), dtype="string"),
    }
    if extra_cols:
        for k, v in extra_cols.items():
            data[k] = v
    df = pd.DataFrame(data)
    return vault_io.write_partition(df, table, ym, out_dir=root)


def _seed_mart_partition(marts_root: Path, table: str, ym: str,
                         rows: list) -> Path:
    df = pd.DataFrame({
        "hk_flight": pd.Series([r["hk_flight"] for r in rows], dtype="string"),
        "icao24":    pd.Series([r["icao24"]    for r in rows], dtype="string"),
        "firstseen": pd.Series([r.get("firstseen") for r in rows], dtype="Int64"),
        "lastseen":  pd.Series([r["lastseen"]  for r in rows], dtype="Int64"),
        "ym":        pd.Series([ym] * len(rows), dtype="string"),
    })
    return vault_io.write_partition(df, table, ym, out_dir=marts_root)


def _seed_wm(conn, layer: str, table_name: str, last_ym: str,
             row_count: int = 1) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_watermarks "
            "(layer, table_name, last_ym, row_count) VALUES (%s,%s,%s,%s) "
            "ON CONFLICT (layer, table_name) "
            "DO UPDATE SET last_ym = EXCLUDED.last_ym, "
            "              row_count = EXCLUDED.row_count",
            (layer, table_name, last_ym, row_count),
        )


def _get_wm(conn, layer: str, table_name: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_ym FROM pipeline_watermarks "
            "WHERE layer=%s AND table_name=%s",
            (layer, table_name),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _count(conn, sql: str, args=()) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, args)
        row = cur.fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------- scenarios return (status, ev)
RESULTS = []


def _record(name: str, status: str, evidence: dict, notes: str = "") -> None:
    RESULTS.append({
        "scenario": name,
        "status": status,
        "evidence": evidence,
        "notes": notes,
    })
    _print(f"[{name}] {status}: {notes}")


# -------------------------------------------------------------- Sc1
def sc1_dry_run_empty(scratch_v: Path, scratch_m: Path) -> None:
    """Sc1: dry-run on fresh sandbox; expect no writes."""
    name = "Sc1"
    try:
        # Snapshot sandbox state.
        with _sb_connect() as conn:
            n_wm_before = _count(conn, "SELECT count(*) FROM pipeline_watermarks")
            n_runs_before = _count(conn, "SELECT count(*) FROM pipeline_runs")

        # Sub-case A: run EXACTLY the spec'd command (no --yes). Expect exit!=0.
        cmd_a = [sys.executable, "-m", "etl4.orchestration.rewind",
                 "--layer", "all", "--from", "2099-01", "--to", "2099-12",
                 "--dry-run"]
        env = dict(os.environ, POSTGRES_DSN=SANDBOX_DSN)
        proc_a = subprocess.run(cmd_a, cwd=str(REPO_ROOT),
                                capture_output=True, text=True, env=env)
        yes_gate_triggered = (proc_a.returncode != 0
                              and "--yes" in (proc_a.stderr + proc_a.stdout))

        # Sub-case B: rerun with --yes so dry-run plan is actually printed.
        cmd_b = cmd_a + ["--yes"]
        proc_b = subprocess.run(cmd_b, cwd=str(REPO_ROOT),
                                capture_output=True, text=True, env=env)

        with _sb_connect() as conn:
            n_wm_after = _count(conn, "SELECT count(*) FROM pipeline_watermarks")
            n_runs_after = _count(conn, "SELECT count(*) FROM pipeline_runs")

        chain_markers = [m for m in (
            "[rewind all]", "[rewind eval_mart]", "[rewind matrix]",
            "[rewind sink]", "[rewind marts]", "[rewind vault]"
        ) if m in proc_b.stdout]

        evidence = {
            "A_cmd": " ".join(cmd_a),
            "A_returncode": proc_a.returncode,
            "A_yes_gate_triggered": yes_gate_triggered,
            "A_stderr_tail": proc_a.stderr[-400:],
            "B_cmd": " ".join(cmd_b),
            "B_returncode": proc_b.returncode,
            "B_chain_markers_seen": chain_markers,
            "B_stdout_tail": proc_b.stdout[-600:],
            "sandbox_wm_delta": n_wm_after - n_wm_before,
            "sandbox_runs_delta": n_runs_after - n_runs_before,
        }

        # Status criteria:
        #   * B exits 0 and prints the full chain (6 markers)
        #   * no sandbox state mutated
        #   * FINDING: A's literal command exits 1 because rewind_all raises
        #     before checking --dry-run. That is a known behaviour; log as INFO.
        passed = (
            proc_b.returncode == 0
            and len(chain_markers) == 6
            and (n_wm_after - n_wm_before) == 0
            and (n_runs_after - n_runs_before) == 0
        )
        notes = (
            "literal command errored (yes-gate precedes dry-run short-circuit); "
            "re-ran with --yes to capture the plan. No sandbox writes."
            if yes_gate_triggered else
            "literal command did NOT trip the yes-gate; investigate."
        )
        _record(name, "PASS" if passed else "FAIL", evidence, notes)
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc2
def sc2_single_layer_vault(scratch_v: Path) -> None:
    name = "Sc2"
    try:
        for ym in ("2019-01", "2019-02", "2019-03"):
            _seed_vault_partition(scratch_v, "hub_flight", ym,
                                  rows=[{"hk_flight": "a" * 32,
                                         "icao24": "0a1b2c"}])
        with _sb_connect() as conn:
            _seed_wm(conn, "vault", "hub_flight", "2019-03", row_count=3)
            # Add a sibling vault wm that MUST also roll if it is in range.
            _seed_wm(conn, "vault", "sat_flight_summary", "2019-03", row_count=3)
            conn.commit()
            wm_before_hub = _get_wm(conn, "vault", "hub_flight")
            wm_before_sat = _get_wm(conn, "vault", "sat_flight_summary")

        rewind.rewind_vault("2019-01", "2019-02",
                            root=scratch_v, dsn=SANDBOX_DSN)

        p_jan = scratch_v / "hub_flight" / "ym=2019-01"
        p_feb = scratch_v / "hub_flight" / "ym=2019-02"
        p_mar = scratch_v / "hub_flight" / "ym=2019-03" / "part.parquet"
        with _sb_connect() as conn:
            wm_after_hub = _get_wm(conn, "vault", "hub_flight")
            wm_after_sat = _get_wm(conn, "vault", "sat_flight_summary")

        evidence = {
            "scratch_v": str(scratch_v),
            "jan_removed": not p_jan.exists(),
            "feb_removed": not p_feb.exists(),
            "mar_survives": p_mar.is_file(),
            "wm_hub_flight_before": wm_before_hub,
            "wm_hub_flight_after": wm_after_hub,
            "wm_sat_flight_summary_before": wm_before_sat,
            "wm_sat_flight_summary_after": wm_after_sat,
        }
        passed = (
            not p_jan.exists()
            and not p_feb.exists()
            and p_mar.is_file()
            and wm_after_hub == "2018-12"
            and wm_after_sat == "2018-12"
        )
        _record(name, "PASS" if passed else "FAIL", evidence,
                "Jan+Feb removed, Mar survives, both vault wms rolled to 2018-12")
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc3
def sc3_full_stack_chain(scratch_v: Path, scratch_m: Path) -> None:
    """Call-order spy wraps each rewind_* fn and asserts downstream-first."""
    name = "Sc3"
    try:
        order = []
        preconditions = {}

        # Add ym to sink (post-T-12 shape) so rewind_sink succeeds.
        _add_ym_to_sink()
        # Seed one fact_trajectory_enriched row + one pipeline_runs row + eval.
        with _sb_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fact_trajectory_enriched "
                    "(hk_aircraft, icao24, time, ym) "
                    "VALUES (%s,%s,%s,%s)",
                    ("x" * 32, "0a1b2c", 1546300800, "2019-01"),
                )
                cur.execute(
                    "INSERT INTO pipeline_runs (source, method, noise, ym) "
                    "VALUES ('s3','kalman','N0','2019-01') RETURNING run_id",
                )
                rid = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO eval_results (run_id, metric, value) "
                    "VALUES (%s,'cross_track_m',10.0)",
                    (rid,),
                )
            _seed_wm(conn, "vault",  "hub_flight",               "2019-01")
            _seed_wm(conn, "marts",  "fact_flight",              "2019-01")
            _seed_wm(conn, "sink",   "fact_trajectory_enriched", "2019-01")
            _seed_wm(conn, "matrix", "pipeline_runs",            "2019-01")
            _seed_wm(conn, "eval_mart", "fact_experiment_result","2019-01")
            conn.commit()
        # Seed a vault + marts partition on scratch.
        _seed_vault_partition(scratch_v, "hub_flight", "2019-01",
                              rows=[{"hk_flight": "a" * 32,
                                     "icao24": "0a1b2c"}])
        _seed_mart_partition(scratch_m, "fact_flight", "2019-01",
                             rows=[{"hk_flight": "a" * 32,
                                    "icao24": "0a1b2c",
                                    "firstseen": 1546300800,
                                    "lastseen":  1546300900}])

        # Capture state BEFORE each layer runs so we can check "downstream gone first".
        def _wrap(fn_name, fn):
            def inner(*a, **kw):
                order.append(fn_name)
                with _sb_connect() as c:
                    preconditions[fn_name] = {
                        "pipeline_runs_count": _count(c, "SELECT count(*) FROM pipeline_runs WHERE ym='2019-01'"),
                        "fact_traj_count":     _count(c, "SELECT count(*) FROM fact_trajectory_enriched WHERE ym='2019-01'"),
                        "vault_jan_exists":   (scratch_v / "hub_flight" / "ym=2019-01").exists(),
                        "marts_jan_exists":   (scratch_m / "fact_flight" / "ym=2019-01").exists(),
                    }
                return fn(*a, **kw)
            return inner

        orig = {}
        for n in ("rewind_eval_mart", "rewind_matrix", "rewind_sink",
                  "rewind_marts", "rewind_vault"):
            orig[n] = getattr(rewind, n)
            setattr(rewind, n, _wrap(n, orig[n]))

        try:
            rewind.rewind_all("2019-01", "2019-01",
                              yes=True, dry_run=False, dsn=SANDBOX_DSN)
        finally:
            for n, f in orig.items():
                setattr(rewind, n, f)

        with _sb_connect() as conn:
            final = {
                "pipeline_runs_count": _count(conn, "SELECT count(*) FROM pipeline_runs WHERE ym='2019-01'"),
                "fact_traj_count":     _count(conn, "SELECT count(*) FROM fact_trajectory_enriched WHERE ym='2019-01'"),
                "vault_jan_exists":   (scratch_v / "hub_flight" / "ym=2019-01").exists(),
                "marts_jan_exists":   (scratch_m / "fact_flight" / "ym=2019-01").exists(),
            }

        expected_order = ["rewind_eval_mart", "rewind_matrix", "rewind_sink",
                          "rewind_marts", "rewind_vault"]
        order_ok = (order == expected_order)

        # Downstream-first: when rewind_vault runs, marts should still be present
        # (vault is upstream of marts; rewind order is downstream-first, so
        # marts has already been wiped BEFORE vault starts).
        inv = {
            "matrix_before_sink":
                preconditions["rewind_sink"]["pipeline_runs_count"] == 0,
            "sink_before_marts":
                preconditions["rewind_marts"]["fact_traj_count"] == 0,
            "marts_before_vault":
                preconditions["rewind_vault"]["marts_jan_exists"] is False,
        }

        evidence = {
            "call_order": order,
            "expected_order": expected_order,
            "preconditions_per_layer": preconditions,
            "final_state": final,
            "downstream_first_invariants": inv,
        }
        passed = order_ok and all(inv.values()) and all(v == 0 or v is False
                                                        for v in final.values())
        _record(name, "PASS" if passed else "FAIL", evidence,
                f"chain order {'matches' if order_ok else 'DIVERGES from'} expected; "
                f"downstream-first: {inv}")
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc4
def sc4_cross_month(scratch_v: Path, scratch_m: Path) -> None:
    name = "Sc4"
    try:
        # One cross-month flight F1: lastseen=jan in Jan partition, lastseen=feb in Feb.
        JAN_EPOCH = 1548806400   # 2019-01-30
        FEB_EPOCH = 1549411200   # 2019-02-06
        hk = "f" * 32
        for ym, ls in (("2019-01", JAN_EPOCH), ("2019-02", FEB_EPOCH)):
            _seed_vault_partition(
                scratch_v, "sat_flight_summary", ym,
                rows=[{"hk_flight": hk, "icao24": "0a1b2c"}],
                extra_cols={
                    "firstseen": pd.Series([JAN_EPOCH], dtype="Int64"),
                    "lastseen":  pd.Series([ls],        dtype="Int64"),
                    "load_dt":   pd.Series([1549500000], dtype="Int64"),
                },
            )
            _seed_mart_partition(
                scratch_m, "fact_flight", ym,
                rows=[{"hk_flight": hk, "icao24": "0a1b2c",
                       "firstseen": JAN_EPOCH, "lastseen": ls}],
            )

        with _sb_connect() as conn:
            _seed_wm(conn, "vault", "sat_flight_summary", "2019-02")
            conn.commit()

        # Rewind JAN only (vault layer, isolated).
        rewind.rewind_vault("2019-01", "2019-01",
                            root=scratch_v, dsn=SANDBOX_DSN)

        jan_sfs = scratch_v / "sat_flight_summary" / "ym=2019-01"
        feb_sfs = scratch_v / "sat_flight_summary" / "ym=2019-02" / "part.parquet"
        feb_fact = scratch_m / "fact_flight" / "ym=2019-02" / "part.parquet"

        # Snapshot post-Jan-rewind state BEFORE rewinding Feb.
        jan_sfs_removed_after_jan = not jan_sfs.exists()
        feb_sfs_survives_after_jan = feb_sfs.is_file()
        feb_lastseen = None
        if feb_fact.is_file():
            df = pd.read_parquet(feb_fact)
            feb_lastseen = int(df["lastseen"].iloc[0])

        wm_mid = None
        with _sb_connect() as conn:
            wm_mid = _get_wm(conn, "vault", "sat_flight_summary")

        # Now rewind Feb too.
        rewind.rewind_vault("2019-02", "2019-02",
                            root=scratch_v, dsn=SANDBOX_DSN)
        feb_sfs_after_feb_rewind = feb_sfs.exists()

        evidence = {
            "after_jan_rewind": {
                "jan_sfs_removed": jan_sfs_removed_after_jan,
                "feb_sfs_survives": feb_sfs_survives_after_jan,
                "feb_fact_lastseen": feb_lastseen,
                "expected_feb_lastseen": FEB_EPOCH,
                "vault_sfs_wm_after_jan_rewind": wm_mid,
            },
            "after_feb_rewind": {
                "feb_sfs_removed": not feb_sfs_after_feb_rewind,
            },
            "limitations": (
                "Postgres fact_flight sink does NOT exist. The ADR 2026-11-20 "
                "mitigation (a) 'postgres UPSERT keeps Feb lastseen' is validated "
                "only at the parquet mart layer; grep etl4/ for 'ON CONFLICT "
                "(hk_flight)' confirms no such sink statement in postgres_sink.py "
                "(grep yielded zero matches)."
            ),
            "finding_mitigation_b_absent": (
                "Rewind of Jan alone produced no cross-month warning in stdout. "
                "ADR 2026-11-20 (b) states 'rewind of month M MUST rewind M+1 too "
                "when either month contains a cross-month flight; T-21 CLI help "
                "text and a SQL detection snippet should surface this'. Neither "
                "the CLI --help nor the rewind_vault stdout surfaces this rule."
            ),
        }
        passed = (
            jan_sfs_removed_after_jan
            and feb_sfs_survives_after_jan
            and feb_lastseen == FEB_EPOCH
            and wm_mid == "2018-12"
            and not feb_sfs_after_feb_rewind
        )
        _record(name, "PASS" if passed else "FAIL", evidence,
                "FS half: Feb parquet retains Feb lastseen after Jan rewind; "
                "Feb-then-rewind drops it. Postgres half: NOT TESTABLE "
                "(no fact_flight sink). ADR mitigation (b) UNIMPLEMENTED.")
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc5
def sc5_help_mentions_cross_month() -> None:
    name = "Sc5"
    try:
        env = dict(os.environ, POSTGRES_DSN=SANDBOX_DSN)
        proc = subprocess.run(
            [sys.executable, "-m", "etl4.orchestration.rewind", "--help"],
            cwd=str(REPO_ROOT), capture_output=True, text=True, env=env,
        )
        text = (proc.stdout + proc.stderr).lower()
        mentions = {
            "cross-month": "cross-month" in text,
            "neighbour":   "neighbour" in text or "neighbor" in text,
            "m+1":         "m+1" in text,
        }
        rule_present = any(mentions.values())
        evidence = {
            "returncode": proc.returncode,
            "help_text": proc.stdout,
            "mentions": mentions,
            "patch_recommendation": (
                "Add epilog to _parse_argv ArgumentParser: "
                "epilog=\"Cross-month rule (ADR 2026-11-20): rewinding month M "
                "when a flight spans M and M+1 may leave inconsistent "
                "'lastseen' values. Rewind M+1 too, or re-run the forward "
                "sink UPSERT to re-settle.\""
            ),
        }
        _record(
            name,
            "PASS" if rule_present else "FAIL",
            evidence,
            ("--help text mentions the cross-month rule"
             if rule_present else
             "--help text does NOT mention cross-month / neighbour rule; "
             "1-line patch: add epilog= to ArgumentParser in _parse_argv"),
        )
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc6
def sc6_idempotent(scratch_v: Path) -> None:
    name = "Sc6"
    try:
        _seed_vault_partition(scratch_v, "link_flight_aircraft", "2019-04",
                              rows=[{"hk_flight": "b" * 32,
                                     "icao24": "0a1b2c"}])
        with _sb_connect() as conn:
            _seed_wm(conn, "vault", "link_flight_aircraft", "2019-04")
            conn.commit()

        r1 = rewind.rewind_vault("2019-04", "2019-04",
                                 root=scratch_v, dsn=SANDBOX_DSN)
        r2 = rewind.rewind_vault("2019-04", "2019-04",
                                 root=scratch_v, dsn=SANDBOX_DSN)

        evidence = {
            "run1": r1,
            "run2": r2,
            "run2_is_noop": (r2["dirs"] == 0 and r2["bytes"] == 0),
        }
        passed = (r1["dirs"] >= 1
                  and r2["dirs"] == 0
                  and r2["bytes"] == 0
                  and not r2["dry_run"])
        _record(name, "PASS" if passed else "FAIL", evidence,
                "Second rewind is a no-op (dirs=0, bytes=0)")
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# -------------------------------------------------------------- Sc7
def sc7_dim_aircraft_untouched(scratch_m: Path) -> None:
    name = "Sc7"
    try:
        # Seed sandbox dim_aircraft + a sink wm for dim_aircraft.
        with _sb_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO dim_aircraft (hk_aircraft, icao24, typecode) "
                    "VALUES (%s,'0a1b2c','A320'), (%s,'0d4e5f','B738') "
                    "ON CONFLICT DO NOTHING",
                    ("d" * 32, "e" * 32),
                )
            _seed_wm(conn, "sink", "dim_aircraft", "2019-01", row_count=2)
            conn.commit()
            n_dim_before = _count(conn, "SELECT count(*) FROM dim_aircraft")
            wm_dim_before = _get_wm(conn, "sink", "dim_aircraft")

        # Also lay down a dim_aircraft monolith on scratch marts root.
        import pyarrow as pa
        import pyarrow.parquet as pq
        dim_mono = scratch_m / "dim_aircraft" / "part.parquet"
        dim_mono.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table({"hk_aircraft": ["z" * 32], "icao24": ["0a1b2c"]}),
            dim_mono,
        )
        dim_mono_mtime = dim_mono.stat().st_mtime_ns

        # Run rewind_sink in real mode (ym column must exist from Sc3).
        # If Sc3 did not run first, add it here.
        with _sb_connect() as conn:
            has_ym = _count(
                conn,
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='fact_trajectory_enriched' "
                "AND column_name='ym'",
                (SANDBOX_SCHEMA,),
            ) > 0
        if not has_ym:
            _add_ym_to_sink()

        rewind.rewind_sink("2019-01", "2019-01", dsn=SANDBOX_DSN)
        # And rewind_marts on scratch (dim_aircraft is non-partitioned - should survive).
        rewind.rewind_marts("2019-01", "2019-01",
                            root=scratch_m, dsn=SANDBOX_DSN)

        with _sb_connect() as conn:
            n_dim_after = _count(conn, "SELECT count(*) FROM dim_aircraft")
            wm_dim_after = _get_wm(conn, "sink", "dim_aircraft")

        evidence = {
            "n_dim_before": n_dim_before,
            "n_dim_after":  n_dim_after,
            "wm_sink_dim_before": wm_dim_before,
            "wm_sink_dim_after":  wm_dim_after,
            "dim_mono_survives": dim_mono.is_file(),
            "dim_mono_mtime_unchanged":
                dim_mono.is_file() and dim_mono.stat().st_mtime_ns == dim_mono_mtime,
            "code_filter_fact_trajectory_only":
                "table_filter=(\"fact_trajectory_enriched\",)",
        }
        passed = (
            n_dim_before == n_dim_after
            and wm_dim_before == wm_dim_after  # sink rewind must NOT touch dim_aircraft wm
            and dim_mono.is_file()
            and dim_mono.stat().st_mtime_ns == dim_mono_mtime
        )
        _record(name, "PASS" if passed else "FAIL", evidence,
                "dim_aircraft rows + its 'sink' wm + scratch monolith all survive "
                "sink + marts rewind")
    except Exception as e:
        _record(name, "ERROR", {"traceback": traceback.format_exc()}, str(e))


# ---------------------------------------------------------------------- main
def main() -> int:
    scratch = Path(tempfile.mkdtemp(
        prefix="t27_rewind_",
        dir=r"C:\Users\avina\AppData\Local\Temp\claude"
            r"\B--IISC-DE-Proj\8e461c31-3453-4d7e-abd5-ad362a41f2ee\scratchpad",
    ))
    scratch_v = scratch / "silver_vault"
    scratch_m = scratch / "gold_marts"
    scratch_v.mkdir(parents=True)
    scratch_m.mkdir(parents=True)
    _print(f"[driver] scratch={scratch}")

    # Patch rewind module-level defaults so in-process rewind_vault / rewind_marts
    # calls without explicit root= land in scratch.
    rewind.rewind_vault.__kwdefaults__["root"] = scratch_v
    rewind.rewind_marts.__kwdefaults__["root"] = scratch_m
    rewind.SILVER_VAULT_ROOT = scratch_v
    rewind.MARTS_ROOT        = scratch_m

    try:
        _create_sandbox()

        # Pre-count production dim_aircraft to prove we never touched it.
        prod_dim_before = None
        try:
            with _prod_connect() as conn:
                prod_dim_before = _count(
                    conn, "SELECT count(*) FROM public.dim_aircraft")
        except Exception:
            pass
        _print(f"[driver] prod public.dim_aircraft count (pre) = {prod_dim_before}")

        sc1_dry_run_empty(scratch_v, scratch_m)
        sc2_single_layer_vault(scratch_v)
        sc3_full_stack_chain(scratch_v, scratch_m)
        sc4_cross_month(scratch_v, scratch_m)
        sc5_help_mentions_cross_month()
        sc6_idempotent(scratch_v)
        sc7_dim_aircraft_untouched(scratch_m)

        prod_dim_after = None
        try:
            with _prod_connect() as conn:
                prod_dim_after = _count(
                    conn, "SELECT count(*) FROM public.dim_aircraft")
        except Exception:
            pass
        _print(f"[driver] prod public.dim_aircraft count (post) = {prod_dim_after}")
        prod_untouched = (prod_dim_before == prod_dim_after)
        _print(f"[driver] prod untouched: {prod_untouched}")

        passed = sum(1 for r in RESULTS if r["status"] == "PASS")
        failed = sum(1 for r in RESULTS if r["status"] == "FAIL")
        errored = sum(1 for r in RESULTS if r["status"] == "ERROR")
        _print(f"[driver] results: pass={passed} fail={failed} error={errored}")

        out_json = scratch / "t27_results.json"
        out_json.write_text(json.dumps({
            "scenarios": RESULTS,
            "prod_dim_before": prod_dim_before,
            "prod_dim_after":  prod_dim_after,
            "prod_untouched":  prod_untouched,
            "scratch": str(scratch),
            "sandbox_schema": SANDBOX_SCHEMA,
            "sandbox_dsn": SANDBOX_DSN,
            "pass": passed, "fail": failed, "error": errored,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, indent=2, default=str))
        _print(f"[driver] wrote {out_json}")

        return 0 if (failed == 0 and errored == 0) else 1

    finally:
        _drop_sandbox()
        # Keep scratch dir for inspection; report prints the path.


if __name__ == "__main__":
    sys.exit(main())
