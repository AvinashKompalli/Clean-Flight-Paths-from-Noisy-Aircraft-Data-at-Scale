"""Run every expectation suite in etl4/qa/expectations.py.

Dual evaluator:
  - If `great_expectations` is importable, each suite runs through the real
    GE library (expectation vocabulary + reporting are authoritative).
  - Otherwise, suites run through a small inline evaluator covering the
    expectation types this project uses (ExpectColumnValues* / table row
    count / match regex / in set). Same pass-fail semantics; narrower
    vocabulary. This fallback exists because great_expectations has not
    yet published wheels for the CI Python (3.14) — pin a validated
    interpreter in `etl4/requirements.txt` when GE is required.

Exit code:
  0  every suite passed
  1  one or more suites failed (details printed, including per-expectation
     result and observed value)

Usage:
  python -m etl4.qa.validate                       # all suites
  python -m etl4.qa.validate --suite bronze_raw_states mart_dim_aircraft
  python -m etl4.qa.validate --dsn postgresql://etl4:change_me@localhost:5432/etl4
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from etl4.qa.expectations import SUITES

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DSN = os.getenv("POSTGRES_DSN", "postgresql://etl4:change_me@localhost:5432/etl4")


# ------------------------------------------------------------------
# Data source loaders
# ------------------------------------------------------------------
def _load_parquet_tree(path: Path, sample: int | None = None) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    if sample and len(df) > sample:
        df = df.sample(sample, random_state=42).reset_index(drop=True)
    return df


def _load_postgres(table: str, dsn: str, sample: int | None = None) -> pd.DataFrame:
    import psycopg
    with psycopg.connect(dsn) as conn:
        q = f"SELECT * FROM {table}"
        if sample:
            q += f" ORDER BY random() LIMIT {int(sample)}"
        return pd.read_sql(q, conn)


def _load(source: dict, dsn: str, sample: int | None) -> pd.DataFrame:
    kind = source["kind"]
    if kind == "parquet_tree":
        return _load_parquet_tree(ROOT / source["path"], sample)
    if kind == "postgres":
        return _load_postgres(source["table"], dsn, sample)
    raise ValueError(f"unknown source kind: {kind}")


# ------------------------------------------------------------------
# Inline evaluator (fallback when great_expectations isn't installed)
# ------------------------------------------------------------------
def _eval_inline(df: pd.DataFrame, exp: dict) -> dict:
    """Evaluate one expectation against a pandas DataFrame."""
    t = exp["type"]
    kw = exp["kwargs"]
    mostly = kw.get("mostly", 1.0)

    def _col(name):
        if name not in df.columns:
            return None
        return df[name]

    if t == "expect_table_row_count_to_be_between":
        n = len(df)
        lo = kw.get("min_value", 0)
        hi = kw.get("max_value", float("inf"))
        passed = lo <= n <= hi
        return {"success": passed, "observed": n, "bounds": [lo, hi]}

    col = _col(kw["column"])
    if col is None:
        return {"success": False, "observed": None, "error": f"column '{kw['column']}' missing"}

    if t == "expect_column_values_to_not_be_null":
        hit = col.notna().mean() if len(col) else 1.0
        return {"success": hit >= mostly, "observed": float(hit), "mostly": mostly}

    if t == "expect_column_values_to_be_unique":
        non_null = col.dropna()
        unique_frac = (non_null.nunique() / len(non_null)) if len(non_null) else 1.0
        return {"success": unique_frac >= mostly, "observed": float(unique_frac),
                "mostly": mostly, "duplicates": int(len(non_null) - non_null.nunique())}

    if t == "expect_column_values_to_be_between":
        s = pd.to_numeric(col, errors="coerce")
        valid = s.dropna()
        if not len(valid):
            return {"success": True, "observed": "no non-null values"}
        lo = kw.get("min_value", float("-inf"))
        hi = kw.get("max_value", float("inf"))
        hit = ((valid >= lo) & (valid <= hi)).mean()
        return {"success": hit >= mostly, "observed": float(hit),
                "bounds": [lo, hi], "mostly": mostly}

    if t == "expect_column_values_to_match_regex":
        s = col.dropna().astype(str)
        if not len(s):
            return {"success": True, "observed": "no non-null values"}
        pat = re.compile(kw["regex"])
        hit = s.map(lambda v: bool(pat.match(v))).mean()
        return {"success": hit >= mostly, "observed": float(hit), "mostly": mostly}

    if t == "expect_column_values_to_be_in_set":
        allowed = set(kw["value_set"])
        s = col.dropna()
        if not len(s):
            return {"success": True, "observed": "no non-null values"}
        hit = s.map(lambda v: v in allowed).mean()
        return {"success": hit >= mostly, "observed": float(hit),
                "allowed": sorted(allowed), "mostly": mostly}

    if t == "expect_column_value_lengths_to_be_between":
        s = col.dropna().astype(str)
        if not len(s):
            return {"success": True, "observed": "no non-null values"}
        lo = kw.get("min_value", 0)
        hi = kw.get("max_value", 10**9)
        hit = s.str.len().between(lo, hi).mean()
        return {"success": hit >= mostly, "observed": float(hit),
                "bounds": [lo, hi], "mostly": mostly}

    return {"success": False, "error": f"unsupported expectation type: {t}"}


def _try_ge_validate(df: pd.DataFrame, expectations: list[dict]) -> list[dict] | None:
    """Try to validate with the real great_expectations library.

    Returns a list of per-expectation result dicts, or None if GE isn't
    available or the API call surface doesn't match the bundled version.
    """
    try:
        import great_expectations as gx
        from great_expectations.core.expectation_suite import ExpectationSuite
        from great_expectations.core.expectation_configuration import ExpectationConfiguration
    except Exception:
        return None
    try:
        suite = ExpectationSuite(name="etl4_ad_hoc")
        for e in expectations:
            suite.add_expectation_configuration(
                ExpectationConfiguration(type=e["type"], kwargs=e["kwargs"])
            )
        ctx = gx.get_context(mode="ephemeral")
        ds = ctx.data_sources.add_pandas(name="etl4_pandas")
        asset = ds.add_dataframe_asset(name="etl4_asset")
        bd = asset.add_batch_definition_whole_dataframe("etl4_batch_def")
        batch = bd.get_batch(batch_parameters={"dataframe": df})
        result = batch.validate(suite)
        out = []
        for i, r in enumerate(result.results):
            out.append({
                "expectation": expectations[i],
                "result": {"success": bool(r.success),
                           "observed": r.result.get("observed_value") if r.result else None},
            })
        return out
    except Exception as exc:
        print(f"[WARN] GE validation failed, falling back to inline evaluator: {exc}",
              flush=True)
        return None


# ------------------------------------------------------------------
# Suite runner
# ------------------------------------------------------------------
def run_suite(name: str, suite: dict, dsn: str, sample: int | None) -> dict:
    print(f"\n[qa] suite: {name}", flush=True)
    df = _load(suite["source"], dsn, sample)
    print(f"[qa]   rows loaded: {len(df):,}", flush=True)

    ge_results = _try_ge_validate(df, suite["expectations"]) if df.shape[0] else None

    results: list[dict] = []
    if ge_results is not None:
        results = ge_results
        for r in results:
            mark = "OK  " if r["result"]["success"] else "FAIL"
            k = r["expectation"]["kwargs"]
            col = k.get("column", "-")
            print(f"[qa]   [{mark}] {r['expectation']['type']} column={col} "
                  f"observed={r['result']['observed']}", flush=True)
    else:
        for e in suite["expectations"]:
            r = _eval_inline(df, e)
            results.append({"expectation": e, "result": r})
            mark = "OK  " if r["success"] else "FAIL"
            col = e["kwargs"].get("column", "-")
            obs = r.get("observed")
            print(f"[qa]   [{mark}] {e['type']} column={col} observed={obs}", flush=True)

    passed = sum(1 for r in results if r["result"].get("success"))
    total  = len(results)
    status = "ok" if passed == total else "fail"
    print(f"[qa]   {status.upper()}  {passed}/{total} expectations passed",
          flush=True)
    return {"suite": name, "passed": passed, "total": total, "status": status,
            "results": results}


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--suite", nargs="+", default=None,
                   help="which suites to run (default: all)")
    p.add_argument("--dsn", default=DEFAULT_DSN)
    p.add_argument("--sample", type=int, default=250_000,
                   help="per-table row sample size (0 = full table)")
    p.add_argument("--json", default=None, help="write report to this path")
    p.add_argument("--fail-fast", action="store_true")
    a = p.parse_args()

    names = a.suite or list(SUITES.keys())
    sample = a.sample if a.sample > 0 else None

    overall: list[dict] = []
    any_failed = False
    for name in names:
        if name not in SUITES:
            print(f"[ERR] unknown suite: {name}", file=sys.stderr)
            sys.exit(2)
        try:
            rep = run_suite(name, SUITES[name], a.dsn, sample)
        except Exception as exc:
            rep = {"suite": name, "status": "error", "error": str(exc)}
            print(f"[qa]   ERROR  {exc}", flush=True)
        overall.append(rep)
        if rep.get("status") != "ok":
            any_failed = True
            if a.fail_fast:
                break

    print("\n[qa] ---- summary ----", flush=True)
    for r in overall:
        print(f"[qa]   {r['status'].upper():5s}  {r['suite']}  "
              f"{r.get('passed', '-')}/{r.get('total', '-')}", flush=True)

    if a.json:
        Path(a.json).write_text(json.dumps(overall, default=str, indent=2))
        print(f"[qa] report written: {a.json}", flush=True)

    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
