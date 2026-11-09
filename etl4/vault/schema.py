"""Data Vault 2.0 helpers: canonical business-key normalisation and hash keys.

See etl4/docs/data_vault_design.md for the schema rationale.
"""
from __future__ import annotations

import hashlib
from typing import Iterable

import numpy as np
import pandas as pd


def _canon(x) -> str:
    """Canonicalise a business-key component for hashing.

    Strips whitespace, uppercases, and converts nulls to a single sentinel
    "~NULL~" so a missing origin airport doesn't collide with a present one
    at the byte-identical empty string.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)) or pd.isna(x):
        return "~NULL~"
    return str(x).strip().upper()


def hash_key(*parts) -> str:
    """MD5 over "|"-joined canonicalised components. Returns 32-char hex."""
    canon = "|".join(_canon(p) for p in parts)
    return hashlib.md5(canon.encode("utf-8")).hexdigest()


def hash_series(*cols: Iterable) -> pd.Series:
    """Vectorised hash over a sequence of pandas Series (business-key parts).

    Avoids a per-row python call. Each column is canonicalised (strip + upper,
    null → "~NULL~"), joined with "|", and md5'd.
    """
    cols = [pd.Series(c) for c in cols]
    n = len(cols[0])
    # Canonicalise each column as Python strings in one pass.
    canonised = []
    for c in cols:
        s = c.astype("string").str.strip().str.upper().fillna("~NULL~")
        canonised.append(s)
    joined = canonised[0].astype(str)
    for extra in canonised[1:]:
        joined = joined + "|" + extra.astype(str)
    return pd.Series(
        [hashlib.md5(v.encode("utf-8")).hexdigest() for v in joined],
        index=joined.index,
        dtype="string",
        name=None,
    )


def rec_hash(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Satellite change-detection hash: md5 over selected attribute columns.

    When the rec_hash matches the previous load for the same business key,
    the satellite row is unchanged and can be skipped. Row-by-row to keep
    the code plain; the satellites that matter (state observations) size at
    ~4M rows and the hash is single-column (time + obs) anyway.
    """
    def _one_row(row):
        s = "|".join(_canon(row[c]) for c in cols)
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    return df.apply(_one_row, axis=1).astype("string")


# Standard audit columns applied to every raw-vault table.
AUDIT_COLS = ("load_dt", "rec_src")
