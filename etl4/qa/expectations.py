"""Data-quality expectation suites, in Great Expectations-compatible form.

Each suite is a list of expectation dicts matching the GE 1.x
ExpectationConfiguration shape. validate.py runs them through the real
`great_expectations` library when it is installed; otherwise it evaluates
them with the inline pandas evaluator in that module (same semantics,
narrower expectation vocabulary).

Five expectations per table, chosen for load-bearing invariants, not
trivia:
  - bronze.raw_states                (parquet tree)
  - mart.dim_aircraft                (Postgres)
  - mart.dim_airport                 (Postgres)
  - mart.fact_trajectory_enriched    (Postgres)
  - mart.fact_experiment_result      (Postgres)
"""
from __future__ import annotations

SUITES: dict[str, dict] = {
    # ---------------- bronze.raw_states ----------------
    "bronze_raw_states": {
        "source": {"kind": "parquet_tree", "path": "data/bronze/raw_states"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "icao24"}},
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "time"}},
            {"type": "expect_column_values_to_be_between",
             "kwargs": {"column": "lat", "min_value": -90.0, "max_value": 90.0,
                        "mostly": 0.999}},
            {"type": "expect_column_values_to_be_between",
             "kwargs": {"column": "lon", "min_value": -180.0, "max_value": 180.0,
                        "mostly": 0.999}},
            {"type": "expect_column_values_to_match_regex",
             "kwargs": {"column": "icao24", "regex": r"^[0-9a-f]{6}$",
                        "mostly": 0.99}},
        ],
    },

    # ---------------- mart.dim_aircraft ----------------
    "mart_dim_aircraft": {
        "source": {"kind": "postgres", "table": "dim_aircraft"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "hk_aircraft"}},
            {"type": "expect_column_values_to_be_unique",
             "kwargs": {"column": "hk_aircraft"}},
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "icao24"}},
            {"type": "expect_column_values_to_match_regex",
             "kwargs": {"column": "icao24", "regex": r"^[0-9A-Fa-f]{6}$",
                        "mostly": 0.99}},
            {"type": "expect_table_row_count_to_be_between",
             "kwargs": {"min_value": 1000}},
        ],
    },

    # ---------------- mart.dim_airport ----------------
    # Parquet-only mart - postgres_sink doesn't load dim_airport because the
    # airport hub is tiny and only joined server-side for enrichment.
    "mart_dim_airport": {
        "source": {"kind": "parquet_tree", "path": "data/gold/marts/dim_airport"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "hk_airport"}},
            {"type": "expect_column_values_to_be_unique",
             "kwargs": {"column": "hk_airport"}},
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "icao_code"}},
            {"type": "expect_column_value_lengths_to_be_between",
             "kwargs": {"column": "icao_code", "min_value": 2, "max_value": 4}},
            {"type": "expect_table_row_count_to_be_between",
             "kwargs": {"min_value": 10}},
        ],
    },

    # ---------------- mart.fact_trajectory_enriched ----------------
    "mart_fact_trajectory_enriched": {
        "source": {"kind": "postgres", "table": "fact_trajectory_enriched"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "hk_aircraft"}},
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "time"}},
            {"type": "expect_column_values_to_be_between",
             "kwargs": {"column": "lat", "min_value": -90.0, "max_value": 90.0,
                        "mostly": 0.999}},
            {"type": "expect_column_values_to_be_between",
             "kwargs": {"column": "lon", "min_value": -180.0, "max_value": 180.0,
                        "mostly": 0.999}},
            {"type": "expect_table_row_count_to_be_between",
             "kwargs": {"min_value": 100_000}},
        ],
    },

    # ---------------- mart.fact_experiment_result ----------------
    "mart_fact_experiment_result": {
        "source": {"kind": "postgres", "table": "fact_experiment_result"},
        "expectations": [
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "method"}},
            {"type": "expect_column_values_to_not_be_null",
             "kwargs": {"column": "noise"}},
            {"type": "expect_column_values_to_be_in_set",
             "kwargs": {"column": "noise", "value_set": ["N0", "N1", "N2", "N3"]}},
            {"type": "expect_column_values_to_be_between",
             "kwargs": {"column": "continuity", "min_value": 0.0, "max_value": 1.0,
                        "mostly": 1.0}},
            {"type": "expect_table_row_count_to_be_between",
             "kwargs": {"min_value": 1, "max_value": 64}},
        ],
    },
}
