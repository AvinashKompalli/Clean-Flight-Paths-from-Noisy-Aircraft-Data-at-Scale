# GitHub Actions workflows

CI for this repo. Two jobs, both on `ubuntu-latest`, triggered on every push and every pull request:

- `pytest` runs `pytest etl4/tests/test_pipeline.py -v --tb=short` under Python 3.11 against `etl4/requirements.txt`, with pip cache keyed on that requirements file.
- `dashboards` validates every file under `etl4/dashboard/*.json` parses as JSON. Runs in parallel; dashboard failures do not block the test run.

No secrets, no Docker, no Kafka — the invariant tests are pandas-level only.
