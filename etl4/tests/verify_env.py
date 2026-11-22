"""Quick import check for the OpenSky env. Phase-1 smoke verification."""
import confluent_kafka
import duckdb
import pytest
import prometheus_client
import pandas
import pyarrow
import requests
import yaml
import dotenv
from pyopensky.trino import Trino  # noqa: F401

print("all core imports OK")
print(f"confluent_kafka   {confluent_kafka.__version__}")
print(f"duckdb            {duckdb.__version__}")
print(f"pytest            {pytest.__version__}")
print(f"pandas            {pandas.__version__}")
print(f"pyarrow           {pyarrow.__version__}")
print(f"requests          {requests.__version__}")
