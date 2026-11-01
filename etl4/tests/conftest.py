import sys
from pathlib import Path

# Add project root to sys.path so `import etl4...` works from pytest.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
