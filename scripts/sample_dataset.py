from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "data" / "sample_csvs"
WAREHOUSE_DIR = REPO_ROOT / "warehouse"

FILE_COUNT = 5
ROWS_PER_FILE = 1000
SEED = 42

DEFAULT_CATALOG = "local"
DEFAULT_NAMESPACE = "db"

SCHEMA_FIELDS = [
    ("record_id", "int"),
    ("customer_id", "int"),
    ("product_category", "string"),
    ("region", "string"),
    ("amount", "double"),
    ("is_priority", "boolean"),
    ("event_date", "date"),
    ("event_ts", "timestamp"),
]

HEADER = [name for name, _ in SCHEMA_FIELDS]

DATE_PATTERN = "yyyy-MM-dd"
TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"

PRODUCT_CATEGORIES = ["books", "electronics", "fashion", "grocery", "home"]
REGIONS = ["north", "south", "east", "west", "central"]


def discover_sample_csv_paths(input_dir: Path = OUTPUT_DIR) -> list[Path]:
    return sorted(path for path in input_dir.glob("sample_*.csv") if path.is_file())
