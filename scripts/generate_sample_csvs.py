from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path


OUTPUT_DIR = Path("data/sample_csvs")
FILE_COUNT = 5
ROWS_PER_FILE = 1000
SEED = 42

HEADER = [
    "record_id",
    "customer_id",
    "product_category",
    "region",
    "amount",
    "is_priority",
    "event_date",
    "event_ts",
]

PRODUCT_CATEGORIES = ["books", "electronics", "fashion", "grocery", "home"]
REGIONS = ["north", "south", "east", "west", "central"]


def build_row(record_id: int, rng: random.Random) -> list[object]:
    base_date = date(2026, 1, 1)
    day_offset = (record_id - 1) % 90
    event_date = base_date + timedelta(days=day_offset)

    base_ts = datetime(2026, 1, 1, 8, 0, 0)
    minute_offset = (record_id - 1) * 7
    event_ts = base_ts + timedelta(minutes=minute_offset)

    return [
        record_id,
        10_000 + rng.randint(1, 900),
        PRODUCT_CATEGORIES[(record_id - 1) % len(PRODUCT_CATEGORIES)],
        REGIONS[(record_id - 1) % len(REGIONS)],
        f"{rng.uniform(10.0, 500.0):.2f}",
        str(record_id % 2 == 0).lower(),
        event_date.isoformat(),
        event_ts.strftime("%Y-%m-%d %H:%M:%S"),
    ]


def main() -> None:
    rng = random.Random(SEED)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    record_id = 1
    for file_index in range(1, FILE_COUNT + 1):
        output_path = OUTPUT_DIR / f"sample_{file_index}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(HEADER)
            for _ in range(ROWS_PER_FILE):
                writer.writerow(build_row(record_id, rng))
                record_id += 1

        print(f"Created {output_path} with {ROWS_PER_FILE} rows")


if __name__ == "__main__":
    main()
