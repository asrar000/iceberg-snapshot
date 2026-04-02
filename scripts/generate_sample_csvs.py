from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    from .sample_dataset import (
        FILE_COUNT,
        HEADER,
        OUTPUT_DIR,
        PRODUCT_CATEGORIES,
        REGIONS,
        ROWS_PER_FILE,
        SEED,
    )
except ImportError:
    from sample_dataset import (
        FILE_COUNT,
        HEADER,
        OUTPUT_DIR,
        PRODUCT_CATEGORIES,
        REGIONS,
        ROWS_PER_FILE,
        SEED,
    )


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


def generate_sample_csvs(output_dir: Path = OUTPUT_DIR) -> None:
    rng = random.Random(SEED)
    output_dir.mkdir(parents=True, exist_ok=True)

    record_id = 1
    for file_index in range(1, FILE_COUNT + 1):
        output_path = output_dir / f"sample_{file_index}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(HEADER)
            for _ in range(ROWS_PER_FILE):
                writer.writerow(build_row(record_id, rng))
                record_id += 1

        print(f"Created {output_path} with {ROWS_PER_FILE} rows")


def main() -> None:
    generate_sample_csvs()


if __name__ == "__main__":
    main()
