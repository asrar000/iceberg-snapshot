from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    from .sample_dataset import (
        FILE_COUNT,
        HEADER,
        MUTABLE_ROWS,
        OUTPUT_DIR,
        PRODUCT_CATEGORIES,
        REGIONS,
        ROWS_PER_FILE,
        STABLE_ROWS,
        UNIQUE_ROWS_PER_FILE,
    )
except ImportError:
    from sample_dataset import (
        FILE_COUNT,
        HEADER,
        MUTABLE_ROWS,
        OUTPUT_DIR,
        PRODUCT_CATEGORIES,
        REGIONS,
        ROWS_PER_FILE,
        STABLE_ROWS,
        UNIQUE_ROWS_PER_FILE,
    )


def base_row(record_id: int) -> list[object]:
    event_date = date(2026, 1, 1) + timedelta(days=(record_id - 1) % 90)
    event_ts = datetime(2026, 1, 1, 8, 0, 0) + timedelta(minutes=(record_id - 1) * 7)
    amount = 25.0 + ((record_id * 173) % 47_500) / 100

    return [
        record_id,
        10_000 + (record_id % 900),
        PRODUCT_CATEGORIES[(record_id - 1) % len(PRODUCT_CATEGORIES)],
        REGIONS[(record_id - 1) % len(REGIONS)],
        f"{amount:.2f}",
        str(record_id % 2 == 0).lower(),
        event_date.isoformat(),
        event_ts.strftime("%Y-%m-%d %H:%M:%S"),
    ]


def build_stable_row(record_id: int) -> list[object]:
    return base_row(record_id)


def build_mutable_row(record_id: int, snapshot_index: int) -> list[object]:
    row = base_row(record_id)
    row[3] = REGIONS[((record_id - 1) + snapshot_index) % len(REGIONS)]

    base_amount = float(row[4])
    row[4] = f"{base_amount + (snapshot_index * ((record_id % 7) + 1) * 2.5):.2f}"
    row[5] = str((record_id + snapshot_index) % 2 == 0).lower()

    event_date = date.fromisoformat(row[6]) + timedelta(days=snapshot_index)
    event_ts = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S") + timedelta(
        hours=snapshot_index * 3
    )
    row[6] = event_date.isoformat()
    row[7] = event_ts.strftime("%Y-%m-%d %H:%M:%S")
    return row


def build_unique_row(record_id: int, snapshot_index: int) -> list[object]:
    row = base_row(record_id)
    row[3] = REGIONS[((record_id - 1) + snapshot_index + 2) % len(REGIONS)]

    base_amount = float(row[4])
    row[4] = f"{base_amount + (snapshot_index * 5.0):.2f}"

    event_date = date.fromisoformat(row[6]) + timedelta(days=snapshot_index * 2)
    event_ts = datetime.strptime(row[7], "%Y-%m-%d %H:%M:%S") + timedelta(
        days=snapshot_index
    )
    row[6] = event_date.isoformat()
    row[7] = event_ts.strftime("%Y-%m-%d %H:%M:%S")
    return row


def generate_sample_csvs(output_dir: Path = OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for file_index in range(1, FILE_COUNT + 1):
        snapshot_index = file_index - 1
        output_path = output_dir / f"sample_{file_index}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(HEADER)

            for record_id in range(1, STABLE_ROWS + 1):
                writer.writerow(build_stable_row(record_id))

            for record_id in range(STABLE_ROWS + 1, STABLE_ROWS + MUTABLE_ROWS + 1):
                writer.writerow(build_mutable_row(record_id, snapshot_index))

            unique_start = STABLE_ROWS + MUTABLE_ROWS + 1 + (
                snapshot_index * UNIQUE_ROWS_PER_FILE
            )
            unique_end = unique_start + UNIQUE_ROWS_PER_FILE
            for record_id in range(unique_start, unique_end):
                writer.writerow(build_unique_row(record_id, snapshot_index))

        print(f"Created {output_path} with {ROWS_PER_FILE} rows")
