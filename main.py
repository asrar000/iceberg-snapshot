from __future__ import annotations

import argparse
from pathlib import Path

from scripts.generate_sample_csvs import generate_sample_csvs
from scripts.load_csvs_to_iceberg import write_csv_files_to_iceberg
from scripts.sample_dataset import (
    DEFAULT_CATALOG,
    DEFAULT_NAMESPACE,
    DEFAULT_TABLE_NAME,
    OUTPUT_DIR,
    WAREHOUSE_DIR,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate sample CSV files and write them as successive snapshots "
            "into a single Iceberg table."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Directory containing the sample CSV files.",
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help="Spark catalog name for the Iceberg tables.",
    )
    parser.add_argument(
        "--namespace",
        default=DEFAULT_NAMESPACE,
        help="Iceberg namespace (database) to create tables in.",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE_NAME,
        help="Iceberg table name that will receive all CSV snapshots.",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=WAREHOUSE_DIR,
        help="Warehouse path for the local Hadoop-backed Iceberg catalog.",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master to use when creating the session.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Reuse existing CSV files instead of regenerating them first.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned snapshot writes without starting Spark.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.skip_generate:
        generate_sample_csvs(args.input_dir)

    write_csv_files_to_iceberg(
        input_dir=args.input_dir,
        catalog=args.catalog,
        namespace=args.namespace,
        table_name=args.table,
        warehouse=args.warehouse,
        master=args.master,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
