from __future__ import annotations

import argparse
from pathlib import Path

from scripts.generate_sample_csvs import generate_sample_csvs
from scripts.load_csvs_to_iceberg import (
    PYSPARK_MISSING_MESSAGE,
    build_spark_schema,
    build_spark_session,
    build_table_name,
    create_namespace_if_missing,
    describe_planned_writes,
    load_csv_as_dataframe,
    print_planned_writes,
    print_snapshot_similarity_and_difference,
    print_snapshot_table,
    write_dataframe_to_iceberg,
)
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
    target_table = build_table_name(args.catalog, args.namespace, args.table)

    if not args.skip_generate:
        print("Generating 5 CSV files with the same schema and snapshot-style overlap...")
        generate_sample_csvs(args.input_dir)

    planned_writes = describe_planned_writes(
        input_dir=args.input_dir,
        catalog=args.catalog,
        namespace=args.namespace,
        table_name=args.table,
    )

    print(f"Target Iceberg table: {target_table}")

    if args.dry_run:
        print_planned_writes(planned_writes)
        return

    try:
        spark = build_spark_session(
            catalog=args.catalog,
            warehouse=args.warehouse,
            master=args.master,
        )
        schema = build_spark_schema()
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark":
            raise SystemExit(PYSPARK_MISSING_MESSAGE) from exc
        raise

    try:
        create_namespace_if_missing(spark, args.catalog, args.namespace)

        for snapshot_index, csv_path, table_identifier in planned_writes:
            print(f"Loading {csv_path.name} as a DataFrame...")
            dataframe = load_csv_as_dataframe(spark, csv_path, schema)
            print(
                f"Writing {csv_path.name} to {table_identifier} "
                f"as snapshot {snapshot_index}..."
            )
            write_dataframe_to_iceberg(
                spark=spark,
                dataframe=dataframe,
                table_identifier=table_identifier,
                snapshot_index=snapshot_index,
            )

        print_snapshot_table(spark, args.catalog, args.namespace, args.table)
        print_snapshot_similarity_and_difference(
            spark,
            args.catalog,
            args.namespace,
            args.table,
            left_position=1,
            right_position=3,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
