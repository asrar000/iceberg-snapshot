from __future__ import annotations

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

INPUT_DIR = OUTPUT_DIR
CATALOG = DEFAULT_CATALOG
NAMESPACE = DEFAULT_NAMESPACE
TABLE_NAME = DEFAULT_TABLE_NAME
WAREHOUSE = WAREHOUSE_DIR
MASTER = "local[*]"


def main() -> None:
    target_table = build_table_name(CATALOG, NAMESPACE, TABLE_NAME)

    print("Generating 5 CSV files with the same schema and snapshot-style overlap...")
    generate_sample_csvs(INPUT_DIR)

    planned_writes = describe_planned_writes(
        input_dir=INPUT_DIR,
        catalog=CATALOG,
        namespace=NAMESPACE,
        table_name=TABLE_NAME,
    )

    print(f"Target Iceberg table: {target_table}")
    print_planned_writes(planned_writes)

    try:
        spark = build_spark_session(
            catalog=CATALOG,
            warehouse=WAREHOUSE,
            master=MASTER,
        )
        schema = build_spark_schema()
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark":
            raise SystemExit(PYSPARK_MISSING_MESSAGE) from exc
        raise

    try:
        create_namespace_if_missing(spark, CATALOG, NAMESPACE)

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

        print_snapshot_table(spark, CATALOG, NAMESPACE, TABLE_NAME)
        print_snapshot_similarity_and_difference(
            spark,
            CATALOG,
            NAMESPACE,
            TABLE_NAME,
            left_position=1,
            right_position=3,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
