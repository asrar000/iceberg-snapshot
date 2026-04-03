from __future__ import annotations

from pathlib import Path

try:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    _PYSPARK_IMPORT_ERROR = None
except ModuleNotFoundError as exc:
    SparkSession = None
    F = None
    BooleanType = None
    DateType = None
    DoubleType = None
    IntegerType = None
    StringType = None
    StructField = None
    StructType = None
    TimestampType = None
    _PYSPARK_IMPORT_ERROR = exc

try:
    from .sample_dataset import (
        DATE_PATTERN,
        SCHEMA_FIELDS,
        TIMESTAMP_PATTERN,
        discover_sample_csv_paths,
    )
except ImportError:
    from sample_dataset import (
        DATE_PATTERN,
        SCHEMA_FIELDS,
        TIMESTAMP_PATTERN,
        discover_sample_csv_paths,
    )

PYSPARK_MISSING_MESSAGE = (
    "PySpark is not installed. Install PySpark and run this script in a Spark "
    "environment with the matching Iceberg runtime package configured."
)


def ensure_pyspark_available() -> None:
    if _PYSPARK_IMPORT_ERROR is not None:
        raise ModuleNotFoundError("pyspark") from _PYSPARK_IMPORT_ERROR


def build_spark_schema():
    ensure_pyspark_available()

    type_map = {
        "int": IntegerType(),
        "string": StringType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }
    return StructType(
        [StructField(name, type_map[data_type], False) for name, data_type in SCHEMA_FIELDS]
    )


def build_spark_session(catalog: str, warehouse: Path, master: str):
    ensure_pyspark_available()

    warehouse_path = warehouse.resolve()
    return (
        SparkSession.builder.appName("csv-to-iceberg")
        .master(master)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse_path))
        .getOrCreate()
    )


def build_table_name(catalog: str, namespace: str, table_name: str) -> str:
    return f"{catalog}.{namespace}.{table_name}"


def build_snapshots_table_name(catalog: str, namespace: str, table_name: str) -> str:
    return f"{build_table_name(catalog, namespace, table_name)}.snapshots"


def describe_planned_writes(
    input_dir: Path,
    catalog: str,
    namespace: str,
    table_name: str,
) -> list[tuple[int, Path, str]]:
    csv_paths = discover_sample_csv_paths(input_dir)
    if not csv_paths:
        raise FileNotFoundError(f"No sample CSV files found in {input_dir}")

    target_table = build_table_name(catalog, namespace, table_name)
    return [
        (snapshot_index, csv_path, target_table)
        for snapshot_index, csv_path in enumerate(csv_paths, start=1)
    ]


def print_planned_writes(planned_writes: list[tuple[int, Path, str]]) -> None:
    for snapshot_index, csv_path, table_identifier in planned_writes:
        print(
            f"Would write {csv_path} -> {table_identifier} "
            f"(snapshot {snapshot_index})"
        )


def iceberg_table_exists(spark, table_identifier: str) -> bool:
    return spark.catalog.tableExists(table_identifier)


def create_namespace_if_missing(spark, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def load_csv_as_dataframe(spark, csv_path: Path, schema):
    return (
        spark.read.option("header", True)
        .option("dateFormat", DATE_PATTERN)
        .option("timestampFormat", TIMESTAMP_PATTERN)
        .schema(schema)
        .csv(str(csv_path))
    )


def write_dataframe_to_iceberg(spark, dataframe, table_identifier: str, snapshot_index: int) -> None:
    ensure_pyspark_available()

    if snapshot_index == 1 and not iceberg_table_exists(spark, table_identifier):
        dataframe.writeTo(table_identifier).using("iceberg").create()
    else:
        dataframe.writeTo(table_identifier).overwrite(F.lit(True))


def print_snapshot_table(spark, catalog: str, namespace: str, table_name: str) -> None:
    snapshots_table = build_snapshots_table_name(catalog, namespace, table_name)
    snapshots_df = spark.sql(
        f"SELECT * FROM {snapshots_table} ORDER BY committed_at"
    )

    print(f"Snapshot table for {build_table_name(catalog, namespace, table_name)}:")
    snapshots_df.show(truncate=False)


def read_snapshot_dataframe(spark, table_identifier: str, snapshot_id: int):
    return (
        spark.read.format("iceberg")
        .option("snapshot-id", snapshot_id)
        .load(table_identifier)
    )


def build_changed_rows_dataframe(left_df, right_df, key_column: str):
    comparable_columns = [column for column in left_df.columns if column != key_column]

    left_alias = left_df.alias("left")
    right_alias = right_df.alias("right")

    difference_condition = None
    for column in comparable_columns:
        column_diff = left_alias[column] != right_alias[column]
        difference_condition = (
            column_diff
            if difference_condition is None
            else difference_condition | column_diff
        )

    if difference_condition is None:
        raise ValueError("No comparable columns were found for snapshot comparison.")

    select_columns = [left_alias[key_column].alias(key_column)]
    for column in comparable_columns:
        select_columns.append(left_alias[column].alias(f"{column}_snapshot_left"))
        select_columns.append(right_alias[column].alias(f"{column}_snapshot_right"))

    return (
        left_alias.join(right_alias, on=key_column, how="inner")
        .where(difference_condition)
        .select(*select_columns)
        .orderBy(key_column)
    )


def print_snapshot_similarity_and_difference(
    spark,
    catalog: str,
    namespace: str,
    table_name: str,
    left_position: int = 1,
    right_position: int = 3,
    key_column: str = "record_id",
) -> None:
    snapshots_table = build_snapshots_table_name(catalog, namespace, table_name)
    snapshot_rows = (
        spark.sql(
            f"""
            SELECT committed_at, snapshot_id, parent_id, operation
            FROM {snapshots_table}
            ORDER BY committed_at
            """
        )
        .collect()
    )

    if len(snapshot_rows) < max(left_position, right_position):
        raise ValueError(
            f"Need at least {max(left_position, right_position)} snapshots to compare "
            f"positions {left_position} and {right_position}, but found {len(snapshot_rows)}."
        )

    left_snapshot = snapshot_rows[left_position - 1]
    right_snapshot = snapshot_rows[right_position - 1]
    table_identifier = build_table_name(catalog, namespace, table_name)

    left_df = read_snapshot_dataframe(spark, table_identifier, left_snapshot.snapshot_id)
    right_df = read_snapshot_dataframe(spark, table_identifier, right_snapshot.snapshot_id)

    similar_rows = left_df.intersect(right_df).orderBy(key_column)
    left_only_rows = left_df.exceptAll(right_df).orderBy(key_column)
    right_only_rows = right_df.exceptAll(left_df).orderBy(key_column)
    changed_rows = build_changed_rows_dataframe(left_df, right_df, key_column)

    print(
        "Comparing snapshot positions "
        f"{left_position} and {right_position} for {table_identifier}:"
    )
    print(
        f"Snapshot {left_position}: id={left_snapshot.snapshot_id}, "
        f"committed_at={left_snapshot.committed_at}, "
        f"operation={left_snapshot.operation}"
    )
    print(
        f"Snapshot {right_position}: id={right_snapshot.snapshot_id}, "
        f"committed_at={right_snapshot.committed_at}, "
        f"operation={right_snapshot.operation}"
    )

    print(
        "Similarity summary: "
        f"{similar_rows.count()} identical full rows exist in both snapshots."
    )
    similar_rows.show(20, truncate=False)

    print(
        "Difference summary: "
        f"{changed_rows.count()} shared {key_column} values changed, "
        f"{left_only_rows.count()} rows exist only in snapshot {left_position}, "
        f"and {right_only_rows.count()} rows exist only in snapshot {right_position}."
    )

    print(f"Rows with changed values for shared {key_column}s:")
    changed_rows.show(20, truncate=False)

    print(f"Rows only in snapshot {left_position}:")
    left_only_rows.show(20, truncate=False)

    print(f"Rows only in snapshot {right_position}:")
    right_only_rows.show(20, truncate=False)
