from __future__ import annotations

from pathlib import Path

try:
    from .sample_dataset import (
        DATE_PATTERN,
        DEFAULT_CATALOG,
        DEFAULT_NAMESPACE,
        OUTPUT_DIR,
        SCHEMA_FIELDS,
        TIMESTAMP_PATTERN,
        WAREHOUSE_DIR,
        discover_sample_csv_paths,
    )
except ImportError:
    from sample_dataset import (
        DATE_PATTERN,
        DEFAULT_CATALOG,
        DEFAULT_NAMESPACE,
        OUTPUT_DIR,
        SCHEMA_FIELDS,
        TIMESTAMP_PATTERN,
        WAREHOUSE_DIR,
        discover_sample_csv_paths,
    )


def build_spark_schema():
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
    from pyspark.sql import SparkSession

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


def build_table_name(csv_path: Path, catalog: str, namespace: str) -> str:
    return f"{catalog}.{namespace}.{csv_path.stem}"


def describe_planned_writes(input_dir: Path, catalog: str, namespace: str) -> list[tuple[Path, str]]:
    csv_paths = discover_sample_csv_paths(input_dir)
    if not csv_paths:
        raise FileNotFoundError(f"No sample CSV files found in {input_dir}")

    return [
        (csv_path, build_table_name(csv_path, catalog, namespace))
        for csv_path in csv_paths
    ]


def write_csv_files_to_iceberg(
    input_dir: Path = OUTPUT_DIR,
    catalog: str = DEFAULT_CATALOG,
    namespace: str = DEFAULT_NAMESPACE,
    warehouse: Path = WAREHOUSE_DIR,
    master: str = "local[*]",
    dry_run: bool = False,
) -> list[str]:
    planned_writes = describe_planned_writes(input_dir, catalog, namespace)

    if dry_run:
        for csv_path, table_name in planned_writes:
            print(f"Would write {csv_path} -> {table_name}")
        return [table_name for _, table_name in planned_writes]

    try:
        spark = build_spark_session(catalog=catalog, warehouse=warehouse, master=master)
    except ModuleNotFoundError as exc:
        if exc.name == "pyspark":
            raise SystemExit(
                "PySpark is not installed. Install PySpark and run this script in a Spark "
                "environment with the matching Iceberg runtime package configured."
            ) from exc
        raise

    schema = build_spark_schema()
    written_tables: list[str] = []

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

        for csv_path, table_name in planned_writes:
            dataframe = (
                spark.read.option("header", True)
                .option("dateFormat", DATE_PATTERN)
                .option("timestampFormat", TIMESTAMP_PATTERN)
                .schema(schema)
                .csv(str(csv_path))
            )

            dataframe.writeTo(table_name).using("iceberg").createOrReplace()
            written_tables.append(table_name)
            print(f"Wrote {csv_path.name} to {table_name}")

        return written_tables
    finally:
        spark.stop()


def main() -> None:
    write_csv_files_to_iceberg()


if __name__ == "__main__":
    main()
