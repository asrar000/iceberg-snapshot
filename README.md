# iceberg-snapshot

`main.py` is the single entry point for the full workflow. It:

1. generates 5 CSV files
2. keeps the same schema across all files
3. includes both similar and dissimilar rows across the files
4. loads each CSV as a Spark DataFrame
5. writes all 5 DataFrames into one Iceberg table as successive snapshots
6. prints the Iceberg snapshot metadata table
7. prints the similarity and dissimilarity between snapshot 1 and snapshot 3

The workflow now uses fixed in-code defaults in [main.py](/home/w3e21/assingments/iceberg-snapshot/main.py):

- input directory: `data/sample_csvs`
- catalog: `local`
- namespace: `db`
- table: `sample_events`
- warehouse: `warehouse/`
- Spark master: `local[*]`

Generated CSV files are written to `data/sample_csvs/`.

CSV schema:

```text
record_id: int
customer_id: int
product_category: string
region: string
amount: double
is_priority: boolean
event_date: date
event_ts: timestamp
```

Each CSV file contains the same schema and exactly 1,000 data rows.

The files are generated as snapshot-like datasets for comparison work:

- 600 rows are identical across all 5 CSV files
- 250 rows reuse the same `record_id` values but change selected columns across files
- 150 rows are unique to each CSV file

To run the full workflow, use:

```bash
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=$PWD/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1 pyspark-shell" .venv/bin/python main.py
```

At startup the script prints the planned snapshot writes into the single Iceberg table:

```text
/home/w3e21/assingments/iceberg-snapshot/data/sample_csvs/sample_1.csv -> local.db.sample_events (snapshot 1)
/home/w3e21/assingments/iceberg-snapshot/data/sample_csvs/sample_2.csv -> local.db.sample_events (snapshot 2)
/home/w3e21/assingments/iceberg-snapshot/data/sample_csvs/sample_3.csv -> local.db.sample_events (snapshot 3)
/home/w3e21/assingments/iceberg-snapshot/data/sample_csvs/sample_4.csv -> local.db.sample_events (snapshot 4)
/home/w3e21/assingments/iceberg-snapshot/data/sample_csvs/sample_5.csv -> local.db.sample_events (snapshot 5)
```

To perform the actual Spark and Iceberg workflow, run the script through a Spark
environment that already has PySpark plus the matching Iceberg runtime JAR for
your Spark version:

```bash
SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost PYSPARK_SUBMIT_ARGS="--conf spark.jars.ivy=$PWD/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1 pyspark-shell" .venv/bin/python main.py
```

This command sets the local Spark host values and attaches the Iceberg Spark
runtime package so Spark can load `SparkCatalog` and the Iceberg session
extensions.

The job creates a local Hadoop-backed Iceberg catalog named `local` and writes
the `sample_events` table into the `db` namespace under `warehouse/`. After the
writes finish, the job prints the Iceberg snapshot metadata table to the
console.

It also compares snapshot 1 and snapshot 3 of `local.db.sample_events` and
prints:

- identical full rows shared by both snapshots
- rows with the same `record_id` but changed values
- rows that only exist in snapshot 1
- rows that only exist in snapshot 3

After the writes complete, you can inspect the snapshot history with:

```sql
SELECT * FROM local.db.sample_events.snapshots;
```
