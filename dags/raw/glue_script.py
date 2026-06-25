import re
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Read job arguments ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", 
    "table", 
    "landing_path",
    "data_interval",
])

# ── Configure spark ───────────────────────────────────────────────────────────

# Initialize a spark context
sc = SparkContext()

# Wrap the spark context in a GlueContext
glueContext = GlueContext(sc)

# Store the glue context's spark_session to the variable `spark`
spark = glueContext.spark_session

# Init a glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args) 

# ── Set up locations ────────────────────────────────────────────────────────────

table = args["table"]
landing_path = args["landing_path"]

ICEBERG_TABLE = f"iceberg.raw.{table}"

# ── Read landing files ──────────────────────────────────────────────────────────

data_interval = args["data_interval"]

# Spark infers schema from JSON natively — no header/inferSchema options needed.
# Default reader expects newline-delimited JSON (one object per line); for files
# containing a single object or pretty-printed JSON, add .option("multiLine", "true").
df = spark.read.json(landing_path)

if df.rdd.isEmpty():
    print(f"No files at {landing_path} — skipping.")
    job.commit()
    sys.exit(0)

# Normalize every discovered source field without hardcoding a table schema.
# This keeps raw tables aligned with the documented snake_case column names
# while still allowing new source tables and columns to flow through unchanged.
def to_snake_case(column_name):
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", column_name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return value.lower()


source_columns = df.columns
normalized_columns = [to_snake_case(column) for column in source_columns]
if len(normalized_columns) != len(set(normalized_columns)):
    raise ValueError(
        f"Column normalization produced duplicate names for table {table}: "
        f"{normalized_columns}"
    )

df = df.toDF(*normalized_columns)

# Add data_interval column to the dataframe
df = df.withColumn("data_interval", F.lit(data_interval))

# ── Write to Iceberg ────────────────────────────────────────────────────────────

# Check if the table exists
table_exists = spark.catalog.tableExists(ICEBERG_TABLE)

if table_exists:
    # Migrate previously created raw tables to the normalized field names.
    # The mapping is derived from the discovered schema, so reruns require no
    # hardcoded source columns or manual table cleanup.
    target_columns = set(spark.table(ICEBERG_TABLE).columns)
    for source_column, normalized_column in zip(source_columns, normalized_columns):
        if (
            source_column != normalized_column
            and source_column in target_columns
            and normalized_column not in target_columns
        ):
            spark.sql(
                f"ALTER TABLE {ICEBERG_TABLE} "
                f"RENAME COLUMN `{source_column}` TO `{normalized_column}`"
            )
            target_columns.remove(source_column)
            target_columns.add(normalized_column)

if not table_exists:

    # If the table doesn't exists create the table and write 
    # the raw data to it using iceberg
    (
        df
        .writeTo(ICEBERG_TABLE)
        # Set "data_interval" as the partition column
        .partitionedBy("data_interval")
        # Use Iceberg as the table format
        .using("iceberg")
        # Set Parquet as the storage format for data files
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        # Snappy compression: high write speed, high read speed, moderate storage costs
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create()
    )
    print(f"[{table}] Created Iceberg table: {ICEBERG_TABLE}")
else:
    # If the table exists, overwrite the partitions
    # found in the raw data
    df.writeTo(ICEBERG_TABLE).overwritePartitions()
    print(f"[{table}] Overwrote partition data_interval={data_interval}")

count = spark.sql(f"""
    SELECT COUNT(*) FROM {ICEBERG_TABLE}
    WHERE data_interval = '{data_interval}'
""").collect()[0][0]
print(f"[{table}] {count:,} rows in partition data_interval={data_interval}")

job.commit()
