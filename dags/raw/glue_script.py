import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "table",
        "landing_path",
        "data_interval",
    ],
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

table = args["table"]
landing_path = args["landing_path"]
data_interval = args["data_interval"]

ICEBERG_TABLE = f"iceberg.raw.{table}"

df = spark.read.json(landing_path)

if df.rdd.isEmpty():
    print(f"No files at {landing_path}; skipping.")
    job.commit()
    sys.exit(0)

df = df.withColumn("data_interval", F.lit(data_interval))

table_exists = spark.catalog.tableExists(ICEBERG_TABLE)

if not table_exists:
    (
        df.writeTo(ICEBERG_TABLE)
        .partitionedBy("data_interval")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create()
    )
    print(f"[{table}] Created Iceberg table: {ICEBERG_TABLE}")
else:
    target_df = spark.table(ICEBERG_TABLE)
    new_cols = set(df.columns) - set(target_df.columns)

    for col in sorted(new_cols):
        col_type = df.schema[col].dataType.simpleString()
        spark.sql(f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN {col} {col_type}")

    df.writeTo(ICEBERG_TABLE).overwritePartitions()
    print(f"[{table}] Overwrote partition data_interval={data_interval}")

count = spark.sql(
    f"""
    SELECT COUNT(*) FROM {ICEBERG_TABLE}
    WHERE data_interval = '{data_interval}'
    """
).collect()[0][0]
print(f"[{table}] {count:,} rows in partition data_interval={data_interval}")

job.commit()
