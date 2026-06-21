import json
import sys
import uuid
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ["JOB_NAME", "config"])
config = json.loads(args["config"])

table_name = config["table"]
sql_s3_path = config["sql"]
upsert_keys = config["upsert_keys"]
partition_keys = config.get("partition_keys", [])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

parsed = urlparse(sql_s3_path)
sql_bucket = parsed.netloc
sql_key = parsed.path.lstrip("/")

s3 = boto3.client("s3")
sql_query = (
    s3.get_object(Bucket=sql_bucket, Key=sql_key)["Body"]
    .read()
    .decode("utf-8")
)

stg_df = spark.sql(sql_query).dropDuplicates(upsert_keys)

if partition_keys:
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

stg_df.createOrReplaceTempView(unique_stg)

target_table = f"iceberg.transactions.{table_name}"
table_exists = spark.catalog.tableExists(target_table)

if not table_exists:
    writer = (
        stg_df.writeTo(target_table)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
    )

    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)

    writer.create()
else:
    target_df = spark.table(target_table)
    new_cols = set(stg_df.columns) - set(target_df.columns)

    for col in sorted(new_cols):
        col_type = stg_df.schema[col].dataType.simpleString()
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")

    on_keys = list(dict.fromkeys(upsert_keys + partition_keys))
    on_clause = " AND ".join([f"t.{key} = s.{key}" for key in on_keys])
    update_assignments = ", ".join([f"t.{col} = s.{col}" for col in stg_df.columns])
    insert_columns = ", ".join(stg_df.columns)
    insert_values = ", ".join([f"s.{col}" for col in stg_df.columns])

    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING {unique_stg} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )

print(f"Completed Iceberg merge for {table_name}")
job.commit()
