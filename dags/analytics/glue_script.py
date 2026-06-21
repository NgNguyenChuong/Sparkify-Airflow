import json
import sys
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

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

parsed = urlparse(sql_s3_path)
sql_bucket = parsed.netloc
sql_key = parsed.path.lstrip("/")

s3 = boto3.client("s3")
sql_query = (
    s3.get_object(Bucket=sql_bucket, Key=sql_key)["Body"]
    .read()
    .decode("utf-8")
)

result_df = spark.sql(sql_query)
target_table = f"iceberg.analytics.{table_name}"

(
    result_df.writeTo(target_table)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print(f"Completed Iceberg overwrite for {table_name}")
job.commit()
