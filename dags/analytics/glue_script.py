import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Use getResolvedOptions to collect arguments passed to the script
# Isolate the "config" argument
args = getResolvedOptions(sys.argv, ["JOB_NAME", "config"])

# Use json.loads to convert the config argument 
# to a python dictionary
config = json.loads(args["config"])

table_name = config['table']
sql_s3_path = config['sql']

# Initialize a spark context
sc = SparkContext()

# Wrap the spark context in a GlueContext
glueContext = GlueContext(sc)

# Store the glue context's spark_session to the variable `spark`
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Using boto3, read the file stored at the bucket an key defined above
# Store the file's string content as the variable `sql_query`
s3_bucket, s3_key = sql_s3_path.replace("s3://", "", 1).split("/", 1)
s3 = boto3.client("s3")
sql_query = s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"].read().decode("utf-8")

# Using spark, execute the SQL query
# Store the output to a variable
result_df = spark.sql(sql_query)

# Create a string that points to the production table
# Use the catalog configured by the GlueJobOperator
target_table = f"iceberg.analytics.{table_name}"

# Write the SQL query's result to the production table
# Replace the table in full
(
    result_df
    .writeTo(target_table)
    .using("iceberg")
    .tableProperty("format-version", "2")
    .tableProperty("write.format.default", "parquet")
    .tableProperty("write.parquet.compression-codec", "snappy")
    .createOrReplace()
)

print(f"Completed Iceberg overwrite for {table_name}")

job.commit()
