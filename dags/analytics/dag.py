import json
from pathlib import Path
from airflow.sdk import DAG, Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


CWD                    = Path(__file__).parent
GLUE_SCRIPT            = CWD / "glue_script.py"
S3_BUCKET              = "{{ var.value.s3_bucket }}"
WAREHOUSE_PATH         = f"s3://{S3_BUCKET}/iceberg-warehouse/analytics/"
GLUE_ROLE_NAME         = "dev-lakehouse-glue-role"
GLUE_CONNECTION_NAME   = "dev-glue-network"
AWS_CONN_ID            = "aws_default"
REGION                 = "us-east-1"
TABLES                  = (
    "artist_facts",
    "session_facts",
    "song_facts",
    "user_facts",
)

# Initialize an Asset that is emitted on completion of the transactions dag
TRANSACTIONS_COMPLETE = Asset("transactions_complete")


# Initialize an Airflow DAG
# - Set the id to "analytics"
# - Trigger the dag when the final event is emitted from the transactions dag
# - Set the maximum active runs to 1
# - Set the maximum active tasks to 2
with DAG(
    dag_id="analytics",
    schedule=[TRANSACTIONS_COMPLETE],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    for table_name in TABLES:
        payload = {"table": table_name}

        GlueJobOperator(
            task_id             = f"promote_{table_name}",
            job_name            = f"analytics_{table_name}",
            script_location     = GLUE_SCRIPT.as_posix(),
            s3_bucket           = S3_BUCKET,
            iam_role_name       = GLUE_ROLE_NAME,
            replace_script_file = True,
            verbose             = True,
            region_name         = REGION,
            script_args         = {
                "--config"                  : json.dumps(payload),
                "--datalake-formats"        : "iceberg",
                "--enable-glue-datacatalog" : "",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                ),
            },
            create_job_kwargs   = {
                "GlueVersion": "5.0",
                "NumberOfWorkers": 2,
                "WorkerType": "G.1X",
                "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
            },
            wait_for_completion = True,
            aws_conn_id         = AWS_CONN_ID,
        )
