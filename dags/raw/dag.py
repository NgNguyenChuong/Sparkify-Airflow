from pathlib import Path

from airflow.sdk import DAG, Asset, task, TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator


CWD = Path(__file__).parent

S3_BUCKET = "{{ var.value.s3_bucket }}"
LANDING_PREFIX = "data_intervals"
WAREHOUSE_LOCATION = f"s3://{S3_BUCKET}/iceberg-warehouse/"
ICEBERG_SCRIPT = CWD / "glue_script.py"

AWS_CONN_ID = "aws_default"
ATHENA_CONN_ID = "athena_default"

GLUE_ROLE_NAME = "dev-lakehouse-glue-role"
GLUE_CONNECTION_NAME = "dev-glue-network"

REGION = "us-east-1"

RAW_INGESTION_PENDING = Asset("raw_ingestion_pending")
RAW_INGESTION_COMPLETE = Asset("raw_ingestion_complete")


with DAG(
    dag_id="raw",
    schedule=[RAW_INGESTION_PENDING],
    max_active_runs=1,
    max_active_tasks=2,
    catchup=False,
) as dag:

    @task(inlets=[RAW_INGESTION_PENDING])
    def capture_landing_keys(s3_bucket: str, *, inlet_events) -> list[dict]:
        events = inlet_events[RAW_INGESTION_PENDING]

        if not events:
            raise ValueError("No raw ingestion asset events found.")

        data_interval = events[-1].extra["data_interval"]
        interval_prefix = f"{LANDING_PREFIX}/{data_interval}/"

        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        table_prefixes = hook.list_prefixes(
            bucket_name=s3_bucket,
            prefix=interval_prefix,
            delimiter="/",
        ) or []

        if not table_prefixes:
            raise ValueError(
                f"No source tables found under s3://{s3_bucket}/{interval_prefix}"
            )

        glue_jobs = []

        for key in sorted(table_prefixes):
            table_name = key.strip("/").split("/")[-1]

            glue_jobs.append(
                {
                    "job_name": f"ingest_raw_{table_name}",
                    "script_args": {
                        "--table": table_name,
                        "--landing_path": f"s3://{s3_bucket}/{key}",
                        "--data_interval": data_interval,
                    },
                }
            )

        return glue_jobs

    table_jobs = capture_landing_keys(S3_BUCKET)

    submit_glue_jobs = GlueJobOperator.partial(
        task_id="ingest",
        aws_conn_id=AWS_CONN_ID,
        script_location=ICEBERG_SCRIPT.as_posix(),
        s3_bucket=S3_BUCKET,
        iam_role_name=GLUE_ROLE_NAME,
        region_name=REGION,
        wait_for_completion=True,
        replace_script_file=True,
        map_index_template="{{ task.script_args['--table'] }}",
        verbose=True,
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
            "DefaultArguments": {
                "--datalake-formats": "iceberg",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse={WAREHOUSE_LOCATION} "
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic"
                ),
            },
        },
    ).expand_kwargs(table_jobs)

    with TaskGroup("validate_raw") as validate_raw:
        SQLCheckOperator.partial(
            task_id="table_has_rows",
            conn_id=ATHENA_CONN_ID,
        ).expand_kwargs(
            table_jobs.map(
                lambda job: {
                    "sql": f"""
                        SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                        FROM raw.{job["script_args"]["--table"]}
                        WHERE data_interval = '{job["script_args"]["--data_interval"]}'
                    """,
                }
            )
        )

    @task(outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(table_jobs: list[dict], **context) -> None:
        if not table_jobs:
            raise ValueError("No table jobs were produced.")

        data_interval = table_jobs[0]["script_args"]["--data_interval"]
        tables = [job["script_args"]["--table"] for job in table_jobs]

        context["outlet_events"][RAW_INGESTION_COMPLETE].extra = {
            "data_interval": data_interval,
            "tables": tables,
        }

    submit_glue_jobs >> validate_raw >> notify_complete(table_jobs)