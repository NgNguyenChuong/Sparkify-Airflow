import json
from pathlib import Path

from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


CWD = Path(__file__).parent
SQL = CWD / "sql"
GLUE_SCRIPT = CWD / "glue_script.py"
S3_BUCKET = "{{ var.value.s3_bucket }}"
S3_SQL = "artifacts/analytics/sql/{{ dag_run.run_id }}/"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/iceberg-warehouse/analytics/"
GLUE_ROLE_NAME = "dev-lakehouse-glue-role"
GLUE_CONNECTION_NAME = "dev-glue-network"
AWS_CONN_ID = "aws_default"
REGION = "us-east-1"

TRANSACTIONS_COMPLETE = Asset("transactions_complete")


with DAG(
    dag_id="analytics",
    schedule=[TRANSACTIONS_COMPLETE],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    for query in sorted(SQL.glob("*.sql")):
        table_name = query.stem
        sql_key = S3_SQL + f"{table_name}.sql"

        @task(task_id=f"{table_name}_upload_sql")
        def upload(file_path: str, s3_bucket: str, s3_key: str, **context) -> str:
            ti = context["ti"]
            sql_template = Path(file_path).read_text()
            sql_rendered = ti.task.render_template(sql_template, context)

            hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            hook.load_string(
                string_data=sql_rendered,
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )
            return f"s3://{s3_bucket}/{s3_key}"

        payload = {
            "sql": f"s3://{S3_BUCKET}/{sql_key}",
            "table": table_name,
        }

        promote = GlueJobOperator(
            task_id=f"promote_{table_name}",
            job_name=f"analytics_{table_name}",
            script_location=GLUE_SCRIPT.as_posix(),
            s3_bucket=S3_BUCKET,
            iam_role_name=GLUE_ROLE_NAME,
            replace_script_file=True,
            verbose=True,
            region_name=REGION,
            script_args={
                "--config": json.dumps(payload),
                "--datalake-formats": "iceberg",
                "--enable-glue-datacatalog": "",
                "--conf": (
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                ),
            },
            create_job_kwargs={
                "GlueVersion": "5.0",
                "NumberOfWorkers": 2,
                "WorkerType": "G.1X",
                "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
            },
            wait_for_completion=True,
            aws_conn_id=AWS_CONN_ID,
        )

        upload(query.as_posix(), S3_BUCKET, sql_key) >> promote
