from pathlib import Path
from airflow.sdk import DAG, Asset, task, TaskGroup

# Import the GlueJob Operator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Import the SQLCheckOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

# Import the S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET = "{{ var.value.s3_bucket }}"
CWD = Path(__file__).parent

# Create a variable named LANDING_PREFIX
# Assign the variable to a string representing 
# the top level folder where data intervals are stored
LANDING_PREFIX = "data_intervals"

# Create a variable named WAREHOUSE_LOCATION 
# Assign the variable to an S3 uri pointing to 
# the location where the iceberg warehouse should be stored
#  (format s3://bucket/location)
WAREHOUSE_LOCATION = f"s3://{S3_BUCKET}/iceberg-warehouse/"

# Create a variable named ICEBERG_SCRIPT that points to the absolute path
# for the glue_script within the raw directory
ICEBERG_SCRIPT = CWD / "glue_script.py"

# Create variables for the connection ids here
AWS_CONN_ID     = "aws_default"
ATHENA_CONN_ID  = "athena_default"
GLUE_ROLE_NAME  = "dev-lakehouse-glue-role"
GLUE_CONNECTION_NAME = "dev-glue-network"

# Keep this line unchanged
REGION = "us-east-1"

# Initialize an Asset that is emitted by the `run_pipeline` dag.
RAW_INGESTION_PENDING = Asset("raw_ingestion_pending")

# Initialize an Asset to represent completion of raw ingestion
RAW_INGESTION_COMPLETE = Asset("raw_ingestion_complete")

# Initialize a DAG 
# - Set the id to "raw"
# - Schedule the dag to trigger when the run_pipeline dag completes
# - Set maximum active runs to 1
# - Set maximum active tasks to 2
with DAG(
    dag_id="raw",
    schedule=[RAW_INGESTION_PENDING],
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    # Define an airflow python task named `capture_landing_keys`
    # - Set the task decorator argument `inlets` to the Asset that triggers
    #   the raw dag
    # - Include the following parameters in the function signature:
    #   - `s3_bucket` a templated string argument. Renders the s3_bucket name at runtime
    #   - `inlet_events` (context variable used for accessing that tasks's inlet events)
    @task(inlets=[RAW_INGESTION_PENDING])
    def capture_landing_keys(s3_bucket, *, inlet_events):
        
        # Pull the data interval value from the 
        # latest inlet Asset events metadata
        events = inlet_events[RAW_INGESTION_PENDING]
        if not events:
            raise ValueError("No raw ingestion pending asset events found.")
        data_interval = events[-1].extra["data_interval"]

        # Initialize an S3Hook
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Using the `LANDING_PREFIX` variable and the data_interval
        # pulled from the Asset, define the S3 prefix
        # for the data_interval folder (format: <landing>/<data_interval>/)
        prefix = f"{LANDING_PREFIX}/{data_interval}/"

        # Use the S3Hook's `list_prefix` method
        # to list the table prefixes stored in the data_interval
        # Push the output to the xcom
        table_prefixes = hook.list_prefixes(
            bucket_name=s3_bucket,
            prefix=prefix,
            delimiter="/",
        ) or []
        if not table_prefixes:
            raise ValueError(f"No landing table prefixes found under s3://{s3_bucket}/{prefix}")
        return [
            {
                "job_name": f"ingest_raw_{key.strip('/').split('/')[-1]}",
                "script_args": {
                    "--table": key.strip("/").split("/")[-1],
                    "--landing_path": f"s3://{s3_bucket}/{key}",
                    "--data_interval": data_interval,
                },
            }
            for key in sorted(table_prefixes)
        ]

    # Keep this line unchanged
    table_keys = capture_landing_keys(S3_BUCKET)

    # Fill in the partial configurations for the GlueJobOperator
    submit_glue_jobs = GlueJobOperator.partial(
        # Set the task_id to `ingest`
        task_id="ingest",

        # Set the aws_conn_id
        aws_conn_id=AWS_CONN_ID,

        # Set the script_location to the absolute path
        # for the raw glue script
        script_location=ICEBERG_SCRIPT.as_posix(),

        # Set the s3_bucket to your templated S3_BUCKET variable
        s3_bucket=S3_BUCKET,

        # Set the iam_role_name to the role defined in this file
        iam_role_name=GLUE_ROLE_NAME,

        # Set the reion_name to the region defined in this file
        region_name=REGION,

        # Set the attribute that ensures downstream tasks
        # wait for this task to complete
        wait_for_completion=True,

        # Set the attribute that ensures the glue script is replaced
        # each time this task is executed
        replace_script_file=True,

        # Keep this line unchanged
        map_index_template="{{ task.script_args['--table'] }}",

        # Set the attribute that ensures glue logs flow into the airflow UI
        verbose=True,
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "Connections": {"Connections": [GLUE_CONNECTION_NAME]},
            "DefaultArguments": {
                # Set the datalake format to "iceberg"
                "--datalake-formats": "iceberg",
                "--conf": (
                    # Register Iceberg's SQL extensions with Spark — adds support for
                    # MERGE INTO, CALL procedures, and other Iceberg-specific syntax.
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    # Define a Spark catalog named 'iceberg' backed by Iceberg's SparkCatalog
                    # implementation. Tables addressed as iceberg.<db>.<table> route through this.
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    # Use AWS Glue Data Catalog as the metadata store for this catalog —
                    # databases and tables are persisted to Glue, visible to Athena.
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    # Use Iceberg's native S3 file IO for reading and writing table data —
                    # avoids HDFS-style overhead and integrates with Iceberg's S3 optimizations.
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    # Root S3 location where this catalog stores Iceberg metadata and data files.
                    # New tables created via this catalog land under <warehouse>/<db>/<table>/.
                    f"--conf spark.sql.catalog.iceberg.warehouse={WAREHOUSE_LOCATION} "
                    # Only overwrite partitions that the incoming dataframe touches; leave
                    # other partitions intact. Required for partition-level upserts/replaces.
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic"
                ),
            },
        }
    ).expand_kwargs(table_keys)

    with TaskGroup("validate_raw") as validate_raw:

        # Pass the following SQL query to an SQLCheckOperator
        # Use an athena connection id
        SQLCheckOperator.partial(
            task_id="table_has_rows",
            conn_id=ATHENA_CONN_ID,
            hook_params={"aws_conn_id": AWS_CONN_ID},
        ).expand_kwargs(
            table_keys.map(lambda job: {
                "sql": f"""
                    SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
                    FROM raw.{job["script_args"]["--table"]}
                    WHERE data_interval = '{job["script_args"]["--data_interval"]}'
                """,
            })
        )
            

    # Define a task that sets `outlets` to the 
    # Asset used to trigger the transactions dag
    @task(inlets=[RAW_INGESTION_PENDING], outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(table_keys, *, inlet_events, outlet_events):

        # Add data_interval to the outlet's metadata
        data_interval = inlet_events[RAW_INGESTION_PENDING][-1].extra["data_interval"]
        outlet_events[RAW_INGESTION_COMPLETE].extra = {
            "data_interval": data_interval,
            "tables": [config["script_args"]["--table"] for config in table_keys],
        }

    # Set task dependencies
    # The glue job, validations, and the notification task
    # should run in sequential order
    submit_glue_jobs >> validate_raw >> notify_complete(table_keys)
