from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


SOURCE_BUCKET = "udacity-content2-dend"
DEST_BUCKET   = "{{ var.value.s3_bucket }}"
AWS_CONN_ID   = "aws_default"
MAX_WORKERS   = 32


with DAG(
    dag_id="setup_s3_data",
    schedule=None,  # Manually triggered
    max_active_runs=1,
    tags=["setup", "bootstrap"],
) as dag:

    # Copy every object from the public udacity-content2-dend bucket into
    # the student's project bucket using a thread pool of boto3 clients.
    # I/O-bound calls parallelize cleanly, so 32 workers gives ~30x speedup
    # over a sequential loop with no extra dependencies.
    @task
    def copy_test_data(**context) -> int:

        dest_bucket = context["templates_dict"]["dest_bucket"]
        hook        = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Pull a configured boto3 client off the hook — picks up the same
        # credentials and region as the rest of the project.
        s3 = hook.get_conn()

        source_keys = hook.list_keys(bucket_name=SOURCE_BUCKET) or []

        if not source_keys:
            raise ValueError(
                f"No files found in s3://{SOURCE_BUCKET}/. "
                f"Verify the bucket name and that your AWS credentials have read access."
            )

        files_to_copy = [k for k in source_keys if not k.endswith("/")]
        total         = len(files_to_copy)

        # Log every ~5% of progress, bounded to at least every 10 files.
        log_every = max(1, min(total // 20, 100))

        print(f"Copying {total} files from s3://{SOURCE_BUCKET}/ to s3://{dest_bucket}/ "
              f"using {MAX_WORKERS} parallel workers")

        def copy_one(key: str) -> None:
            s3.copy_object(
                CopySource={"Bucket": SOURCE_BUCKET, "Key": key},
                Bucket=dest_bucket,
                Key=key,
            )

        completed = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(copy_one, k) for k in files_to_copy]
            for future in as_completed(futures):
                future.result()  # surface exceptions
                completed += 1

                if completed % log_every == 0 or completed == total:
                    pct = (completed / total) * 100
                    bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
                    print(f"[{bar}] {completed:>5}/{total} ({pct:5.1f}%)")

        print(f"Done — copied {total} files.")
        return total

    copy_test_data.override(templates_dict={"dest_bucket": DEST_BUCKET})()