"""
dags/daily_student_pipeline.py — DAG chạy hàng ngày lúc 2:00 sáng
====================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner":            "nhom8",
    "depends_on_past":  False,
    "email_on_failure": False,   # tắt email, dùng custom alert
    "email_on_retry":   False,
    "retries":          0,
    "retry_delay":      timedelta(minutes=5),
}


def task_extract(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract import DataExtractor

    extractor = DataExtractor()
    data      = extractor.extract_full()
    summary   = data.summary()

    # Push summary vào XCom để monitor
    context["ti"].xcom_push(key="extract_summary", value=summary)
    context["ti"].xcom_push(key="run_id",          value=extractor._last_run_id)

    print(f"Extract xong: {summary}")
    return summary


def task_validate(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.validation.ge_validation import DataValidator

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="extract_data")

    validator = DataValidator()
    result    = validator.validate_from_staging(run_id=run_id)

    if not result["success"]:
        failed = result.get("failed_expectations", [])
        raise ValueError(
            f"Data validation FAILED! {len(failed)} expectation(s) failed:\n"
            + "\n".join(f"  - {e}" for e in failed[:5])
        )

    print(f"Validation OK: {result.get('evaluated_expectations', 0)} checks passed")
    context["ti"].xcom_push(key="validation_result", value=result)


def task_transform(**context):

    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract   import DataExtractor
    from src.etl.transform import DataTransformer


    run_id    = context["ti"].xcom_pull(key="run_id", task_ids="extract_data")
    extractor = DataExtractor()
    data      = extractor.load_from_staging(run_id=run_id)

    transformer  = DataTransformer()
    transformed  = transformer.transform_all(data)
    summary      = transformed.summary()

    context["ti"].xcom_push(key="transform_summary",  value=summary)
    context["ti"].xcom_push(key="staging_run_id",     value=transformer._last_run_id)

    print(f"Transform xong: {summary}")
    return summary


def task_load(**context):

    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.transform import DataTransformer
    from src.etl.load      import DataLoader


    staging_run_id = context["ti"].xcom_pull(
        key="staging_run_id", task_ids="transform_data"
    )
    transformer = DataTransformer()
    transformed = transformer.load_from_staging(run_id=staging_run_id)

    loader = DataLoader()
    stats  = loader.load_all(transformed)

    context["ti"].xcom_push(key="load_stats", value=stats)
    print(f"Load xong: {stats}")
    return stats


def task_alert_success(**context):

    extract_summary  = context["ti"].xcom_pull(key="extract_summary",  task_ids="extract_data")
    transform_summary = context["ti"].xcom_pull(key="transform_summary", task_ids="transform_data")
    load_stats       = context["ti"].xcom_pull(key="load_stats",       task_ids="load_data")

    print("=" * 60)
    print("PIPELINE DAILY STUDENT — THANH CONG")
    print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Extract:   {extract_summary}")
    print(f"Transform: {transform_summary}")
    print(f"Load:      {load_stats}")
    print("=" * 60)


def task_alert_failure(context):

    task_instance = context.get("task_instance")
    exception     = context.get("exception")

    print("=" * 60)
    print(f"PIPELINE FAILED!")
    print(f"Task: {task_instance.task_id}")
    print(f"Lỗi: {exception}")
    print(f"Log: {task_instance.log_url}")
    print("=" * 60)

    # Trong production: gửi email/Slack ở đây
    # send_slack_alert(f"Pipeline failed at task {task_instance.task_id}")

# DAG DEFINITION

with DAG(
    dag_id="daily_student_pipeline",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline hang ngay: Extract → Validate → Transform → Load",
    schedule_interval="0 2 * * *", 
    start_date=datetime(2026, 1, 1),
    catchup=False,                   
    tags=["etl", "daily", "production"],
    on_failure_callback=task_alert_failure,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=task_extract,
        doc_md="Extract dữ liệu từ PostgreSQL, CSV và API. Lưu staging vào MinIO.",
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
        doc_md="Great Expectations kiểm tra chất lượng. Fail -> dừng pipeline.",
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
        doc_md="Tính GPA, xếp loại, merge 3 nguồn -> TransformedData.",
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=task_load,
        doc_md="Load vào Data Warehouse.",
    )

    alert = PythonOperator(
        task_id="alert_success",
        python_callable=task_alert_success,
        doc_md="Log kết quả pipeline thành công.",
    )

    # ── Thứ tự thực thi ──
    start >> extract >> validate >> transform >> load >> alert >> end