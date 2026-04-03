from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner":            "nhom8",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          0,
    "retry_delay":      timedelta(minutes=5),
}


# ════════════════════════════════════════════════════════════════════════════
# TASK 1: EXTRACT
# ════════════════════════════════════════════════════════════════════════════

def task_extract(**context):
    """
    Extract dữ liệu từ 3 nguồn:
      - Nguồn 1: PostgreSQL (học vụ)
      - Nguồn 2: CSV (Phòng CTSV)
      - Nguồn 3: API JSON (tài chính)
    Upload kết quả lên MinIO bucket raw-data.
    Push run_id vào XCom để các task sau dùng.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract import DataExtractor

    extractor = DataExtractor()
    data      = extractor.extract_full()
    summary   = data.summary()

    context["ti"].xcom_push(key="extract_summary", value=summary)
    context["ti"].xcom_push(key="run_id",          value=extractor._last_run_id)

    print(f"Extract xong: {summary}")
    return summary


# ════════════════════════════════════════════════════════════════════════════
# TASK 2: VALIDATE — dùng Great Expectations thật sự
# ════════════════════════════════════════════════════════════════════════════

def task_validate(**context):
    """
    Validate dữ liệu từ MinIO staging bằng Great Expectations.

    Luồng:
      1. DataValidator lấy run_id từ XCom
      2. Download parquet từ MinIO raw-data
      3. Load ExpectationSuite từ great_expectations/expectations/*.json
      4. Validate từng nguồn với ge.from_pandas()
      5. Nếu có expectation thất bại → raise ValueError → Airflow mark FAIL
         → các task downstream không chạy (transform, load không chạy với data xấu)

    Kết quả push vào XCom để alert_success có thể báo cáo.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    # Import DataValidator đã dùng GE thật
    from src.validation.ge_validation import DataValidator

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="extract_data")

    validator = DataValidator()
    result    = validator.validate_from_staging(run_id=run_id)

    # Lưu kết quả để alert_success báo cáo
    context["ti"].xcom_push(key="validation_result", value={
        "success":                 result["success"],
        "run_id":                  result["run_id"],
        "evaluated_expectations":  result["evaluated_expectations"],
        "successful_expectations": result["successful_expectations"],
        "failed_count":            len(result["failed_expectations"]),
    })

    # ── Xử lý kết quả ────────────────────────────────────────────────────
    if not result["success"]:
        failed = result.get("failed_expectations", [])

        # In chi tiết từng suite để dễ debug
        print("=" * 60)
        print("GREAT EXPECTATIONS — VALIDATION FAILED")
        print("=" * 60)
        for suite_name, suite_r in result.get("suite_results", {}).items():
            if not suite_r.get("success") and not suite_r.get("skipped"):
                print(f"  Suite '{suite_name}': {suite_r['failed']} failures")
                for f in suite_r.get("failures", []):
                    print(f"    - {f}")
        print("=" * 60)

        raise ValueError(
            f"Great Expectations validation FAILED!\n"
            f"  {len(failed)} expectation(s) không đạt:\n"
            + "\n".join(f"  - {e}" for e in failed[:10])   # in tối đa 10
            + ("\n  ..." if len(failed) > 10 else "")
        )

    # Success
    ev  = result["evaluated_expectations"]
    ok  = result["successful_expectations"]
    print(f"Validation OK: {ok}/{ev} expectations passed")
    print(f"GE suites: {list(result.get('suite_results', {}).keys())}")


# ════════════════════════════════════════════════════════════════════════════
# TASK 3: TRANSFORM
# ════════════════════════════════════════════════════════════════════════════

def task_transform(**context):
    """
    Transform dữ liệu từ MinIO staging:
      - Chuẩn hóa, xử lý NULL, loại trùng lặp
      - Tính GPA, xếp loại học lực, điểm rèn luyện
      - Merge 3 nguồn → TransformedData
      - Upload kết quả lên MinIO staging-data bucket
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract   import DataExtractor
    from src.etl.transform import DataTransformer

    run_id    = context["ti"].xcom_pull(key="run_id", task_ids="extract_data")
    extractor = DataExtractor()
    data      = extractor.load_from_staging(run_id=run_id)

    transformer = DataTransformer()
    transformed = transformer.transform_all(data)
    summary     = transformed.summary()

    context["ti"].xcom_push(key="transform_summary",  value=summary)
    context["ti"].xcom_push(key="staging_run_id",     value=transformer._last_run_id)

    print(f"Transform xong: {summary}")
    return summary


# ════════════════════════════════════════════════════════════════════════════
# TASK 4: LOAD
# ════════════════════════════════════════════════════════════════════════════

def task_load(**context):
    """
    Load dữ liệu vào Data Warehouse:
      - Upsert các Dimension tables (dim_sinh_vien SCD2, dim_hoc_phan...)
      - Insert Fact tables (fact_hoc_tap, fact_ctsv, fact_tai_chinh)
      - Rebuild agg_student_summary từ 3 nguồn
    """
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


# ════════════════════════════════════════════════════════════════════════════
# TASK 5: ALERT SUCCESS
# ════════════════════════════════════════════════════════════════════════════

def task_alert_success(**context):
    """Tổng hợp và in kết quả pipeline."""
    ti = context["ti"]

    extract_summary   = ti.xcom_pull(key="extract_summary",   task_ids="extract_data")
    validation_result = ti.xcom_pull(key="validation_result", task_ids="validate_data")
    transform_summary = ti.xcom_pull(key="transform_summary", task_ids="transform_data")
    load_stats        = ti.xcom_pull(key="load_stats",        task_ids="load_data")

    print("=" * 60)
    print("PIPELINE DAILY STUDENT — THANH CONG")
    print(f"Thoi gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Extract  : {extract_summary}")
    print(
        f"Validate : "
        f"{validation_result.get('successful_expectations','?')}/"
        f"{validation_result.get('evaluated_expectations','?')} passed"
    )
    print(f"Transform: {transform_summary}")
    print(f"Load     : {load_stats}")
    print("=" * 60)


# ════════════════════════════════════════════════════════════════════════════
# ALERT FAILURE CALLBACK
# ════════════════════════════════════════════════════════════════════════════

def task_alert_failure(context):
    """Gọi khi bất kỳ task nào fail. In thông tin để debug."""
    task_instance = context.get("task_instance")
    exception     = context.get("exception")

    print("=" * 60)
    print("PIPELINE FAILED!")
    print(f"Task     : {task_instance.task_id}")
    print(f"Loi      : {exception}")
    print(f"Log URL  : {task_instance.log_url}")
    print("=" * 60)

    # Production: gửi Slack/email alert ở đây


# ════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="daily_student_pipeline",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline hang ngay: Extract → GE Validate → Transform → Load",
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
        doc_md=(
            "Extract từ PostgreSQL, CSV và API JSON. "
            "Upload staging vào MinIO raw-data. "
            "Push run_id vào XCom."
        ),
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
        doc_md=(
            "Great Expectations validate 3 nguồn dữ liệu. "
            "Suites: students_suite, grades_suite, attendance_suite. "
            "FAIL → pipeline dừng ngay, không transform/load data xấu."
        ),
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
        doc_md=(
            "Chuẩn hóa dữ liệu, tính GPA, xếp loại. "
            "Merge 3 nguồn → TransformedData. "
            "Upload vào MinIO staging-data."
        ),
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=task_load,
        doc_md=(
            "Load vào Data Warehouse (PostgreSQL). "
            "Upsert dimensions, insert facts. "
            "Rebuild agg_student_summary từ 3 nguồn."
        ),
    )

    alert = PythonOperator(
        task_id="alert_success",
        python_callable=task_alert_success,
        doc_md="Log tổng kết kết quả pipeline.",
    )
