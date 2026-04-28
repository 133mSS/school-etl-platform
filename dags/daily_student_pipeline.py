from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from src.utils.metrics import push_validate_metrics
import logging

DEFAULT_ARGS = {
    "owner":            "nhom8",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          0,
    "retry_delay":      timedelta(minutes=5),
}
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# TASK 2: VALIDATE — dùng Great Expectations thật sự
# ════════════════════════════════════════════════════════════════════════════

def task_validate(**context):
    """Validate dữ liệu từ MinIO staging bằng Great Expectations."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.validation.ge_validation import DataValidator, build_data_docs
    from src.utils.metrics import push_validate_metrics

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="init_run_id")

    validator = DataValidator()
    result = validator.validate_from_staging(run_id=run_id)

    for suite_name, suite_r in result.get("suite_results", {}).items():
        if suite_r.get("skipped"):
            continue
        push_validate_metrics(
            suite=suite_name,
            evaluated=suite_r.get("evaluated", 0),
            successful=suite_r.get("successful", 0),
            failed=suite_r.get("failed", 0),
            run_id=run_id,
        )

    # Build HTML DataDocs
    build_data_docs()

    context["ti"].xcom_push(key="validation_result", value={
        "success":                 result["success"],
        "run_id":                  result["run_id"],
        "evaluated_expectations":  result["evaluated_expectations"],
        "successful_expectations": result["successful_expectations"],
        "failed_count":            len(result["failed_expectations"]),
    })

    if not result["success"]:
        failed = result.get("failed_expectations", [])

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
            + "\n".join(f"  - {e}" for e in failed[:10])
            + ("\n  ..." if len(failed) > 10 else "")
        )

    ev = result["evaluated_expectations"]
    ok = result["successful_expectations"]
    print(f"Validation OK: {ok}/{ev} expectations passed")
    print(f"GE suites: {list(result.get('suite_results', {}).keys())}")


# ════════════════════════════════════════════════════════════════════════════
# TASK 3: TRANSFORM
# ════════════════════════════════════════════════════════════════════════════

def task_transform(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract   import DataExtractor
    from src.etl.transform import DataTransformer

    # ⭐ FIX: dùng init_run_id thay vì extract_data
    run_id    = context["ti"].xcom_pull(key="run_id", task_ids="init_run_id")
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
    """Load dữ liệu vào Data Warehouse."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.transform import DataTransformer
    from src.etl.load      import DataLoader
    from src.utils.metrics import push_load_metrics

    staging_run_id = context["ti"].xcom_pull(
        key="staging_run_id", task_ids="transform_data"
    )

    if not staging_run_id:
        raise ValueError(
            "Không tìm thấy staging_run_id từ task transform_data."
        )

    transformer = DataTransformer()
    transformed = transformer.load_from_staging(run_id=staging_run_id)

    loader = DataLoader()

    try:
        stats = loader.load_all(transformed)
    except Exception:
        import traceback
        print("=" * 60)
        print("LOAD FAILED — Chi tiết lỗi:")
        print(traceback.format_exc())
        print("=" * 60)
        raise

    for table_name, count in stats.items():
        push_load_metrics(
            table=table_name,
            records=count,
            run_id=staging_run_id,
        )

    context["ti"].xcom_push(key="load_stats", value=stats)

    print("=" * 60)
    print("LOAD HOÀN TẤT — Thống kê:")
    total = 0
    for table, count in stats.items():
        print(f"  {table:<30s}: {count:>10,} records")
        total += count
    print(f"  {'TỔNG':<30s}: {total:>10,} records")
    print("=" * 60)

    return stats


# ════════════════════════════════════════════════════════════════════════════
# TASK 5: ALERT SUCCESS
# ════════════════════════════════════════════════════════════════════════════

def task_alert_success(**context):
    ti = context['ti']
    validation_result = ti.xcom_pull(
        task_ids='validate_data',
        key='validation_result'
    )
    load_stats = ti.xcom_pull(task_ids='load_data', key='load_stats')

    if validation_result is None:
        logger.warning("⚠️ validation_result is None.")
        print("=" * 50)
        print("⚠️ ALERT: Validation chưa chạy hoặc đã fail!")
        print("=" * 50)
        return

    successful = validation_result.get('successful_expectations', 0)
    evaluated  = validation_result.get('evaluated_expectations', 0)
    failed_ct  = validation_result.get('failed_count', 0)
    run_id     = validation_result.get('run_id', 'unknown')
    success    = validation_result.get('success', False)

    print("=" * 50)
    print(f"✅ Validation Summary | run_id: {run_id}")
    print(f"   Status: {'PASSED' if success else 'FAILED'}")
    print(f"   Evaluated: {evaluated}")
    print(f"   Passed:    {successful}")
    print(f"   Failed:    {failed_ct}")
    print("=" * 50)

    if load_stats:
        print("\n  Records loaded:")
        total = 0
        for table, count in load_stats.items():
            print(f"    {table:<25s}: {count:>10,}")
            total += count
        print(f"    {'TỔNG':<25s}: {total:>10,}")
    print("=" * 50)


# ════════════════════════════════════════════════════════════════════════════
# ALERT FAILURE CALLBACK
# ════════════════════════════════════════════════════════════════════════════

def task_alert_failure(context):
    """Gọi khi bất kỳ task nào fail."""
    task_instance = context.get("task_instance")
    exception     = context.get("exception")

    print("=" * 60)
    print("PIPELINE FAILED!")
    print(f"Task     : {task_instance.task_id}")
    print(f"Loi      : {exception}")
    print(f"Log URL  : {task_instance.log_url}")
    print("=" * 60)


from airflow.utils.task_group import TaskGroup


# ════════════════════════════════════════════════════════════════════════════
# EXTRACT TASKS — chạy SONG SONG cho 3 nguồn
# ════════════════════════════════════════════════════════════════════════════

def task_extract_postgres(**context):
    """Extract Nguồn 1: PostgreSQL (Phòng Đào tạo) — 10 bảng."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract import PostgreSQLExtractor
    from src.utils.minio_client import MinIOClient
    from src.utils.metrics import push_extract_metrics

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="init_run_id")

    extractor = PostgreSQLExtractor()
    pg_data = extractor.extract_all()

    client = MinIOClient()
    total = 0
    for table_name, df in pg_data.items():
        if df.empty:
            continue
        client.upload_df(df, f"nguon1_{table_name}.parquet", run_id, bucket="raw")
        total += len(df)

    push_extract_metrics(source="postgres", records=total, run_id=run_id)
    print(f"[Extract PG] {total:,} records | run_id={run_id}")
    return total


def task_extract_csv(**context):
    """Extract Nguồn 2: CSV (Phòng CTSV)."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract import CSVExtractor
    from src.utils.minio_client import MinIOClient
    from src.utils.metrics import push_extract_metrics

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="init_run_id")

    extractor = CSVExtractor()
    df = extractor.extract_all()

    if not df.empty:
        client = MinIOClient()
        client.upload_df(df, "nguon2_ctsv.parquet", run_id, bucket="raw")

    push_extract_metrics(source="csv", records=len(df), run_id=run_id)
    print(f"[Extract CSV] {len(df):,} records | run_id={run_id}")
    return len(df)


def task_extract_api(**context):
    """Extract Nguồn 3: REST API (Tài chính)."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.extract import APIExtractor, PostgreSQLExtractor
    from src.utils.minio_client import MinIOClient
    from src.utils.metrics import push_extract_metrics

    run_id = context["ti"].xcom_pull(key="run_id", task_ids="init_run_id")

    pg = PostgreSQLExtractor()
    hk_df = pg._read_table("hoc_ky_nam_hoc")
    semester_list = hk_df["ma_hoc_ky"].tolist() if not hk_df.empty else []

    extractor = APIExtractor()
    df = extractor.extract_all_semesters(semester_list)

    if not df.empty:
        client = MinIOClient()
        client.upload_df(df, "nguon3_tai_chinh.parquet", run_id, bucket="raw")

    push_extract_metrics(source="api", records=len(df), run_id=run_id)
    print(f"[Extract API] {len(df):,} records | run_id={run_id}")
    return len(df)


def task_init_run_id(**context):
    """Tạo run_id 1 lần, push XCom cho các task extract dùng chung."""
    from src.utils.minio_client import MinIOClient
    run_id = MinIOClient.make_run_id()
    context["ti"].xcom_push(key="run_id", value=run_id)
    print(f"Run ID: {run_id}")
    return run_id


# ════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION — có nhánh song song + TaskGroup
# ════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="daily_student_pipeline",
    default_args=DEFAULT_ARGS,
    description="ETL hằng ngày: Extract (parallel) → GE Validate → Transform → Load",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "daily", "production"],
    on_failure_callback=task_alert_failure,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    init_run_id = PythonOperator(
        task_id="init_run_id",
        python_callable=task_init_run_id,
        doc_md="Sinh run_id duy nhất cho 1 pipeline run, dùng chung qua XCom.",
    )

    with TaskGroup("extract_group", tooltip="Extract song song 3 nguồn") as extract_group:
        extract_pg = PythonOperator(
            task_id="extract_postgres",
            python_callable=task_extract_postgres,
        )
        extract_csv = PythonOperator(
            task_id="extract_csv",
            python_callable=task_extract_csv,
        )
        extract_api = PythonOperator(
            task_id="extract_api",
            python_callable=task_extract_api,
        )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate,
        doc_md="GE validate 4 nguồn + build DataDocs HTML.",
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform,
        doc_md="Chuẩn hoá, tính GPA, dedup, SCD2 detect.",
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=task_load,
        doc_md="Upsert dim, insert fact, rebuild aggregate.",
    )

    alert = PythonOperator(
        task_id="alert_success",
        python_callable=task_alert_success,
        doc_md="Tổng kết kết quả + push metrics.",
    )

    start >> init_run_id >> extract_group >> validate >> transform >> load >> alert >> end