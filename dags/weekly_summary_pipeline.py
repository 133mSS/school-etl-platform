from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator

DEFAULT_ARGS = {
    "owner":           "nhom8",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
}


def task_aggregate_weekly(**context):
    """
    Tính lại toàn bộ agg_student_summary từ data warehouse.
    Đây là "weekly refresh" — đảm bảo số liệu tổng hợp chính xác
    sau 1 tuần chạy daily pipeline.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.aggregation import WeeklyAggregator

    agg    = WeeklyAggregator()
    result = agg.run()

    context["ti"].xcom_push(key="agg_result", value=result)
    print(f"Weekly aggregation xong: {result}")
    return result


def task_generate_report(**context):
    """
    Tạo báo cáo tóm tắt tuần:
    - Số SV cảnh báo học vụ mới
    - Số SV đủ điều kiện học bổng
    - Tỷ lệ đạt môn trung bình
    - Top 10 môn có tỷ lệ rớt cao nhất
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.etl.aggregation import WeeklyReporter

    reporter = WeeklyReporter()
    report   = reporter.generate()

    print("=" * 60)
    print("BÁO CÁO TUẦN")
    print(f"  SV cảnh báo học vụ : {report.get('sv_canh_bao', 0)}")
    print(f"  SV đủ ĐK học bổng  : {report.get('sv_hoc_bong', 0)}")
    print(f"  Tỷ lệ đạt môn TB   : {report.get('ty_le_dat', 0):.1f}%")
    print("=" * 60)

    context["ti"].xcom_push(key="weekly_report", value=report)


with DAG(
    dag_id="weekly_summary_pipeline",
    default_args=DEFAULT_ARGS,
    description="Tổng hợp báo cáo tuần — mỗi thứ Hai 6:00 sáng",
    schedule_interval="0 6 * * 1",   # Thứ Hai 6:00 sáng
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "weekly", "report"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    aggregate = PythonOperator(
        task_id="aggregate_weekly",
        python_callable=task_aggregate_weekly,
        doc_md="Refresh agg_student_summary từ toàn bộ data warehouse.",
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        doc_md="Tạo báo cáo tóm tắt tuần.",
    )

    start >> aggregate >> report >> end