
"""
Custom ETL metrics — push lên Prometheus Pushgateway.
Dùng để monitor pipeline thực sự, không chỉ infra.
"""
import os
from typing import Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway,
)

from src.utils.logger import get_logger

logger = get_logger("utils.metrics")

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")
JOB_NAME = "etl_pipeline"


def _push(registry: CollectorRegistry, run_id: str) -> None:
    """Push registry lên Pushgateway, fail-silent (không làm chết pipeline)."""
    try:
        push_to_gateway(
            PUSHGATEWAY_URL,
            job=JOB_NAME,
            registry=registry,
            grouping_key={"run_id": run_id},
        )
    except Exception as e:
        logger.warning(f"  metrics | Push thất bại: {e} (pipeline vẫn tiếp tục)")


def push_extract_metrics(source: str, records: int, run_id: str) -> None:
    """Push số records extract được từ 1 nguồn."""
    registry = CollectorRegistry()
    g = Gauge(
        "etl_records_extracted",
        "Số records extract từ nguồn",
        labelnames=["source"],
        registry=registry,
    )
    g.labels(source=source).set(records)
    _push(registry, run_id)


def push_validate_metrics(
    suite: str,
    evaluated: int,
    successful: int,
    failed: int,
    run_id: str,
) -> None:
    """Push kết quả Great Expectations từng suite."""
    registry = CollectorRegistry()

    g_eval = Gauge(
        "etl_ge_expectations_evaluated",
        "Số expectations được chạy",
        labelnames=["suite"],
        registry=registry,
    )
    g_pass = Gauge(
        "etl_ge_expectations_passed",
        "Số expectations đạt",
        labelnames=["suite"],
        registry=registry,
    )
    g_fail = Gauge(
        "etl_ge_expectations_failed",
        "Số expectations thất bại",
        labelnames=["suite"],
        registry=registry,
    )

    g_eval.labels(suite=suite).set(evaluated)
    g_pass.labels(suite=suite).set(successful)
    g_fail.labels(suite=suite).set(failed)
    _push(registry, run_id)


def push_load_metrics(table: str, records: int, run_id: str) -> None:
    """Push số records load vào warehouse."""
    registry = CollectorRegistry()
    g = Gauge(
        "etl_records_loaded",
        "Số records load vào warehouse",
        labelnames=["table"],
        registry=registry,
    )
    g.labels(table=table).set(records)
    _push(registry, run_id)


def push_pipeline_duration(stage: str, seconds: float, run_id: str) -> None:
    """Push thời gian từng stage."""
    registry = CollectorRegistry()
    g = Gauge(
        "etl_stage_duration_seconds",
        "Thời gian chạy 1 stage của pipeline",
        labelnames=["stage"],
        registry=registry,
    )
    g.labels(stage=stage).set(seconds)
    _push(registry, run_id)


def push_pipeline_status(success: bool, run_id: str) -> None:
    """Push trạng thái pipeline cuối cùng."""
    registry = CollectorRegistry()
    g = Gauge(
        "etl_pipeline_success",
        "1 nếu pipeline thành công, 0 nếu fail",
        registry=registry,
    )
    g.set(1 if success else 0)
    _push(registry, run_id)