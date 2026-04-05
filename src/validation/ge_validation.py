# src/validation/ge_validation.py

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from src.utils.minio_client import MinIOClient
from src.utils.logger import get_logger

logger = get_logger("validation.ge")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GE_DIR        = _PROJECT_ROOT / "great_expectations"
SUITE_DIR     = GE_DIR / "expectations"

SUITE_FILES = {
    "students_suite":   SUITE_DIR / "students_suite.json",
    "grades_suite":     SUITE_DIR / "grades_suite.json",
    "ctsv_suite":       SUITE_DIR / "ctsv_suite.json",
    "tai_chinh_suite":  SUITE_DIR / "tai_chinh_suite.json",
    "warehouse_suite":  SUITE_DIR / "warehouse_suite.json",
}


# ──────────────────────────────────────────────────────────────
# Module-level helpers
# ──────────────────────────────────────────────────────────────

def _load_suite(suite_name: str) -> ExpectationSuite:
    fpath = SUITE_FILES.get(suite_name)
    if fpath is None:
        raise KeyError(f"Suite '{suite_name}' chua duoc dang ky trong SUITE_FILES")
    if not fpath.exists():
        raise FileNotFoundError(f"Khong tim thay suite file: {fpath}")

    with open(fpath, "r", encoding="utf-8") as f:
        suite_dict = json.load(f)

    suite = ExpectationSuite(**suite_dict)
    logger.info(f"  GE | Loaded '{suite_name}' ({len(suite.expectations)} expectations)")
    return suite


def _run_suite(df: pd.DataFrame, suite_name: str, asset_name: str) -> Dict[str, Any]:
    """Chạy một GE suite trên DataFrame, trả về dict kết quả chuẩn hoá."""
    if df is None or df.empty:
        logger.warning(f"  GE | [{suite_name}] DataFrame rong — skip")
        return {
            "suite_name": suite_name,
            "asset_name": asset_name,
            "success":    True,
            "evaluated":  0,
            "passed":     0,
            "failed":     0,
            "failures":   [],
            "statistics": {},
            "skipped":    True,
        }

    suite  = _load_suite(suite_name)
    ge_df  = ge.from_pandas(df, expectation_suite=suite)
    result = ge_df.validate(result_format="SUMMARY", catch_exceptions=True)

    stats      = result.statistics
    evaluated  = stats.get("evaluated_expectations", 0)
    passed     = stats.get("successful_expectations", 0)
    failed_cnt = stats.get("unsuccessful_expectations", 0)
    success    = bool(result.success)

    failures: List[str] = []
    for er in result.results:
        if not er.success:
            exp_type = er.expectation_config.expectation_type
            kwargs   = er.expectation_config.kwargs
            col      = kwargs.get("column", "")
            mostly   = kwargs.get("mostly", 1.0)
            rd       = er.result or {}

            msg = f"[{asset_name}] {exp_type}"
            if col:
                msg += f" | col='{col}'"
            if rd.get("unexpected_percent") is not None:
                msg += f" | unexpected={rd['unexpected_percent']:.1f}%"
            if mostly < 1.0:
                msg += f" | mostly={mostly}"
            if er.exception_info and er.exception_info.get("raised_exception"):
                msg += f" | EXC: {er.exception_info.get('exception_message', '')[:60]}"
            failures.append(msg)

    logger.info(
        f"  GE | [{suite_name}] {asset_name}: "
        f"{passed}/{evaluated} ({'OK' if success else 'FAIL'})"
    )
    for f in failures:
        logger.warning(f"    FAIL: {f}")

    return {
        "suite_name": suite_name,
        "asset_name": asset_name,
        "success":    success,
        "evaluated":  evaluated,
        "passed":     passed,
        "failed":     failed_cnt,
        "failures":   failures,
        "statistics": {
            "evaluated":       evaluated,
            "successful":      passed,
            "unsuccessful":    failed_cnt,
            "success_percent": stats.get("success_percent", 0.0),
        },
        "skipped": False,
    }


def _compute_overall_result(
    suite_results:   Dict[str, Any],
    all_failures:    List[str],
    total_evaluated: int,
) -> Dict[str, Any]:
    """
    Tính overall result với 3 trạng thái:
      - 'ok'      : có evaluate, không có failure
      - 'failed'  : có ít nhất 1 failure
      - 'no_data' : tất cả đều skip (không validate được gì)
    """
    all_skipped = all(r.get("skipped", False) for r in suite_results.values())

    if all_skipped or total_evaluated == 0:
        return {
            "success": False,
            "status":  "no_data",
            "reason":  "Tất cả DataFrames đều rỗng — không có gì để validate",
        }
    elif len(all_failures) == 0:
        return {
            "success": True,
            "status":  "ok",
            "reason":  None,
        }
    else:
        return {
            "success": False,
            "status":  "failed",
            "reason":  f"{len(all_failures)} expectations thất bại",
        }


# ──────────────────────────────────────────────────────────────
# Main class
# ──────────────────────────────────────────────────────────────

class DataValidator:

    def __init__(self):
        if not GE_DIR.exists():
            raise FileNotFoundError(f"Khong tim thay thu muc GE: {GE_DIR}")
        if not SUITE_DIR.exists():
            raise FileNotFoundError(f"Khong tim thay thu muc expectations: {SUITE_DIR}")
        logger.info(f"  GE | DataValidator khoi tao | GE dir: {GE_DIR}")

    # ──────────────────────────────────────────────────────────
    # Public methods
    # ──────────────────────────────────────────────────────────

    def validate_from_staging(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate raw + prepared data từ MinIO staging.
        Trả về dict chuẩn hoá với success / status / suite_results.
        """
        client = MinIOClient()

        if run_id is None:
            run_id = client.get_latest_run_id(bucket="raw")
        if run_id is None:
            raise FileNotFoundError("Không có staging data trong MinIO bucket 'raw'.")

        logger.info(f"  GE | Bắt đầu validation | run_id={run_id}")

        # ── Download dữ liệu raw ──────────────────────────────
        df_sv       = client.download_df("nguon1_sinh_vien.parquet",        run_id)
        df_diem     = client.download_df("nguon1_diem.parquet",             run_id)
        df_tong_hop = client.download_df("nguon1_tong_hop_ket_qua.parquet", run_id)
        df_ctsv     = client.download_df("nguon2_ctsv.parquet",             run_id)
        df_tc       = client.download_df("nguon3_tai_chinh.parquet",        run_id)

        logger.info(
            f"  GE | Downloaded raw: sv={len(df_sv)}, diem={len(df_diem)}, "
            f"tong_hop={len(df_tong_hop)}, ctsv={len(df_ctsv)}, tc={len(df_tc)}"
        )

        # ── Tầng 1: Validate raw data ─────────────────────────
        logger.info("  GE | [Tầng 1] Validate raw data...")
        raw_suite_results: Dict[str, Any] = {
            "grades_raw": _run_suite(
                df_diem,
                "grades_suite",
                "diem_hoc_phan_raw",
            ),
            "ctsv_raw": _run_suite(
                self._prepare_ctsv_df(df_ctsv),
                "ctsv_suite",
                "ctsv_data",
            ),
            "tai_chinh": _run_suite(
                self._prepare_tc_df(df_tc),
                "tai_chinh_suite",
                "tai_chinh_data",
            ),
        }

        # ── Tầng 2: Validate prepared / merged data ───────────
        logger.info("  GE | [Tầng 2] Validate prepared data...")
        df_students_prepared = self._prepare_students_df(df_sv, df_tong_hop)

        prepared_suite_results: Dict[str, Any] = {
            "students_prepared": _run_suite(
                df_students_prepared,
                "students_suite",
                "sinh_vien_prepared",
            ),
            "tong_hop_raw": self._validate_tong_hop_raw(df_tong_hop),
        }

        # ── Tổng hợp ──────────────────────────────────────────
        all_suite_results: Dict[str, Any] = {
            **raw_suite_results,
            **prepared_suite_results,
        }

        all_failures:    List[str] = []
        total_evaluated  = 0
        total_successful = 0

        for r in all_suite_results.values():
            if r.get("skipped"):
                continue
            total_evaluated  += r["evaluated"]
            total_successful += r["passed"]
            all_failures.extend(r["failures"])

        # _compute_overall_result là module-level function
        overall = _compute_overall_result(all_suite_results, all_failures, total_evaluated)

        # ── Log summary ───────────────────────────────────────
        logger.info("=" * 60)
        logger.info(f"  GE | VALIDATION {overall['status'].upper()}")
        logger.info(
            f"  GE | run_id: {run_id} | "
            f"{total_successful}/{total_evaluated} passed"
        )
        for name, r in all_suite_results.items():
            if r.get("skipped"):
                status = "SKIP"
            elif r["success"]:
                status = "OK  "
            else:
                status = "FAIL"
            logger.info(
                f"    {name:<25}: {status} "
                f"({r.get('passed', 0)}/{r.get('evaluated', 0)})"
            )
        if overall.get("reason"):
            logger.warning(f"  GE | Reason: {overall['reason']}")
        logger.info("=" * 60)

        return {
            "success":                 overall["success"],
            "status":                  overall["status"],   # 'ok' | 'failed' | 'no_data'
            "run_id":                  run_id,
            "evaluated_expectations":  total_evaluated,
            "successful_expectations": total_successful,
            "failed_expectations":     all_failures,
            "suite_results":           all_suite_results,
        }

    def validate_warehouse(self) -> Dict[str, Any]:
        """
        Validate bảng agg_student_summary trong Data Warehouse.
        Dùng cho weekly_summary_pipeline sau khi rebuild xong.
        """
        from src.config.database import warehouse_engine

        logger.info("  GE | Validate warehouse — agg_student_summary")

        df_agg = pd.read_sql(
            """
            SELECT
                ma_sinh_vien, gpa_he_4, muc_do_rui_ro,
                tong_no_hoc_phi, canh_bao_hoc_vu,
                diem_rl_trung_binh, co_no_hoc_phi
            FROM agg_student_summary
            """,
            warehouse_engine,
        )

        logger.info(f"  GE | agg_student_summary: {len(df_agg)} records")

        result       = _run_suite(df_agg, "warehouse_suite", "agg_student_summary")
        all_failures = result.get("failures", [])

        return {
            "success":                 result["success"],
            "status":                  "ok" if result["success"] else "failed",
            "evaluated_expectations":  result["evaluated"],
            "successful_expectations": result["passed"],
            "failed_expectations":     all_failures,
            "suite_results":           {"warehouse": result},
        }

    # ──────────────────────────────────────────────────────────
    # Private static helpers
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _prepare_students_df(
        df_sv: pd.DataFrame,
        df_tong_hop: pd.DataFrame,
    ) -> pd.DataFrame:
        df = df_sv.copy()

        # Merge GPA và canh_bao vào để GE có thể validate
        if not df_tong_hop.empty:
            df = df.merge(
                df_tong_hop[["ma_sinh_vien", "gpa_he_4", "canh_bao_hoc_vu"]],
                on="ma_sinh_vien",
                how="left",
            )

        for col in ["ho", "ten", "email", "khoa_hoc",
                    "trang_thai_hoc_tap", "ma_nganh", "ma_lop"]:
            if col not in df.columns:
                df[col] = None

        return df

    @staticmethod
    def _prepare_ctsv_df(df_ctsv: pd.DataFrame) -> pd.DataFrame:
        if df_ctsv.empty:
            return pd.DataFrame()

        df = df_ctsv.copy()
        for col in ["ma_sinh_vien", "hoc_ky", "diem_ren_luyen", "xep_loai_rl",
                    "muc_tien_hb", "hinh_thuc_ky_luat", "ly_do_ky_luat", "loai_hoc_bong"]:
            if col not in df.columns:
                df[col] = None

        df["diem_ren_luyen"] = pd.to_numeric(df["diem_ren_luyen"], errors="coerce")
        df["muc_tien_hb"]    = pd.to_numeric(df["muc_tien_hb"],    errors="coerce").fillna(0)
        return df

    @staticmethod
    def _prepare_tc_df(df_tc: pd.DataFrame) -> pd.DataFrame:
        if df_tc.empty:
            return pd.DataFrame()

        df = df_tc.copy()
        for col in ["ma_sinh_vien", "hoc_ky", "hoc_phi_phai_dong",
                    "da_dong", "con_no", "so_tien_mien_giam"]:
            if col not in df.columns:
                df[col] = None

        for col in ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        return df

    @staticmethod
    def _validate_tong_hop_raw(df_tong_hop: pd.DataFrame) -> Dict[str, Any]:
        """Inline validation cho tong_hop (không có GE suite riêng)."""
        if df_tong_hop is None or df_tong_hop.empty:
            return {
                "suite_name": "tong_hop_inline",
                "asset_name": "tong_hop_ket_qua",
                "success":    True,
                "evaluated":  0,
                "passed":     0,
                "failed":     0,
                "failures":   [],
                "statistics": {},
                "skipped":    True,
            }

        failures: List[str] = []
        evaluated = 0
        passed    = 0

        # Kiểm tra cột bắt buộc
        required_cols = ["ma_sinh_vien", "gpa_he_4", "canh_bao_hoc_vu"]
        for col in required_cols:
            evaluated += 1
            if col not in df_tong_hop.columns:
                failures.append(f"[tong_hop] Thiếu cột '{col}'")
            else:
                passed += 1

        # Kiểm tra gpa_he_4 trong khoảng hợp lệ [0, 4]
        if "gpa_he_4" in df_tong_hop.columns:
            evaluated += 1
            valid_gpa  = df_tong_hop["gpa_he_4"].dropna()
            out_range  = valid_gpa[(valid_gpa < 0) | (valid_gpa > 4)]
            if len(out_range) > 0:
                pct = len(out_range) / len(df_tong_hop) * 100
                failures.append(
                    f"[tong_hop] gpa_he_4 ngoài [0,4]: "
                    f"{len(out_range)} records ({pct:.1f}%)"
                )
            else:
                passed += 1

        # Kiểm tra ma_sinh_vien không null
        if "ma_sinh_vien" in df_tong_hop.columns:
            evaluated += 1
            null_count = df_tong_hop["ma_sinh_vien"].isna().sum()
            if null_count > 0:
                failures.append(
                    f"[tong_hop] ma_sinh_vien NULL: {null_count} records"
                )
            else:
                passed += 1

        success = len(failures) == 0
        return {
            "suite_name": "tong_hop_inline",
            "asset_name": "tong_hop_ket_qua",
            "success":    success,
            "evaluated":  evaluated,
            "passed":     passed,
            "failed":     evaluated - passed,
            "failures":   failures,
            "statistics": {
                "evaluated":       evaluated,
                "successful":      passed,
                "unsuccessful":    evaluated - passed,
                "success_percent": (passed / evaluated * 100) if evaluated > 0 else 0.0,
            },
            "skipped": False,
        }