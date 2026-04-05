"""
src/validation/ge_validation.py
================================
Validate dữ liệu từ MinIO staging bằng Great Expectations 0.18.8.

4 suite files trong great_expectations/expectations/:
  students_suite.json   → df_students  (sinh_vien PostgreSQL)
  grades_suite.json     → df_grades    (diem_hoc_phan PostgreSQL)
  ctsv_suite.json       → df_ctsv      (CSV Phong CTSV)
  tai_chinh_suite.json  → df_tc        (API JSON vendor)

THAY DOI so voi version cu (attendance_suite dung chung):
  - Tach attendance_suite thanh ctsv_suite + tai_chinh_suite
  - Moi suite chi chua expectations phu hop voi DataFrame cua no
  - Tranh KeyError khi column khong ton tai trong DataFrame
"""

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

# Mapping 4 suite rieng biet
# Cần thêm:
SUITE_FILES = {
    "students_suite":   SUITE_DIR / "students_suite.json",
    "grades_suite":     SUITE_DIR / "grades_suite.json",
    "ctsv_suite":       SUITE_DIR / "ctsv_suite.json",
    "tai_chinh_suite":  SUITE_DIR / "tai_chinh_suite.json",
    "warehouse_suite":  SUITE_DIR / "warehouse_suite.json",  # ← thêm
}


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
    if df is None or df.empty:
        logger.warning(f"  GE | [{suite_name}] DataFrame rong — skip")
        return {
            "suite_name": suite_name, "asset_name": asset_name,
            "success": True, "evaluated": 0, "passed": 0,
            "failed": 0, "failures": [], "statistics": {}, "skipped": True,
        }

    suite    = _load_suite(suite_name)
    ge_df    = ge.from_pandas(df, expectation_suite=suite)
    result   = ge_df.validate(result_format="SUMMARY", catch_exceptions=True)

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
            msg      = f"[{asset_name}] {exp_type}"
            if col:
                msg += f" | col='{col}'"
            if rd.get("unexpected_percent") is not None:
                msg += f" | unexpected={rd['unexpected_percent']:.1f}%"
            if mostly < 1.0:
                msg += f" | mostly={mostly}"
            if er.exception_info and er.exception_info.get("raised_exception"):
                msg += f" | EXC: {er.exception_info.get('exception_message','')[:60]}"
            failures.append(msg)

    logger.info(f"  GE | [{suite_name}] {asset_name}: {passed}/{evaluated} ({'OK' if success else 'FAIL'})")
    for f in failures:
        logger.warning(f"    FAIL: {f}")

    return {
        "suite_name": suite_name, "asset_name": asset_name,
        "success": success, "evaluated": evaluated,
        "passed": passed, "failed": failed_cnt,
        "failures": failures,
        "statistics": {
            "evaluated": evaluated, "successful": passed,
            "unsuccessful": failed_cnt,
            "success_percent": stats.get("success_percent", 0.0),
        },
    }


class DataValidator:
    def __init__(self):
        if not GE_DIR.exists():
            raise FileNotFoundError(f"Khong tim thay thu muc GE: {GE_DIR}")
        if not SUITE_DIR.exists():
            raise FileNotFoundError(f"Khong tim thay thu muc expectations: {SUITE_DIR}")
        logger.info(f"  GE | DataValidator khoi tao | GE dir: {GE_DIR}")


    
    def validate_from_staging(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        client = MinIOClient()
        if run_id is None:
            run_id = client.get_latest_run_id(bucket="raw")
            if run_id is None:
                raise FileNotFoundError("Khong co staging data trong MinIO.")

        logger.info(f"  GE | Bat dau validation | run_id={run_id}")

        df_sv       = client.download_df("nguon1_sinh_vien.parquet", run_id)
        df_diem     = client.download_df("nguon1_diem.parquet",      run_id)
        df_tong_hop = client.download_df("nguon1_tong_hop_ket_qua.parquet", run_id)
        df_ctsv     = client.download_df("nguon2_ctsv.parquet",      run_id)
        df_tc       = client.download_df("nguon3_tai_chinh.parquet", run_id)

        logger.info(
            f"  GE | Downloaded: sv={len(df_sv)}, diem={len(df_diem)}, "
            f"ctsv={len(df_ctsv)}, tc={len(df_tc)}"
        )

        df_students = self._prepare_students_df(df_sv, df_tong_hop)
        df_grades   = df_diem.copy() if not df_diem.empty else pd.DataFrame()
        df_ctsv_ok  = self._prepare_ctsv_df(df_ctsv)
        df_tc_ok    = self._prepare_tc_df(df_tc)

        logger.info("  GE | Chay 4 validation suites...")

        suite_results = {
            "students":   _run_suite(df_students, "students_suite",  "sinh_vien"),
            "grades":     _run_suite(df_grades,   "grades_suite",    "diem_hoc_phan"),
            "ctsv":       _run_suite(df_ctsv_ok,  "ctsv_suite",      "ctsv_data"),
            "tai_chinh":  _run_suite(df_tc_ok,    "tai_chinh_suite", "tai_chinh_data"),
        }

        all_failures: List[str] = []
        total_evaluated = 0
        total_successful = 0
        for r in suite_results.values():
            if r.get("skipped"):
                continue
            total_evaluated  += r["evaluated"]
            total_successful += r["passed"]
            all_failures.extend(r["failures"])

        overall_success = len(all_failures) == 0

        logger.info("=" * 60)
        logger.info(f"  GE | VALIDATION {'OK' if overall_success else 'FAILED'}")
        logger.info(f"  GE | run_id: {run_id} | {total_successful}/{total_evaluated} passed")
        for name, r in suite_results.items():
            status = "SKIP" if r.get("skipped") else ("OK" if r["success"] else "FAIL")
            logger.info(f"       {name:<12}: {status} ({r.get('passed',0)}/{r.get('evaluated',0)})")
        logger.info("=" * 60)

        return {
            "success":                 overall_success,
            "run_id":                  run_id,
            "evaluated_expectations":  total_evaluated,
            "successful_expectations": total_successful,
            "failed_expectations":     all_failures,
            "suite_results":           suite_results,
        }
# ge_validation.py — cần thêm hàm này:
    def validate_warehouse(self) -> Dict[str, Any]:
        """
        Validate bảng agg_student_summary trong Data Warehouse.
        Dùng cho weekly_summary_pipeline sau khi rebuild xong.
        """
        import pandas as pd
        from src.config.database import warehouse_engine

        logger.info("  GE | Validate warehouse — agg_student_summary")

        # Đọc từ warehouse DB, không phải MinIO
        df_agg = pd.read_sql("""
            SELECT 
                ma_sinh_vien, gpa_he_4, muc_do_rui_ro,
                tong_no_hoc_phi, canh_bao_hoc_vu,
                diem_rl_trung_binh, co_no_hoc_phi
            FROM agg_student_summary
        """, warehouse_engine)

        logger.info(f"  GE | agg_student_summary: {len(df_agg)} records")

        # Chạy warehouse_suite
        result = _run_suite(df_agg, "warehouse_suite", "agg_student_summary")

        # Trả về cùng format với validate_from_staging
        all_failures = result.get("failures", [])
        return {
            "success":                 result["success"],
            "evaluated_expectations":  result["evaluated"],
            "successful_expectations": result["passed"],
            "failed_expectations":     all_failures,
            "suite_results":           {"warehouse": result},
        }        

    # Cần sửa:
    @staticmethod
    def _prepare_students_df(df_sv, df_tong_hop):
        df = df_sv.copy()

        # Merge GPA và canh_bao vào để GE có thể validate
        if not df_tong_hop.empty:
            df = df.merge(
                df_tong_hop[["ma_sinh_vien", "gpa_he_4", "canh_bao_hoc_vu"]],
                on="ma_sinh_vien",
                how="left"
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
        df["muc_tien_hb"]    = pd.to_numeric(df["muc_tien_hb"], errors="coerce").fillna(0)
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