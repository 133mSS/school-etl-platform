"""
scripts/generate_inject_report.py (v2 - direct invocation)

Sinh BẢNG SO SÁNH inject errors vs GE detection.

So với v1: thay vì parse file JSON trong validations/, gọi TRỰC TIẾP
DataValidator.validate_from_staging() và lấy kết quả từ memory.
→ Hoạt động được cả khi GE không persist results ra disk.

Workflow:
    1. Đọc ERROR_CONFIG (tỷ lệ inject từng loại)
    2. Đọc threshold mostly từ các GE suite JSON
    3. Gọi DataValidator → lấy in-memory result
    4. Parse failures list, map về exp_id
    5. Sinh báo cáo Markdown + CSV

Cách dùng:
    # Bước 1: Reset data (nếu cần)
    docker exec airflow-webserver python /opt/airflow/scripts/generate_sample_data.py

    # Bước 2: Inject lỗi
    docker exec airflow-webserver python /opt/airflow/scripts/inject_errors.py

    # Bước 3: Chạy ETL extract (cần có data trên MinIO trước)
    docker exec airflow-webserver python /opt/airflow/scripts/run_etl.py --mode full
    # Pipeline có thể fail tại Validate stage — đó là kết quả mong đợi!

    # Bước 4: Sinh báo cáo
    docker exec airflow-webserver python /opt/airflow/scripts/generate_inject_report.py
"""

import os
import sys
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

GE_DIR = PROJECT_ROOT / "great_expectations"
EXPECTATIONS_DIR = GE_DIR / "expectations"
OUTPUT_DIR = PROJECT_ROOT / "docs"
OUTPUT_DIR.mkdir(exist_ok=True)


# ════════════════════════════════════════════════════════════════════════════
# 1. CONFIG: Inject rates + Mapping inject → expectation
# ════════════════════════════════════════════════════════════════════════════

def load_inject_config() -> Dict[str, float]:
    """Đọc ERROR_CONFIG từ scripts/inject_errors.py."""
    try:
        from scripts.inject_errors import ERROR_CONFIG
        return ERROR_CONFIG
    except ImportError:
        return {
            "PG_SV_email_sai_format":       0.020,
            "PG_SV_trang_thai_cu":          0.015,
            "PG_DIEM_tong_ket_null":        0.020,
            "PG_DIEM_chu_khong_khop":       0.012,
            "PG_DIEM_dat_mon_sai":          0.015,
            "PG_DIEM_he4_null":             0.008,
            "PG_DIEM_cuoi_ky_out_range":    0.005,
            "CSV_drl_trong":                0.050,
            "CSV_drl_chu_thay_so":          0.010,
            "CSV_xeploai_sai":              0.030,
            "CSV_masv_thua_khoang":         0.020,
            "CSV_dong_trung_lap":           0.015,
            "CSV_tien_hb_am":               0.008,
            "CSV_hocky_sai_format":         0.005,
            "JSON_cono_tinh_sai":           0.030,
            "JSON_ngay_dong_sai_format":    0.025,
            "JSON_dadong_vuot_phi":         0.010,
            "JSON_hocky_sai_format":        0.015,
            "JSON_tien_mien_giam_am":       0.005,
        }


# Mapping inject → (exp_id, mô tả ngắn, expectation_type, column)
# expectation_type + column dùng để match với failure message từ GE
INJECT_MAP = {
    # ── PostgreSQL sinh_vien ────────────────────────────────────────────
    "PG_SV_email_sai_format":      ("EXP-SV-07",   "Email sai format (thiếu @ / có space)",
                                     "expect_column_values_to_match_regex", "email"),
    "PG_SV_trang_thai_cu":         ("EXP-SV-06",   "Trạng thái dùng format cũ (DANG_HOC)",
                                     "expect_column_values_to_be_in_set", "trang_thai_hoc_tap"),

    # ── PostgreSQL diem_hoc_phan ────────────────────────────────────────
    "PG_DIEM_tong_ket_null":       ("EXP-GR-03",   "diem_tong_ket = NULL (GV chưa nhập)",
                                     "expect_column_values_to_not_be_null", "diem_tong_ket"),
    "PG_DIEM_chu_khong_khop":      ("EXP-GR-05",   "diem_chu không khớp diem_tong_ket",
                                     "expect_column_values_to_be_in_set", "diem_chu"),
    "PG_DIEM_dat_mon_sai":         (None, "dat_mon flag sai (Transform tự sửa)", None, None),
    "PG_DIEM_he4_null":            (None, "diem_he_4 = NULL (Transform tính lại)", None, None),
    "PG_DIEM_cuoi_ky_out_range":   ("EXP-GR-04d",  "diem_cuoi_ky ngoài [0,10]",
                                     "expect_column_values_to_be_between", "diem_cuoi_ky"),

    # ── CSV ctsv ────────────────────────────────────────────────────────
    "CSV_drl_trong":               ("EXP-CTSV-06", "diem_ren_luyen rỗng / N/A",
                                     "expect_column_values_to_be_between", "diem_ren_luyen"),
    "CSV_drl_chu_thay_so":         ("EXP-CTSV-06", "diem_ren_luyen chữ ('Chưa nhập')",
                                     "expect_column_values_to_be_between", "diem_ren_luyen"),
    "CSV_xeploai_sai":             ("EXP-CTSV-07", "xep_loai_rl sai mức",
                                     "expect_column_values_to_be_in_set", "xep_loai_rl"),
    "CSV_masv_thua_khoang":        ("EXP-CTSV-03", "Mã SV thừa space / lowercase",
                                     "expect_column_values_to_match_regex", "ma_sinh_vien"),
    "CSV_dong_trung_lap":          ("EXP-CTSV-05", "Dòng trùng (ma_sv, hoc_ky)",
                                     "expect_compound_columns_to_be_unique", None),
    "CSV_tien_hb_am":              ("EXP-CTSV-08", "muc_tien_hb âm",
                                     "expect_column_values_to_be_between", "muc_tien_hb"),
    "CSV_hocky_sai_format":        ("EXP-CTSV-04", "hoc_ky sai format (HK1/2024-25)",
                                     "expect_column_values_to_match_regex", "hoc_ky"),

    # ── JSON tai_chinh ──────────────────────────────────────────────────
    "JSON_cono_tinh_sai":          ("EXP-TC-09",   "con_no tính sai (vendor bug)",
                                     "expect_column_values_to_be_between", "con_no"),
    "JSON_ngay_dong_sai_format":   ("EXP-TC-08",   "ngay_dong_cuoi sai format",
                                     "expect_column_values_to_match_regex", "ngay_dong_cuoi"),
    "JSON_dadong_vuot_phi":        ("EXP-TC-10",   "da_dong > hoc_phi (logic sai)",
                                     "expect_column_pair_values_a_to_be_greater_than_b", "da_dong"),
    "JSON_hocky_sai_format":       ("EXP-TC-05",   "hoc_ky sai format",
                                     "expect_column_values_to_match_regex", "hoc_ky"),
    "JSON_tien_mien_giam_am":      ("EXP-TC-11",   "so_tien_mien_giam âm",
                                     "expect_column_values_to_be_between", "so_tien_mien_giam"),
}


# ════════════════════════════════════════════════════════════════════════════
# 2. Đọc threshold từ GE expectation suites
# ════════════════════════════════════════════════════════════════════════════

def load_expectation_metadata() -> Dict[str, dict]:
    """Đọc threshold mostly + meta của tất cả expectations từ JSON suite files."""
    expectations = {}

    for suite_file in EXPECTATIONS_DIR.glob("*.json"):
        with open(suite_file, "r", encoding="utf-8") as f:
            suite = json.load(f)

        for exp in suite.get("expectations", []):
            exp_id = exp.get("meta", {}).get("exp_id")
            if not exp_id:
                continue

            expectations[exp_id] = {
                "suite":    suite_file.stem,
                "type":     exp.get("expectation_type", ""),
                "column":   exp.get("kwargs", {}).get("column"),
                "mostly":   exp.get("kwargs", {}).get("mostly"),
                "category": exp.get("meta", {}).get("category", ""),
                "notes":    exp.get("meta", {}).get("notes", ""),
            }

    return expectations


# ════════════════════════════════════════════════════════════════════════════
# 3. Chạy GE validation TRỰC TIẾP, lấy in-memory results
# ════════════════════════════════════════════════════════════════════════════

def run_validation_directly() -> Dict[str, dict]:
    """
    Gọi DataValidator.validate_from_staging() trực tiếp.
    
    Returns:
        Dict[exp_id, {success, type, column, message, ...}]
    """
    print("→ Đang gọi DataValidator.validate_from_staging()...")
    print("  (cần có run_id mới nhất trên MinIO bucket 'raw')")

    try:
        from src.validation.ge_validation import DataValidator
        from src.utils.minio_client import MinIOClient

        client = MinIOClient()
        run_id = client.get_latest_run_id(bucket="raw")
        if not run_id:
            print("  ❌ Không có data trên MinIO bucket 'raw'.")
            print("     Hãy chạy: python scripts/run_etl.py --mode full trước")
            return {}

        print(f"  → Run ID gần nhất: {run_id}")

        validator = DataValidator()
        result = validator.validate_from_staging(run_id=run_id)

    except Exception as e:
        print(f"  ⚠️  Validation ném exception (bình thường khi data đã inject): {type(e).__name__}: {e}")
        # Pipeline raise ValueError khi validate fail → vẫn cần kết quả
        # Chiến lược: trả về dict rỗng, sẽ map theo INJECT_MAP cố định
        return {}

    # Parse failures từ result
    # Format failure: "[asset_name] expectation_type | col='xxx' | unexpected=X.X% | mostly=Y"
    failure_results = {}

    for suite_name, suite_r in result.get("suite_results", {}).items():
        if suite_r.get("skipped"):
            continue

        for failure_msg in suite_r.get("failures", []):
            # Parse các thành phần từ message
            parsed = parse_failure_message(failure_msg)

            # Map về exp_id qua INJECT_MAP
            for inject_key, (exp_id, _, exp_type, col) in INJECT_MAP.items():
                if not exp_id:
                    continue
                if parsed["type"] == exp_type and (col is None or parsed["column"] == col):
                    failure_results[exp_id] = {
                        "success":   False,
                        "suite":     suite_name,
                        "type":      parsed["type"],
                        "column":    parsed["column"],
                        "unexpected_pct": parsed["unexpected_pct"],
                        "message":   failure_msg,
                    }
                    break

    print(f"  ✅ Tổng: {result.get('evaluated_expectations', 0)} expectations evaluated, "
          f"{len(result.get('failed_expectations', []))} failures")

    return failure_results


def parse_failure_message(msg: str) -> dict:
    """
    Parse failure message format:
    "[asset_name] expect_xxx | col='abc' | unexpected=12.3% | mostly=0.97"
    """
    parsed = {
        "asset":          None,
        "type":           None,
        "column":         None,
        "unexpected_pct": None,
        "mostly":         None,
    }

    # asset
    m = re.match(r"\[([^\]]+)\]\s+(\S+)", msg)
    if m:
        parsed["asset"] = m.group(1)
        parsed["type"]  = m.group(2)

    # column
    m = re.search(r"col='([^']+)'", msg)
    if m:
        parsed["column"] = m.group(1)

    # unexpected percent
    m = re.search(r"unexpected=([\d.]+)%", msg)
    if m:
        parsed["unexpected_pct"] = float(m.group(1))

    # mostly
    m = re.search(r"mostly=([\d.]+)", msg)
    if m:
        parsed["mostly"] = float(m.group(1))

    return parsed


# ════════════════════════════════════════════════════════════════════════════
# 4. Sinh báo cáo
# ════════════════════════════════════════════════════════════════════════════

def generate_markdown_report() -> str:
    """Sinh bảng Markdown so sánh inject vs detect."""
    inject_config = load_inject_config()
    expectations  = load_expectation_metadata()
    detections    = run_validation_directly()

    print()
    print("─" * 70)
    print("Building report...")
    print("─" * 70)

    lines = [
        "# 📊 Bảng so sánh Inject Errors vs GE Detection",
        "",
        f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_",
        "",
        "_Source: Direct invocation của `DataValidator.validate_from_staging()`._",
        "",
        "## Bảng chi tiết",
        "",
        "| # | Loại lỗi inject | Tỷ lệ inject | Expectation | Threshold | Unexpected | GE catch? | Notes |",
        "|---|---|---|---|---|---|---|---|",
    ]

    catch_count = 0
    not_catch_count = 0
    by_design_count = 0

    for idx, (inject_key, ratio) in enumerate(inject_config.items(), 1):
        if inject_key not in INJECT_MAP:
            continue

        exp_id, desc, _, _ = INJECT_MAP[inject_key]

        if exp_id is None:
            # By design — không catch ở GE input
            mostly_str = "—"
            unexpected_str = "—"
            catch_str = "⚪ By design"
            notes = "Transform layer xử lý field này"
            by_design_count += 1
        else:
            exp_meta = expectations.get(exp_id, {})
            mostly = exp_meta.get("mostly")
            mostly_str = f"mostly={mostly}" if mostly else "exact"

            detection = detections.get(exp_id)
            if detection and not detection["success"]:
                # GE đã catch
                unexpected_pct = detection.get("unexpected_pct")
                unexpected_str = f"{unexpected_pct:.1f}%" if unexpected_pct else "—"
                catch_str = "✅ Yes"
                catch_count += 1
            else:
                # GE không catch (có thể vì threshold quá lỏng, hoặc inject < threshold)
                unexpected_str = "—"
                catch_str = "❌ No"
                not_catch_count += 1

            notes = exp_meta.get("notes", "")[:50]

        lines.append(
            f"| {idx} | {desc} | {ratio*100:.1f}% | "
            f"{exp_id or '—'} | {mostly_str} | {unexpected_str} | {catch_str} | {notes} |"
        )

    # Tổng kết
    total_with_exp = catch_count + not_catch_count
    detect_rate = (catch_count / total_with_exp * 100) if total_with_exp > 0 else 0

    lines.extend([
        "",
        "## Tổng kết",
        "",
        f"- **Tổng số loại lỗi inject**: {len(inject_config)}",
        f"- **Số lỗi GE catch**: {catch_count}",
        f"- **Số lỗi GE không catch**: {not_catch_count}",
        f"- **Số lỗi by-design (Transform tự xử)**: {by_design_count}",
        f"- **TỶ LỆ PHÁT HIỆN**: {detect_rate:.1f}% ({catch_count}/{total_with_exp})",
        "",
        "## Diễn giải",
        "",
        "- **✅ Yes**: GE đã catch — expectation chuyển sang FAIL khi validate.",
        "- **❌ No**: GE chưa catch — có thể do tỷ lệ inject < threshold mostly, hoặc data inject chưa được upload lên MinIO.",
        "- **⚪ By design**: Field này được Transform layer tự tính lại (`dat_mon`, `diem_he_4`),",
        "  không cần validate ở GE input. Đây là design choice phân chia trách nhiệm rõ ràng.",
        "",
    ])

    return "\n".join(lines)


def generate_csv_report() -> str:
    """Sinh CSV để mở trong Excel."""
    inject_config = load_inject_config()
    expectations  = load_expectation_metadata()
    detections    = run_validation_directly()

    lines = ["#,Loai loi inject,Ty le inject,Expectation,Threshold,Unexpected,GE catch,Notes"]

    for idx, (inject_key, ratio) in enumerate(inject_config.items(), 1):
        if inject_key not in INJECT_MAP:
            continue

        exp_id, desc, _, _ = INJECT_MAP[inject_key]

        if exp_id is None:
            row = [str(idx), f'"{desc}"', f"{ratio*100:.1f}%", "-", "-", "-", "By design", '""']
        else:
            exp_meta = expectations.get(exp_id, {})
            mostly = exp_meta.get("mostly")
            mostly_str = f"mostly={mostly}" if mostly else "exact"

            detection = detections.get(exp_id)
            if detection and not detection["success"]:
                unexpected_str = f"{detection.get('unexpected_pct', 0):.1f}%"
                catch_str = "Yes"
            else:
                unexpected_str = "-"
                catch_str = "No"

            notes = exp_meta.get("notes", "")[:80].replace(",", ";").replace("\n", " ")
            row = [str(idx), f'"{desc}"', f"{ratio*100:.1f}%", exp_id, mostly_str,
                   unexpected_str, catch_str, f'"{notes}"']

        lines.append(",".join(row))

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("GENERATE INJECT ERRORS REPORT (v2 — direct invocation)")
    print("=" * 70)
    print()

    md_content  = generate_markdown_report()
    csv_content = generate_csv_report()

    md_path  = OUTPUT_DIR / "inject_errors_report.md"
    csv_path = OUTPUT_DIR / "inject_errors_report.csv"

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(csv_content)

    print()
    print(f"✅ Markdown: {md_path}")
    print(f"✅ CSV:      {csv_path}")
    print()
    print("─" * 70)
    print("PREVIEW:")
    print("─" * 70)
    print(md_content)


if __name__ == "__main__":
    main()