"""
scripts/inject_errors.py

Inject lỗi thực tế vào dữ liệu để demo Great Expectations bắt được data bẩn.
Mỗi loại inject MAP TRỰC TIẾP đến 1 expectation trong GE suite — không inject
loại nào mà GE không catch được (tránh demo vô nghĩa khi bảo vệ).

Map inject ↔ GE expectation:
  PostgreSQL sinh_vien    → students_suite.json (EXP-SV-06, EXP-SV-07)
  PostgreSQL diem_hphan   → grades_suite.json (EXP-GR-03, GR-04d, GR-04e, GR-05)
  CSV ctsv                → ctsv_suite.json (EXP-CTSV-03→09)
  JSON taichinh           → tai_chinh_suite.json (EXP-TC-04→11)
"""

import os
import sys
import json
import glob
import shutil
import random
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ── Path setup ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Kết nối database ────────────────────────────────────────────────────────
import os

DB_HOST = os.getenv("POSTGRES_SOURCE_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_SOURCE_PORT", "5434")
DB_USER = os.getenv("POSTGRES_SOURCE_USER", "school_user")
DB_PASS = os.getenv("POSTGRES_SOURCE_PASSWORD", "school_pass")
DB_NAME = os.getenv("POSTGRES_SOURCE_DB", "school_source")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL, echo=False)

# ── Thư mục dữ liệu ─────────────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR  = DATA_DIR / "csv"
JSON_DIR = DATA_DIR / "api_json"
BCK_DIR  = DATA_DIR / "_backup_before_inject"

random.seed(2024)   # seed cố định → kết quả lặp lại được mỗi lần chạy

# ════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH TỶ LỆ LỖI — mỗi entry đều có expectation tương ứng trong GE suite
# ════════════════════════════════════════════════════════════════════════════
ERROR_CONFIG = {

    # ── Nguồn 1: PostgreSQL - sinh_vien ──────────────────────────────────
    # Lý do thực tế: dữ liệu migrate từ hệ thống quản lý cũ
    "PG_SV_email_sai_format":       0.020,  # → EXP-SV-07 (mostly=0.98)
    "PG_SV_trang_thai_cu":          0.015,  # → EXP-SV-06 (mostly=0.97)

    # ── Nguồn 1: PostgreSQL - diem_hoc_phan ──────────────────────────────
    # Lý do thực tế: GV nhập điểm muộn, hoặc bypass validation
    "PG_DIEM_tong_ket_null":        0.020,  # → EXP-GR-03  (mostly=0.97)
    "PG_DIEM_chu_khong_khop":       0.012,  # → EXP-GR-05  (mostly=0.986)
    "PG_DIEM_dat_mon_sai":          0.015,  # → transform sẽ flag (không vào GE)
    "PG_DIEM_he4_null":             0.008,  # → expectation he_4 NOT NULL nếu có
    "PG_DIEM_cuoi_ky_out_range":    0.005,  # → EXP-GR-04d (mostly=0.994)

    # ── Nguồn 2: CSV - ctsv ──────────────────────────────────────────────
    # Lý do thực tế: nhân viên xuất từ Excel, nhập tay một phần
    "CSV_drl_trong":                0.050,  # → EXP-CTSV-06 (mostly=0.92)
    "CSV_xeploai_sai":              0.030,  # → EXP-CTSV-07 (mostly=0.88)
    "CSV_masv_thua_khoang":         0.020,  # → EXP-CTSV-03 (regex, mostly=0.93)
    "CSV_dong_trung_lap":           0.015,  # → EXP-CTSV-05 (compound unique)
    "CSV_tien_hb_am":               0.008,  # → EXP-CTSV-08 (>=0, mostly=0.99)
    "CSV_hocky_sai_format":         0.005,  # → EXP-CTSV-04 (regex, mostly=0.993)
    "CSV_drl_chu_thay_so":          0.010,  # → EXP-CTSV-06 (chữ không phải số)

    # ── Nguồn 3: JSON - taichinh ─────────────────────────────────────────
    # Lý do thực tế: vendor API làm tròn, format không nhất quán
    "JSON_cono_tinh_sai":           0.030,  # → EXP-TC-09 (mostly=0.968)
    "JSON_ngay_dong_sai_format":    0.025,  # → transform sẽ check format
    "JSON_dadong_vuot_phi":         0.010,  # → transform sẽ recalculate
    "JSON_hocky_sai_format":        0.025,  # → EXP-TC-05 (regex, mostly=0.983)
    "JSON_tien_mien_giam_am":       0.012,  # → EXP-TC-10 (>=0, mostly=0.994)
    "JSON_thieu_truong":            0.008,  # → EXP-TC-11 (column set)
}

# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def pick(lst, rate):
    """Chọn ngẫu nhiên ~rate*100% phần tử từ list, trả về set các index."""
    n = max(1, int(len(lst) * rate))
    return set(random.sample(range(len(lst)), min(n, len(lst))))


def _diem_to_chu_sai(diem_tong_ket):
    """Trả về điểm chữ SAI so với điểm số (dịch 1-2 bậc)."""
    thang = [
        (9.5, "A+"), (8.5, "A"), (8.0, "B+"), (7.0, "B"),
        (6.5, "C+"), (5.5, "C"), (5.0, "D+"), (4.0, "D"), (0.0, "F"),
    ]
    idx_dung = len(thang) - 1
    for i, (nguong, _) in enumerate(thang):
        if diem_tong_ket >= nguong:
            idx_dung = i
            break
    delta = random.choice([-2, -1, 1, 2])
    idx_sai = max(0, min(len(thang) - 1, idx_dung + delta))
    return thang[idx_sai][1]


def _xep_loai_rl_sai(drl):
    """Trả về xep_loai_rl SAI so với điểm rèn luyện."""
    dung_map = [(90, "Xuất sắc"), (80, "Tốt"), (65, "Khá"),
                (50, "Trung bình"), (35, "Yếu"), (0, "Kém")]
    tat_ca = ["Xuất sắc", "Tốt", "Khá", "Trung bình", "Yếu", "Kém"]
    dung = "Kém"
    for nguong, xeploai in dung_map:
        if drl >= nguong:
            dung = xeploai
            break
    return random.choice([x for x in tat_ca if x != dung])


# ════════════════════════════════════════════════════════════════════════════
# INJECT VÀO POSTGRESQL
# ════════════════════════════════════════════════════════════════════════════

def inject_postgres_sinh_vien(dry_run: bool) -> dict:
    """Inject email sai format + trang_thai cũ. (Đã bỏ gioi_tinh, ngay_sinh
    vì students_suite không có expectation tương ứng — demo sẽ vô nghĩa.)"""
    print("\n" + "─" * 60)
    print("📋 NGUỒN 1 — PostgreSQL: bảng sinh_vien")
    print("─" * 60)

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ma_sinh_vien, email, trang_thai_hoc_tap "
            "FROM sinh_vien ORDER BY ma_sinh_vien"
        )).fetchall()

    total = len(rows)
    print(f"  Tổng SV: {total:,}")

    stats = {}
    updates = []
    ma_svs = [r.ma_sinh_vien for r in rows]

    # ── Lỗi 1: Email sai format → EXP-SV-07 ──────────────────────────────
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_email_sai_format"])
    for i in idxs:
        r = rows[i]
        # 2 variants: bỏ @ hoàn toàn hoặc thêm khoảng trắng quanh @
        bad_email = random.choice([
            r.email.replace("@", ""),
            r.email.replace("@", " @ "),
        ])
        updates.append((
            "UPDATE sinh_vien SET email = :email WHERE ma_sinh_vien = :ma",
            {"email": bad_email, "ma": r.ma_sinh_vien}
        ))
    stats["email_sai_format"] = len(idxs)
    print(f"  ✏️  email sai format         : {len(idxs):>4} bản ghi")

    # ── Lỗi 2: Trạng thái dùng format cũ → EXP-SV-06 ─────────────────────
    tt_cu_map = {
        "Đang học":   ["dang hoc", "DANG_HOC"],
        "Bảo lưu":    ["bao luu",  "BAO_LUU"],
        "Thôi học":   ["thoi hoc", "THOI_HOC"],
        "Tốt nghiệp": ["tot nghiep", "TOT_NGHIEP"],
    }
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_trang_thai_cu"])
    for i in idxs:
        r = rows[i]
        bad_tt = random.choice(tt_cu_map.get(r.trang_thai_hoc_tap, ["dang hoc"]))
        updates.append((
            "UPDATE sinh_vien SET trang_thai_hoc_tap = :tt WHERE ma_sinh_vien = :ma",
            {"tt": bad_tt, "ma": r.ma_sinh_vien}
        ))
    stats["trang_thai_cu"] = len(idxs)
    print(f"  ✏️  trang_thai format cũ     : {len(idxs):>4} bản ghi")

    # ── Thực thi ─────────────────────────────────────────────────────────
    if not dry_run and updates:
        with engine.begin() as conn:
            for sql, params in updates:
                conn.execute(text(sql), params)
        print(f"  ✅ Đã UPDATE {len(updates)} bản ghi vào sinh_vien")
    elif dry_run:
        print(f"  🔍 [DRY RUN] Sẽ UPDATE {len(updates)} bản ghi")

    return stats


def inject_postgres_diem(dry_run: bool) -> dict:
    """Inject 5 loại lỗi vào diem_hoc_phan."""
    print("\n" + "─" * 60)
    print("📋 NGUỒN 1 — PostgreSQL: bảng diem_hoc_phan")
    print("─" * 60)

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ma_diem, diem_tong_ket, dat_mon "
            "FROM diem_hoc_phan WHERE diem_tong_ket IS NOT NULL "
            "ORDER BY ma_diem"
        )).fetchall()

    total = len(rows)
    print(f"  Tổng điểm (có giá trị): {total:,}")

    stats = {}
    updates = []
    ma_diems = [r.ma_diem for r in rows]

    # ── Lỗi 1: diem_tong_ket = NULL → EXP-GR-03 ──────────────────────────
    null_idxs = pick(ma_diems, ERROR_CONFIG["PG_DIEM_tong_ket_null"])
    for i in null_idxs:
        updates.append((
            "UPDATE diem_hoc_phan SET diem_tong_ket=NULL, diem_chu=NULL, "
            "diem_he_4=NULL, dat_mon=NULL WHERE ma_diem = :md",
            {"md": rows[i].ma_diem}
        ))
    stats["tong_ket_null"] = len(null_idxs)
    print(f"  ✏️  diem_tong_ket = NULL      : {len(null_idxs):>4} bản ghi")

    valid_idxs = [i for i in range(len(rows)) if i not in null_idxs]

    # ── Lỗi 2: diem_chu không khớp → EXP-GR-05 ───────────────────────────
    sample = sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_chu_khong_khop"]))
    for i in [valid_idxs[j] for j in sample]:
        r = rows[i]
        bad_chu = _diem_to_chu_sai(float(r.diem_tong_ket))
        updates.append((
            "UPDATE diem_hoc_phan SET diem_chu = :dc WHERE ma_diem = :md",
            {"dc": bad_chu, "md": r.ma_diem}
        ))
    stats["chu_khong_khop"] = len(sample)
    print(f"  ✏️  diem_chu không khớp      : {len(sample):>4} bản ghi")

    # ── Lỗi 3: dat_mon sai (trigger bị disable) ──────────────────────────
    sample2 = sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_dat_mon_sai"]))
    for i in [valid_idxs[j] for j in sample2]:
        r = rows[i]
        updates.append((
            "UPDATE diem_hoc_phan SET dat_mon = :dm WHERE ma_diem = :md",
            {"dm": not bool(r.dat_mon), "md": r.ma_diem}
        ))
    stats["dat_mon_sai"] = len(sample2)
    print(f"  ✏️  dat_mon không nhất quán  : {len(sample2):>4} bản ghi")

    # ── Lỗi 4: diem_he_4 = NULL ──────────────────────────────────────────
    sample3 = sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_he4_null"]))
    for i in [valid_idxs[j] for j in sample3]:
        updates.append((
            "UPDATE diem_hoc_phan SET diem_he_4 = NULL WHERE ma_diem = :md",
            {"md": rows[i].ma_diem}
        ))
    stats["he4_null"] = len(sample3)
    print(f"  ✏️  diem_he_4 = NULL          : {len(sample3):>4} bản ghi")

    # ── Lỗi 5: diem_cuoi_ky out of [0, 10] → EXP-GR-04d ──────────────────
    sample4 = sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_cuoi_ky_out_range"]))
    for i in [valid_idxs[j] for j in sample4]:
        # 2 variants: số quá cao hoặc số âm
        bad_score = random.choice([
            round(random.uniform(10.1, 15.0), 2),
            round(random.uniform(-2.0, -0.1), 2),
        ])
        updates.append((
            "UPDATE diem_hoc_phan SET diem_cuoi_ky = :ck WHERE ma_diem = :md",
            {"ck": bad_score, "md": rows[i].ma_diem}
        ))
    stats["cuoi_ky_out_range"] = len(sample4)
    print(f"  ✏️  diem_cuoi_ky ngoài [0,10]: {len(sample4):>4} bản ghi")

    # ── Thực thi ─────────────────────────────────────────────────────────
    if not dry_run and updates:
        with engine.begin() as conn:
            for sql, params in updates:
                conn.execute(text(sql), params)
        print(f"  ✅ Đã UPDATE {len(updates)} bản ghi vào diem_hoc_phan")
    elif dry_run:
        print(f"  🔍 [DRY RUN] Sẽ UPDATE {len(updates)} bản ghi")

    return stats


# ════════════════════════════════════════════════════════════════════════════
# INJECT VÀO CSV
# ════════════════════════════════════════════════════════════════════════════

def inject_csv_files(dry_run: bool) -> dict:
    """Inject lỗi vào các file ctsv_HK*.csv."""
    print("\n" + "─" * 60)
    print("📋 NGUỒN 2 — CSV: thư mục data/csv/")
    print("─" * 60)

    csv_files = sorted(glob.glob(str(CSV_DIR / "ctsv_HK*.csv")))
    if not csv_files:
        csv_files = [f for f in sorted(glob.glob(str(CSV_DIR / "ctsv_*.csv")))
                     if "all" not in Path(f).name]

    if not csv_files:
        print(f"  ⚠️  Không tìm thấy file CSV trong {CSV_DIR}")
        return {}

    print(f"  Tìm thấy {len(csv_files)} file CSV")

    total_stats = {"drl_trong": 0, "xeploai_sai": 0, "masv_thua_khoang": 0,
                   "dong_trung_lap": 0, "tien_am": 0, "hocky_sai": 0,
                   "drl_chu": 0, "tong_records": 0}

    for fpath in csv_files:
        fname = Path(fpath).name
        df = pd.read_csv(fpath, dtype=str, encoding="utf-8")
        n = len(df)
        total_stats["tong_records"] += n

        # ── Lỗi 1: diem_ren_luyen trống → EXP-CTSV-06 ────────────────────
        idxs = list(pick(range(n), ERROR_CONFIG["CSV_drl_trong"]))
        for i in idxs:
            df.at[i, "diem_ren_luyen"] = random.choice(["", "N/A"])
            df.at[i, "xep_loai_rl"] = ""
        total_stats["drl_trong"] += len(idxs)

        # ── Lỗi 2: diem_ren_luyen là chữ → EXP-CTSV-06 ───────────────────
        idxs2 = [i for i in pick(range(n), ERROR_CONFIG["CSV_drl_chu_thay_so"])
                 if i not in idxs]
        for i in idxs2:
            df.at[i, "diem_ren_luyen"] = random.choice(["Chưa nhập", "NULL"])
        total_stats["drl_chu"] += len(idxs2)

        # ── Lỗi 3: xep_loai không khớp ĐRL → EXP-CTSV-07 ─────────────────
        valid = [i for i in range(n) if i not in idxs and i not in idxs2
                 and str(df.at[i, "diem_ren_luyen"]).replace(".", "").isdigit()]
        idxs3 = list(pick(valid, ERROR_CONFIG["CSV_xeploai_sai"]))
        for i in idxs3:
            try:
                df.at[i, "xep_loai_rl"] = _xep_loai_rl_sai(float(df.at[i, "diem_ren_luyen"]))
            except (ValueError, TypeError):
                pass
        total_stats["xeploai_sai"] += len(idxs3)

        # ── Lỗi 4: mã SV thừa khoảng trắng / lowercase → EXP-CTSV-03 ────
        idxs4 = list(pick(range(n), ERROR_CONFIG["CSV_masv_thua_khoang"]))
        for i in idxs4:
            ma = str(df.at[i, "ma_sinh_vien"])
            df.at[i, "ma_sinh_vien"] = random.choice([ma + " ", ma.lower()])
        total_stats["masv_thua_khoang"] += len(idxs4)

        # ── Lỗi 5: dòng trùng lặp → EXP-CTSV-05 (compound unique) ───────
        n_dup = max(1, int(n * ERROR_CONFIG["CSV_dong_trung_lap"]))
        if n > 0:
            dup_rows = df.sample(n=min(n_dup, n), random_state=2024)
            df = pd.concat([df, dup_rows], ignore_index=True)
            df = df.sample(frac=1, random_state=2024).reset_index(drop=True)
        total_stats["dong_trung_lap"] += n_dup

        # ── Lỗi 6: muc_tien_hb âm → EXP-CTSV-08 ──────────────────────────
        hb_idxs = df[df["muc_tien_hb"].notna() & (df["muc_tien_hb"] != "")
                     & (df["muc_tien_hb"] != "0")].index.tolist()
        if hb_idxs:
            n_am = max(1, int(len(hb_idxs) * ERROR_CONFIG["CSV_tien_hb_am"]))
            for i in random.sample(hb_idxs, min(n_am, len(hb_idxs))):
                try:
                    val = float(df.at[i, "muc_tien_hb"])
                    df.at[i, "muc_tien_hb"] = str(-abs(val))
                except (ValueError, TypeError):
                    df.at[i, "muc_tien_hb"] = "-1200000"
                total_stats["tien_am"] += 1

        # ── Lỗi 7: hoc_ky format sai → EXP-CTSV-04 ───────────────────────
        idxs7 = list(pick(range(len(df)), ERROR_CONFIG["CSV_hocky_sai_format"]))
        for i in idxs7:
            hk = str(df.at[i, "hoc_ky"])
            df.at[i, "hoc_ky"] = random.choice([hk.replace("-", "/"), hk.lower()])
        total_stats["hocky_sai"] += len(idxs7)

        # ── Ghi lại file ──────────────────────────────────────────────────
        if not dry_run:
            df.to_csv(fpath, index=False, encoding="utf-8")
            print(f"  ✅ {fname}: {n} → {len(df)} records")
        else:
            print(f"  🔍 [DRY RUN] {fname}: {n} → {len(df)} records")

    # Cập nhật ctsv_all.csv
    if not dry_run:
        all_dfs = [pd.read_csv(f, dtype=str, encoding="utf-8") for f in csv_files]
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            combined.to_csv(str(CSV_DIR / "ctsv_all.csv"), index=False, encoding="utf-8")
            print(f"  ✅ ctsv_all.csv cập nhật: {len(combined)} records")

    return total_stats


# ════════════════════════════════════════════════════════════════════════════
# INJECT VÀO JSON
# ════════════════════════════════════════════════════════════════════════════

def inject_json_files(dry_run: bool) -> dict:
    """Inject lỗi vào các file taichinh_HK*.json."""
    print("\n" + "─" * 60)
    print("📋 NGUỒN 3 — JSON API: thư mục data/api_json/")
    print("─" * 60)

    json_files = sorted(glob.glob(str(JSON_DIR / "taichinh_HK*.json")))
    if not json_files:
        json_files = [f for f in sorted(glob.glob(str(JSON_DIR / "taichinh_*.json")))
                      if "all" not in Path(f).name]

    if not json_files:
        print(f"  ⚠️  Không tìm thấy file JSON trong {JSON_DIR}")
        return {}

    print(f"  Tìm thấy {len(json_files)} file JSON")

    total_stats = {"cono_sai": 0, "ngay_format_sai": 0, "dadong_vuot": 0,
                   "hocky_sai": 0, "tien_mien_am": 0, "thieu_truong": 0,
                   "tong_records": 0}

    for fpath in json_files:
        fname = Path(fpath).name
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)

        # ★ FIX: Hỗ trợ cả 2 format JSON
        #   (a) wrapped: {"metadata": {...}, "data": [...]}  — format v3.0
        #   (b) list trực tiếp: [{...}, {...}]               — format cũ
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            records = payload["data"]
            wrapped = True
        elif isinstance(payload, list):
            records = payload
            wrapped = False
        else:
            print(f"  ⚠️  {fname}: format JSON không hỗ trợ — bỏ qua")
            continue

        n = len(records)
        total_stats["tong_records"] += n
        if n == 0:
            continue

        all_idxs = list(range(n))

        # ── Lỗi 1: con_no tính sai → EXP-TC-09 ───────────────────────────
        idxs = sorted(pick(all_idxs, ERROR_CONFIG["JSON_cono_tinh_sai"]))
        for i in idxs:
            phi = records[i].get("hoc_phi_phai_dong", 0)
            da_dong = records[i].get("da_dong", 0)
            sai_so = random.choice([random.randint(1000, 10000),
                                    -random.randint(1000, 10000)])
            records[i]["con_no"] = max(0, (phi - da_dong) + sai_so)
        total_stats["cono_sai"] += len(idxs)

        # ── Lỗi 2: ngay_dong format sai (transform sẽ check) ─────────────
        idxs2 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_ngay_dong_sai_format"]))
        for i in idxs2:
            nd = records[i].get("ngay_dong_cuoi", "2024-09-01")
            if nd and nd != "None":
                try:
                    dt = datetime.strptime(str(nd)[:10], "%Y-%m-%d")
                    # 2 variants: dd/mm/yyyy hoặc thay - bằng .
                    records[i]["ngay_dong_cuoi"] = random.choice([
                        dt.strftime("%d/%m/%Y"),
                        str(nd).replace("-", "."),
                    ])
                except (ValueError, TypeError):
                    records[i]["ngay_dong_cuoi"] = "01/01/2024"
        total_stats["ngay_format_sai"] += len(idxs2)

        # ── Lỗi 3: da_dong > hoc_phi (đóng thừa) ─────────────────────────
        idxs3 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_dadong_vuot_phi"]))
        for i in idxs3:
            phi = records[i].get("hoc_phi_phai_dong", 0)
            if phi > 0:
                records[i]["da_dong"] = int(phi * 1.5)
                records[i]["con_no"] = 0
        total_stats["dadong_vuot"] += len(idxs3)

        # ── Lỗi 4: hoc_ky format sai → EXP-TC-05 ─────────────────────────
        idxs4 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_hocky_sai_format"]))
        for i in idxs4:
            hk = records[i].get("hoc_ky", "HK1-2024-25")
            records[i]["hoc_ky"] = random.choice([
                hk.replace("-", "_"),
                hk.replace("HK", "Ky"),
            ])
        total_stats["hocky_sai"] += len(idxs4)

        # ── Lỗi 5: so_tien_mien_giam âm → EXP-TC-10 ──────────────────────
        mg_idxs = [i for i in all_idxs
                   if records[i].get("duoc_mien_giam") and records[i].get("so_tien_mien_giam", 0) > 0]
        if mg_idxs:
            n_am = max(1, int(len(mg_idxs) * ERROR_CONFIG["JSON_tien_mien_giam_am"]))
            for i in random.sample(mg_idxs, min(n_am, len(mg_idxs))):
                records[i]["so_tien_mien_giam"] = -abs(records[i]["so_tien_mien_giam"])
                total_stats["tien_mien_am"] += 1

        # ── Lỗi 6: thiếu trường ngay_dong_cuoi → EXP-TC-11 ───────────────
        idxs6 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_thieu_truong"]))
        for i in idxs6:
            records[i].pop("ngay_dong_cuoi", None)
        total_stats["thieu_truong"] += len(idxs6)

        # ── Ghi lại file ──────────────────────────────────────────────────
        if not dry_run:
            with open(fpath, "w", encoding="utf-8") as f:
                if wrapped:
                    payload["data"] = records   # cập nhật records, giữ metadata
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                else:
                    json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  ✅ {fname}: {n} records")
        else:
            print(f"  🔍 [DRY RUN] {fname}: {n} records")

    # Cập nhật taichinh_all.json — hỗ trợ cả 2 format
    if not dry_run:
        all_records = []
        for fpath in json_files:
            with open(fpath, "r", encoding="utf-8") as f:
                p = json.load(f)
            if isinstance(p, dict) and isinstance(p.get("data"), list):
                all_records.extend(p["data"])
            elif isinstance(p, list):
                all_records.extend(p)

        all_fpath = JSON_DIR / "taichinh_all.json"
        # Phát hiện format gốc của taichinh_all.json để giữ nhất quán
        if all_fpath.exists():
            with open(all_fpath, "r", encoding="utf-8") as f:
                orig = json.load(f)
            if isinstance(orig, dict) and "data" in orig:
                orig["data"] = all_records
                with open(all_fpath, "w", encoding="utf-8") as f:
                    json.dump(orig, f, ensure_ascii=False, indent=2)
            else:
                with open(all_fpath, "w", encoding="utf-8") as f:
                    json.dump(all_records, f, ensure_ascii=False, indent=2)
        else:
            with open(all_fpath, "w", encoding="utf-8") as f:
                json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"  ✅ taichinh_all.json cập nhật: {len(all_records)} records")

    return total_stats


# ════════════════════════════════════════════════════════════════════════════
# BACKUP & RESTORE
# ════════════════════════════════════════════════════════════════════════════

def backup_files():
    """Sao lưu CSV/JSON trước khi inject."""
    BCK_DIR.mkdir(parents=True, exist_ok=True)
    bck_csv  = BCK_DIR / "csv";      bck_csv.mkdir(exist_ok=True)
    bck_json = BCK_DIR / "api_json"; bck_json.mkdir(exist_ok=True)

    count = 0
    for f in glob.glob(str(CSV_DIR / "*.csv")):
        shutil.copyfile(f, bck_csv / Path(f).name); count += 1
    for f in glob.glob(str(JSON_DIR / "*.json")):
        shutil.copyfile(f, bck_json / Path(f).name); count += 1
    print(f"  💾 Đã backup {count} files → {BCK_DIR}")


def restore_files():
    """Khôi phục CSV/JSON từ backup. (PG phải chạy generate lại.)"""
    if not BCK_DIR.exists():
        print(f"  ❌ Không tìm thấy backup tại {BCK_DIR}")
        return False
    count = 0
    for f in glob.glob(str(BCK_DIR / "csv" / "*.csv")):
        shutil.copy2(f, CSV_DIR / Path(f).name); count += 1
    for f in glob.glob(str(BCK_DIR / "api_json" / "*.json")):
        shutil.copy2(f, JSON_DIR / Path(f).name); count += 1
    print(f"  ✅ Đã restore {count} files từ backup")
    print(f"  ⚠️  Lưu ý: PostgreSQL không restore tự động — chạy generate_sample_data.py")
    return True


# ════════════════════════════════════════════════════════════════════════════
# BÁO CÁO
# ════════════════════════════════════════════════════════════════════════════

def print_summary(pg_sv, pg_diem, csv_s, json_s, dry_run):
    mode = "DRY RUN" if dry_run else "ĐÃ THỰC THI"
    print("\n" + "=" * 65)
    print(f"  📊 BÁO CÁO INJECT LỖI — [{mode}]")
    print("=" * 65)

    print("\n  NGUỒN 1 — PostgreSQL sinh_vien:")
    print(f"    email sai format         : {pg_sv.get('email_sai_format', 0):>5}")
    print(f"    trang_thai format cũ     : {pg_sv.get('trang_thai_cu', 0):>5}")

    print("\n  NGUỒN 1 — PostgreSQL diem_hoc_phan:")
    print(f"    diem_tong_ket = NULL     : {pg_diem.get('tong_ket_null', 0):>5}")
    print(f"    diem_chu không khớp      : {pg_diem.get('chu_khong_khop', 0):>5}")
    print(f"    dat_mon không nhất quán  : {pg_diem.get('dat_mon_sai', 0):>5}")
    print(f"    diem_he_4 = NULL         : {pg_diem.get('he4_null', 0):>5}")
    print(f"    diem_cuoi_ky ngoài range : {pg_diem.get('cuoi_ky_out_range', 0):>5}")

    print(f"\n  NGUỒN 2 — CSV ({csv_s.get('tong_records', 0):,} records):")
    print(f"    diem_rl trống            : {csv_s.get('drl_trong', 0):>5}")
    print(f"    diem_rl là chữ           : {csv_s.get('drl_chu', 0):>5}")
    print(f"    xep_loai không khớp      : {csv_s.get('xeploai_sai', 0):>5}")
    print(f"    mã SV thừa khoảng trắng  : {csv_s.get('masv_thua_khoang', 0):>5}")
    print(f"    dòng trùng lặp           : {csv_s.get('dong_trung_lap', 0):>5}")
    print(f"    muc_tien_hb âm           : {csv_s.get('tien_am', 0):>5}")
    print(f"    hoc_ky format sai        : {csv_s.get('hocky_sai', 0):>5}")

    print(f"\n  NGUỒN 3 — JSON ({json_s.get('tong_records', 0):,} records):")
    print(f"    con_no tính sai          : {json_s.get('cono_sai', 0):>5}")
    print(f"    ngay_dong format sai     : {json_s.get('ngay_format_sai', 0):>5}")
    print(f"    da_dong vượt hoc_phi     : {json_s.get('dadong_vuot', 0):>5}")
    print(f"    hoc_ky format sai        : {json_s.get('hocky_sai', 0):>5}")
    print(f"    so_tien_mien_giam âm     : {json_s.get('tien_mien_am', 0):>5}")
    print(f"    thiếu field ngay_dong    : {json_s.get('thieu_truong', 0):>5}")

    total = (sum(pg_sv.values()) + sum(pg_diem.values()) +
             sum(v for k, v in csv_s.items() if k != "tong_records") +
             sum(v for k, v in json_s.items() if k != "tong_records"))

    print("\n" + "─" * 65)
    print(f"  TỔNG LỖI ĐÃ INJECT: {total:,} bản ghi")
    print("─" * 65)

    if not dry_run:
        print("""
  Bước tiếp theo:
    1. Chạy ETL:    python scripts/run_etl.py
    2. GE sẽ phát hiện lỗi → Airflow DAG dừng tại task validate_data
    3. Xem kết quả: http://localhost:3000 (Grafana)

  Khôi phục:
    python scripts/inject_errors.py --restore
    python scripts/generate_sample_data.py   (để restore PG)""")
    print("=" * 65)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Inject lỗi thực tế vào dữ liệu để demo Great Expectations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
  python scripts/inject_errors.py            # inject lỗi (có backup)
  python scripts/inject_errors.py --dry-run  # xem sẽ inject gì, không sửa
  python scripts/inject_errors.py --restore  # khôi phục CSV/JSON từ backup
        """
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Chỉ hiển thị sẽ inject gì, không thực sự sửa dữ liệu")
    p.add_argument("--restore", action="store_true",
                   help="Khôi phục file CSV/JSON từ backup")
    return p.parse_args()


def main():
    args = parse_args()

    print("=" * 65)
    print("  🔧 INJECT LỖI THỰC TẾ — ETL Platform PTIT Nhóm 8")
    print("=" * 65)
    print(f"  Data dir : {DATA_DIR}")
    print(f"  Mode     : {'DRY RUN' if args.dry_run else 'THỰC THI'}")

    if args.restore:
        print("\n🔄 CHẾ ĐỘ RESTORE")
        restore_files()
        return

    if not CSV_DIR.exists():
        print(f"\n  ❌ Không tìm thấy thư mục CSV: {CSV_DIR}")
        print(f"     Hãy chạy generate_sample_data.py trước!")
        sys.exit(1)

    if not args.dry_run:
        print(f"\n💾 Đang backup dữ liệu gốc...")
        backup_files()

    pg_sv = pg_diem = {}
    try:
        pg_sv   = inject_postgres_sinh_vien(args.dry_run)
        pg_diem = inject_postgres_diem(args.dry_run)
    except Exception as e:
        print(f"\n  ⚠️  Lỗi kết nối PostgreSQL: {e}")
        print(f"      Đảm bảo docker-compose đang chạy. Tiếp tục với CSV/JSON...")

    csv_s  = inject_csv_files(args.dry_run)
    json_s = inject_json_files(args.dry_run)

    print_summary(pg_sv, pg_diem, csv_s, json_s, args.dry_run)


if __name__ == "__main__":
    main()