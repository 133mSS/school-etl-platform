"""
scripts/inject_errors.py
  PostgreSQL sinh_vien  : email sai, trang_thai cũ, gioi_tinh lạ, ngay_sinh sai
  PostgreSQL diem_hphan : điểm NULL, diem_chu không khớp, dat_mon sai, diem_he4 NULL
  CSV ctsv              : diem_rl trống, xep_loai sai, mã SV thừa dấu cách, trùng dòng
  JSON taichinh         : con_no tính sai, ngay_dong format lạ, da_dong vượt phi, hoc_ky sai
"""


import os
import sys
import json
import glob
import shutil
import random
import argparse
import csv as csv_module
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ── Path setup ─────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# ── Kết nối database ────────────────────────────────────────────────────────
DB_URL = "postgresql+psycopg2://school_user:school_pass@localhost:5434/school_source"
engine = create_engine(DB_URL, echo=False)

# ── Thư mục dữ liệu ─────────────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / "data"
CSV_DIR  = DATA_DIR / "csv"
JSON_DIR = DATA_DIR / "api_json"
BCK_DIR  = DATA_DIR / "_backup_before_inject"   # thư mục backup

random.seed(2024)   # seed cố định → kết quả lặp lại được mỗi lần chạy

# ════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH TỶ LỆ LỖI
# Thay đổi các con số này nếu muốn inject nhiều/ít lỗi hơn
# ════════════════════════════════════════════════════════════════════════════
ERROR_CONFIG = {

    # ── Nguồn 1: PostgreSQL - bảng sinh_vien ─────────────────────────────
    # Lý do thực tế: dữ liệu migrate từ hệ thống quản lý cũ (Access/Excel)
    # sang PostgreSQL, nhiều trường bị sai encoding hoặc format
    "PG_SV_email_sai_format":       0.020,  # 2.0% email bị mất @ hoặc domain sai
    "PG_SV_trang_thai_cu":          0.015,  # 1.5% trạng thái dùng format cũ không dấu
    "PG_SV_gioi_tinh_la":           0.010,  # 1.0% giới tính ghi "M"/"F" thay vì "Nam"/"Nữ"
    "PG_SV_ngay_sinh_tuong_lai":    0.005,  # 0.5% ngày sinh nhập sai (năm trong tương lai)
    "PG_SV_ma_sv_sai_case":         0.008,  # 0.8% mã SV bị lowercase: "b21dckd001"

    # ── Nguồn 1: PostgreSQL - bảng diem_hoc_phan ─────────────────────────
    # Lý do thực tế: GV nhập điểm muộn, hoặc nhập trực tiếp vào DB bỏ qua validation
    "PG_DIEM_tong_ket_null":        0.020,  # 2.0% diem_tong_ket NULL (GV chưa nhập)
    "PG_DIEM_chu_khong_khop":       0.012,  # 1.2% diem_chu không khớp diem_tong_ket
    "PG_DIEM_dat_mon_sai":          0.015,  # 1.5% dat_mon ngược với diem_tong_ket
    "PG_DIEM_he4_null":             0.008,  # 0.8% diem_he_4 NULL dù có diem_tong_ket
    "PG_DIEM_cuoi_ky_out_range":    0.005,  # 0.5% diem_cuoi_ky > 10 (lỗi nhập 11.5 thay vì 1.15)

    # ── Nguồn 2: CSV - file ctsv_*.csv ────────────────────────────────────
    # Lý do thực tế: nhân viên Phòng CTSV xuất từ Excel, nhập tay một phần
    "CSV_drl_trong":                0.050,  # 5.0% diem_ren_luyen bị trống (chưa chấm)
    "CSV_xeploai_sai":              0.030,  # 3.0% xep_loai_rl không khớp diem_ren_luyen
    "CSV_masv_thua_khoang":         0.020,  # 2.0% mã SV bị thừa dấu cách đầu/cuối
    "CSV_dong_trung_lap":           0.015,  # 1.5% dòng bị copy-paste trùng lặp
    "CSV_tien_hb_am":               0.008,  # 0.8% muc_tien_hb âm (lỗi nhập âm nhầm)
    "CSV_hocky_sai_format":         0.005,  # 0.5% hoc_ky format sai "HK1/2024-25"
    "CSV_drl_chu_thay_so":          0.010,  # 1.0% diem_ren_luyen = "N/A" hoặc "chưa có"

    # ── Nguồn 3: JSON - file taichinh_*.json ──────────────────────────────
    # Lý do thực tế: vendor API làm tròn số, thiếu trường, format ngày không nhất quán
    "JSON_cono_tinh_sai":           0.030,  # 3.0% con_no != hoc_phi - da_dong (±sai số)
    "JSON_ngay_dong_sai_format":    0.025,  # 2.5% ngay_dong_cuoi format "dd/mm/yyyy"
    "JSON_dadong_vuot_phi":         0.010,  # 1.0% da_dong > hoc_phi_phai_dong (đóng thừa)
    "JSON_hocky_sai_format":        0.015,  # 1.5% hoc_ky dùng "_" thay "-": "HK1_2024_25"
    "JSON_tien_mien_giam_am":       0.005,  # 0.5% so_tien_mien_giam âm (vendor bug)
    "JSON_thieu_truong":            0.008,  # 0.8% thiếu field "ngay_dong_cuoi" hoàn toàn
}

# ════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════

def pick(lst, rate):
    """Chọn ngẫu nhiên ~rate*100% phần tử từ list, trả về set các index."""
    n = max(1, int(len(lst) * rate))
    return set(random.sample(range(len(lst)), min(n, len(lst))))


def _diem_to_chu_sai(diem_tong_ket):
    """
    Trả về điểm chữ SAI so với điểm số — để inject lỗi diem_chu không khớp.
    Cụ thể: dịch lên/xuống 1 bậc so với đúng.
    """
    thang = [
        (9.5, "A+"), (8.5, "A"), (8.0, "B+"), (7.0, "B"),
        (6.5, "C+"), (5.5, "C"), (5.0, "D+"), (4.0, "D"), (0.0, "F"),
    ]
    dung = "F"
    idx_dung = len(thang) - 1
    for i, (nguong, chu) in enumerate(thang):
        if diem_tong_ket >= nguong:
            dung = chu
            idx_dung = i
            break

    # Dịch chuyển 1-2 bậc ngẫu nhiên
    delta = random.choice([-2, -1, 1, 2])
    idx_sai = max(0, min(len(thang) - 1, idx_dung + delta))
    return thang[idx_sai][1]


def _xep_loai_rl_sai(drl):
    """Trả về xep_loai_rl SAI so với điểm rèn luyện."""
    dung_map = [
        (90, "Xuất sắc"), (80, "Tốt"), (65, "Khá"),
        (50, "Trung bình"), (35, "Yếu"), (0, "Kém"),
    ]
    tat_ca = ["Xuất sắc", "Tốt", "Khá", "Trung bình", "Yếu", "Kém"]
    dung = "Kém"
    for nguong, xeploai in dung_map:
        if drl >= nguong:
            dung = xeploai
            break
    # Chọn 1 xếp loại khác với đúng
    sai_options = [x for x in tat_ca if x != dung]
    return random.choice(sai_options)


# ════════════════════════════════════════════════════════════════════════════
# INJECT LỖI VÀO POSTGRESQL
# ════════════════════════════════════════════════════════════════════════════

def inject_postgres_sinh_vien(dry_run: bool) -> dict:
    """
    Inject lỗi vào bảng sinh_vien trong PostgreSQL.
    Chỉ UPDATE — không INSERT hay DELETE.
    """
    print("\n" + "─" * 60)
    print("📋 NGUỒN 1 — PostgreSQL: bảng sinh_vien")
    print("─" * 60)

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ma_sinh_vien, ho, ten, email, trang_thai_hoc_tap, "
            "gioi_tinh, ngay_sinh FROM sinh_vien ORDER BY ma_sinh_vien"
        )).fetchall()

    total = len(rows)
    print(f"  Tổng SV: {total:,}")

    stats = {}
    updates = []   # list of (sql, params)

    ma_svs = [r.ma_sinh_vien for r in rows]

    # ── Lỗi 1: Email sai format ───────────────────────────────────────────
    # Thực tế: hệ thống cũ lưu email không có @, hoặc domain bị cắt
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_email_sai_format"])
    loi_email = [
        lambda e: e.replace("@", ""),                        # bỏ @ hoàn toàn
        lambda e: e.replace("@student.ptit.edu.vn", ""),    # chỉ còn username
        lambda e: e.replace("student.ptit.edu.vn", "ptit"), # domain rút gọn sai
        lambda e: e + ".vn",                                  # thêm đuôi thừa
        lambda e: e.replace("@", " @ "),                     # khoảng trắng quanh @
    ]
    for i in idxs:
        r = rows[i]
        bad_email = random.choice(loi_email)(r.email)
        updates.append((
            "UPDATE sinh_vien SET email = :email WHERE ma_sinh_vien = :ma",
            {"email": bad_email, "ma": r.ma_sinh_vien}
        ))
    stats["email_sai_format"] = len(idxs)
    print(f"  ✏️  email sai format         : {len(idxs):>4} bản ghi")

    # ── Lỗi 2: Trạng thái dùng format hệ thống cũ ────────────────────────
    # Thực tế: hệ thống cũ lưu không dấu, khi migrate không convert
    tt_cu_map = {
        "Đang học":   ["dang hoc", "DANG_HOC",  "DangHoc",   "đang học "],
        "Bảo lưu":    ["bao luu",  "BAO_LUU",   "Bao luu",   "bảo lưu "],
        "Thôi học":   ["thoi hoc", "THOI_HOC",  "Thoi hoc",  "thôi học "],
        "Tốt nghiệp": ["tot nghiep","TOT_NGHIEP","Tot Nghiep","tốt nghiệp"],
    }
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_trang_thai_cu"])
    for i in idxs:
        r = rows[i]
        tt_options = tt_cu_map.get(r.trang_thai_hoc_tap, ["dang hoc"])
        bad_tt = random.choice(tt_options)
        updates.append((
            "UPDATE sinh_vien SET trang_thai_hoc_tap = :tt WHERE ma_sinh_vien = :ma",
            {"tt": bad_tt, "ma": r.ma_sinh_vien}
        ))
    stats["trang_thai_cu"] = len(idxs)
    print(f"  ✏️  trang_thai format cũ     : {len(idxs):>4} bản ghi")

    # ── Lỗi 3: Giới tính format lạ ───────────────────────────────────────
    # Thực tế: form nhập liệu từ hệ thống nước ngoài dùng M/F
    gt_la = {"Nam": ["M", "male", "MALE", "Nam "], "Nữ": ["F", "female", "Nu", "Nữ "]}
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_gioi_tinh_la"])
    for i in idxs:
        r = rows[i]
        options = gt_la.get(r.gioi_tinh or "Nam", ["M"])
        updates.append((
            "UPDATE sinh_vien SET gioi_tinh = :gt WHERE ma_sinh_vien = :ma",
            {"gt": random.choice(options), "ma": r.ma_sinh_vien}
        ))
    stats["gioi_tinh_la"] = len(idxs)
    print(f"  ✏️  gioi_tinh format lạ      : {len(idxs):>4} bản ghi")

    # ── Lỗi 4: Ngày sinh trong tương lai ─────────────────────────────────
    # Thực tế: nhập nhầm năm 2006 → 2060 hoặc 2026 → 2206
    idxs = pick(ma_svs, ERROR_CONFIG["PG_SV_ngay_sinh_tuong_lai"])
    for i in idxs:
        r = rows[i]
        # Ngày sinh đúng khoảng 2000-2007, inject thành 2030-2040
        ngay_sai = date(
            random.randint(2030, 2040),
            random.randint(1, 12),
            random.randint(1, 28)
        )
        updates.append((
            "UPDATE sinh_vien SET ngay_sinh = :ns WHERE ma_sinh_vien = :ma",
            {"ns": ngay_sai, "ma": r.ma_sinh_vien}
        ))
    stats["ngay_sinh_tuong_lai"] = len(idxs)
    print(f"  ✏️  ngay_sinh tương lai      : {len(idxs):>4} bản ghi")

    # ── Lỗi 5: Mã SV lowercase ───────────────────────────────────────────
    # Thực tế: một số hệ thống con ghi mã SV bằng chữ thường
    # KHÔNG inject vào primary key (sẽ vi phạm constraint)
    # Thay vào đó inject vào một bảng khác có dùng ma_sinh_vien làm FK text
    # → ở đây inject vào trường comment/note nếu có, hoặc bỏ qua
    # Thực ra đây là lỗi xảy ra ở nguồn CSV và JSON, không phải PG (PK không thể sửa)
    stats["ma_sv_sai_case"] = 0   # skip — xử lý ở tầng CSV/JSON

    # ── Thực thi ─────────────────────────────────────────────────────────
    if not dry_run and updates:
        with engine.begin() as conn:
            for sql, params in updates:
                conn.execute(text(sql), params)
        print(f"  ✅ Đã UPDATE {len(updates)} bản ghi vào PostgreSQL sinh_vien")
    elif dry_run:
        print(f"  🔍 [DRY RUN] Sẽ UPDATE {len(updates)} bản ghi (không thực thi)")

    return stats


def inject_postgres_diem(dry_run: bool) -> dict:
    """
    Inject lỗi vào bảng diem_hoc_phan trong PostgreSQL.
    """
    print("\n" + "─" * 60)
    print("📋 NGUỒN 1 — PostgreSQL: bảng diem_hoc_phan")
    print("─" * 60)

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ma_diem, ma_dang_ky, diem_tong_ket, diem_chu, "
            "diem_he_4, dat_mon, diem_cuoi_ky "
            "FROM diem_hoc_phan "
            "WHERE diem_tong_ket IS NOT NULL "   # chỉ chọn bản ghi đã có điểm
            "ORDER BY ma_diem"
        )).fetchall()

    total = len(rows)
    print(f"  Tổng điểm (có giá trị): {total:,}")

    stats = {}
    updates = []

    ma_diems = [r.ma_diem for r in rows]

    # ── Lỗi 1: diem_tong_ket = NULL ──────────────────────────────────────
    # Thực tế: GV nhập điểm muộn, hệ thống lock bảng trước khi GV nhập xong
    idxs = pick(ma_diems, ERROR_CONFIG["PG_DIEM_tong_ket_null"])
    for i in idxs:
        r = rows[i]
        updates.append((
            "UPDATE diem_hoc_phan SET diem_tong_ket = NULL, diem_chu = NULL, "
            "diem_he_4 = NULL, dat_mon = NULL WHERE ma_diem = :md",
            {"md": r.ma_diem}
        ))
    stats["tong_ket_null"] = len(idxs)
    print(f"  ✏️  diem_tong_ket = NULL      : {len(idxs):>4} bản ghi")

    # Lấy các index còn lại (không phải null) để inject lỗi tiếp
    null_idxs = idxs
    valid_idxs = [i for i in range(len(rows)) if i not in null_idxs]

    # ── Lỗi 2: diem_chu không khớp diem_tong_ket ──────────────────────────
    # Thực tế: GV nhập điểm số đúng nhưng điểm chữ sửa tay → lệch nhau
    sample_valid = [valid_idxs[i] for i in
                    sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_chu_khong_khop"]))]
    for i in sample_valid:
        r = rows[i]
        bad_chu = _diem_to_chu_sai(float(r.diem_tong_ket))
        updates.append((
            "UPDATE diem_hoc_phan SET diem_chu = :dc WHERE ma_diem = :md",
            {"dc": bad_chu, "md": r.ma_diem}
        ))
    stats["chu_khong_khop"] = len(sample_valid)
    print(f"  ✏️  diem_chu không khớp      : {len(sample_valid):>4} bản ghi")

    # ── Lỗi 3: dat_mon sai với diem_tong_ket ──────────────────────────────
    # Thực tế: trường dat_mon được tính toán bởi trigger, nhưng trigger bị
    # disable trong lúc nhập batch → dat_mon không được cập nhật
    sample_valid2 = [valid_idxs[i] for i in
                     sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_dat_mon_sai"]))]
    for i in sample_valid2:
        r = rows[i]
        # Đảo ngược dat_mon
        bad_dat = not bool(r.dat_mon)
        updates.append((
            "UPDATE diem_hoc_phan SET dat_mon = :dm WHERE ma_diem = :md",
            {"dm": bad_dat, "md": r.ma_diem}
        ))
    stats["dat_mon_sai"] = len(sample_valid2)
    print(f"  ✏️  dat_mon không nhất quán  : {len(sample_valid2):>4} bản ghi")

    # ── Lỗi 4: diem_he_4 = NULL dù có diem_tong_ket ──────────────────────
    # Thực tế: script tính điểm hệ 4 bị lỗi ở 1 batch nhỏ, chạy thiếu
    sample_valid3 = [valid_idxs[i] for i in
                     sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_he4_null"]))]
    for i in sample_valid3:
        r = rows[i]
        updates.append((
            "UPDATE diem_hoc_phan SET diem_he_4 = NULL WHERE ma_diem = :md",
            {"md": r.ma_diem}
        ))
    stats["he4_null"] = len(sample_valid3)
    print(f"  ✏️  diem_he_4 = NULL          : {len(sample_valid3):>4} bản ghi")

    # ── Lỗi 5: diem_cuoi_ky ngoài [0, 10] ────────────────────────────────
    # Thực tế: nhập nhầm 11.5 thay vì 1.15, hoặc -0.5 thay vì 0.5
    sample_valid4 = [valid_idxs[i] for i in
                     sorted(pick(valid_idxs, ERROR_CONFIG["PG_DIEM_cuoi_ky_out_range"]))]
    for i in sample_valid4:
        r = rows[i]
        # Random: score quá cao (>10) hoặc âm (<0)
        bad_score = random.choice([
            round(random.uniform(10.1, 15.0), 2),   # nhập thừa số
            round(random.uniform(-2.0, -0.1), 2),   # nhập âm
        ])
        updates.append((
            "UPDATE diem_hoc_phan SET diem_cuoi_ky = :ck WHERE ma_diem = :md",
            {"ck": bad_score, "md": r.ma_diem}
        ))
    stats["cuoi_ky_out_range"] = len(sample_valid4)
    print(f"  ✏️  diem_cuoi_ky ngoài [0,10]: {len(sample_valid4):>4} bản ghi")

    # ── Thực thi ─────────────────────────────────────────────────────────
    if not dry_run and updates:
        with engine.begin() as conn:
            for sql, params in updates:
                conn.execute(text(sql), params)
        print(f"  ✅ Đã UPDATE {len(updates)} bản ghi vào PostgreSQL diem_hoc_phan")
    elif dry_run:
        print(f"  🔍 [DRY RUN] Sẽ UPDATE {len(updates)} bản ghi (không thực thi)")

    return stats


# ════════════════════════════════════════════════════════════════════════════
# INJECT LỖI VÀO CSV
# ════════════════════════════════════════════════════════════════════════════

def inject_csv_files(dry_run: bool) -> dict:
    """
    Inject lỗi vào tất cả file ctsv_*.csv (trừ ctsv_all.csv).
    Đọc file → inject → ghi đè.
    """
    print("\n" + "─" * 60)
    print("📋 NGUỒN 2 — CSV: thư mục data/csv/")
    print("─" * 60)

    csv_files = sorted(glob.glob(str(CSV_DIR / "ctsv_HK*.csv")))
    if not csv_files:
        # Fallback: thử pattern ctsv_HK*
        csv_files = sorted(glob.glob(str(CSV_DIR / "ctsv_*.csv")))
        csv_files = [f for f in csv_files if "all" not in Path(f).name]

    if not csv_files:
        print(f"  ⚠️  Không tìm thấy file CSV trong {CSV_DIR}")
        return {}

    print(f"  Tìm thấy {len(csv_files)} file CSV")

    total_stats = {
        "drl_trong": 0, "xeploai_sai": 0, "masv_thua_khoang": 0,
        "dong_trung_lap": 0, "tien_am": 0, "hocky_sai": 0,
        "drl_chu": 0, "tong_records": 0
    }

    for fpath in csv_files:
        fname = Path(fpath).name
        df = pd.read_csv(fpath, dtype=str, encoding="utf-8")
        n = len(df)
        total_stats["tong_records"] += n

        # ── Lỗi 1: diem_ren_luyen bị trống ──────────────────────────────
        # Thực tế: nhân viên bỏ qua ô hoặc Excel tự chuyển sang empty
        idxs = list(pick(range(n), ERROR_CONFIG["CSV_drl_trong"]))
        for i in idxs:
            bad_val = random.choice(["", "N/A", "n/a", "#N/A", " ", "chưa có"])
            df.at[i, "diem_ren_luyen"] = bad_val
            # Xếp loại cũng mất theo
            df.at[i, "xep_loai_rl"] = ""
        total_stats["drl_trong"] += len(idxs)

        # ── Lỗi 2: diem_ren_luyen = chuỗi chữ thay vì số ────────────────
        # Thực tế: cột được format Text trong Excel, ai đó nhập chữ nhầm
        idxs2 = list(pick(range(n), ERROR_CONFIG["CSV_drl_chu_thay_so"]))
        # Loại các index đã bị lỗi 1
        idxs2 = [i for i in idxs2 if i not in set(idxs)]
        for i in idxs2:
            bad_val = random.choice(["Chưa nhập", "NULL", "tốt", "khá", "--"])
            df.at[i, "diem_ren_luyen"] = bad_val
        total_stats["drl_chu"] += len(idxs2)

        # ── Lỗi 3: xep_loai_rl không khớp với diem_ren_luyen ─────────────
        # Thực tế: copy-paste cột xếp loại từ file trước, chưa cập nhật
        valid_drl_idxs = [
            i for i in range(n)
            if i not in set(idxs) and i not in set(idxs2)
            and str(df.at[i, "diem_ren_luyen"]).replace(".", "").isdigit()
        ]
        idxs3 = list(pick(valid_drl_idxs, ERROR_CONFIG["CSV_xeploai_sai"]))
        for i in idxs3:
            try:
                drl_val = float(df.at[i, "diem_ren_luyen"])
                df.at[i, "xep_loai_rl"] = _xep_loai_rl_sai(drl_val)
            except (ValueError, TypeError):
                pass
        total_stats["xeploai_sai"] += len(idxs3)

        # ── Lỗi 4: mã SV thừa dấu cách ──────────────────────────────────
        # Thực tế: Excel tự thêm khoảng trắng sau khi copy từ web
        idxs4 = list(pick(range(n), ERROR_CONFIG["CSV_masv_thua_khoang"]))
        for i in idxs4:
            ma = str(df.at[i, "ma_sinh_vien"])
            bad_ma = random.choice([
                ma + " ",          # khoảng trắng cuối
                " " + ma,          # khoảng trắng đầu
                ma + "  ",         # 2 khoảng trắng cuối
                ma.lower(),        # lowercase toàn bộ
                ma[:3] + " " + ma[3:],  # khoảng trắng giữa mã
            ])
            df.at[i, "ma_sinh_vien"] = bad_ma
        total_stats["masv_thua_khoang"] += len(idxs4)

        # ── Lỗi 5: Dòng bị trùng lặp ─────────────────────────────────────
        # Thực tế: nhân viên export 2 lần rồi ghép file, một số dòng bị dupe
        n_dup = max(1, int(n * ERROR_CONFIG["CSV_dong_trung_lap"]))
        if n > 0:
            dup_rows = df.sample(n=min(n_dup, n), random_state=2024)
            df = pd.concat([df, dup_rows], ignore_index=True)
            # Shuffle để trùng lặp không nằm cuối file
            df = df.sample(frac=1, random_state=2024).reset_index(drop=True)
        total_stats["dong_trung_lap"] += n_dup

        # ── Lỗi 6: muc_tien_hb âm ────────────────────────────────────────
        # Thực tế: nhập -1200000 nhầm thay vì 1200000 (gõ thêm dấu trừ)
        hb_idxs = df[
            df["muc_tien_hb"].notna() &
            (df["muc_tien_hb"] != "") &
            (df["muc_tien_hb"] != "0")
        ].index.tolist()
        if hb_idxs:
            n_am = max(1, int(len(hb_idxs) * ERROR_CONFIG["CSV_tien_hb_am"]))
            chosen = random.sample(hb_idxs, min(n_am, len(hb_idxs)))
            for i in chosen:
                try:
                    val = float(df.at[i, "muc_tien_hb"])
                    df.at[i, "muc_tien_hb"] = str(-abs(val))
                except (ValueError, TypeError):
                    df.at[i, "muc_tien_hb"] = "-1200000"
            total_stats["tien_am"] += len(chosen)

        # ── Lỗi 7: hoc_ky format sai ─────────────────────────────────────
        # Thực tế: người xuất file gõ sai format, không theo chuẩn
        idxs7 = list(pick(range(len(df)), ERROR_CONFIG["CSV_hocky_sai_format"]))
        for i in idxs7:
            hk = str(df.at[i, "hoc_ky"])
            bad_hk = random.choice([
                hk.replace("-", "/"),       # "HK1/2024-25"
                hk.replace("-", "_"),       # "HK1_2024_25"
                hk.lower(),                  # "hk1-2024-25"
                hk.replace("HK", "Hk"),    # "Hk1-2024-25"
                hk + " ",                   # khoảng trắng thừa
            ])
            df.at[i, "hoc_ky"] = bad_hk
        total_stats["hocky_sai"] += len(idxs7)

        # ── Ghi lại file ──────────────────────────────────────────────────
        if not dry_run:
            df.to_csv(fpath, index=False, encoding="utf-8")
            print(f"  ✅ {fname}: {n} → {len(df)} records (sau inject)")
        else:
            print(f"  🔍 [DRY RUN] {fname}: {n} → {len(df)} records")

    # Cập nhật lại ctsv_all.csv từ các file đã inject
    if not dry_run:
        all_dfs = []
        for fpath in csv_files:
            all_dfs.append(pd.read_csv(fpath, dtype=str, encoding="utf-8"))
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            combined.to_csv(str(CSV_DIR / "ctsv_all.csv"), index=False, encoding="utf-8")
            print(f"  ✅ ctsv_all.csv cập nhật: {len(combined)} records")

    return total_stats


# ════════════════════════════════════════════════════════════════════════════
# INJECT LỖI VÀO JSON
# ════════════════════════════════════════════════════════════════════════════

def inject_json_files(dry_run: bool) -> dict:
    """
    Inject lỗi vào tất cả file taichinh_HK*.json (trừ taichinh_all.json).
    """
    print("\n" + "─" * 60)
    print("📋 NGUỒN 3 — JSON API: thư mục data/api_json/")
    print("─" * 60)

    json_files = sorted(glob.glob(str(JSON_DIR / "taichinh_HK*.json")))
    if not json_files:
        json_files = sorted(glob.glob(str(JSON_DIR / "taichinh_*.json")))
        json_files = [f for f in json_files if "all" not in Path(f).name]

    if not json_files:
        print(f"  ⚠️  Không tìm thấy file JSON trong {JSON_DIR}")
        return {}

    print(f"  Tìm thấy {len(json_files)} file JSON")

    total_stats = {
        "cono_sai": 0, "ngay_format_sai": 0, "dadong_vuot": 0,
        "hocky_sai": 0, "tien_mien_am": 0, "thieu_truong": 0,
        "tong_records": 0
    }

    for fpath in json_files:
        fname = Path(fpath).name
        with open(fpath, "r", encoding="utf-8") as f:
            records = json.load(f)

        n = len(records)
        total_stats["tong_records"] += n
        if n == 0:
            continue

        # Tạo list index để pick lỗi
        all_idxs = list(range(n))

        # ── Lỗi 1: con_no tính sai ────────────────────────────────────────
        # Thực tế: vendor API làm tròn số, hoặc bug trong logic tính nợ
        # con_no đúng = hoc_phi_phai_dong - da_dong
        # inject: con_no += random sai số nhỏ
        idxs = sorted(pick(all_idxs, ERROR_CONFIG["JSON_cono_tinh_sai"]))
        for i in idxs:
            phi    = records[i].get("hoc_phi_phai_dong", 0)
            da_dong= records[i].get("da_dong", 0)
            # Sai số từ 1000 đến 50000 VND (làm tròn sai)
            sai_so = random.choice([
                random.randint(1000, 10000),      # cộng thừa
                -random.randint(1000, 10000),     # trừ thiếu
                random.randint(50000, 200000),    # sai lớn
            ])
            records[i]["con_no"] = max(0, (phi - da_dong) + sai_so)
        total_stats["cono_sai"] += len(idxs)

        # ── Lỗi 2: ngay_dong_cuoi format sai ────────────────────────────
        # Thực tế: vendor API trả format dd/mm/yyyy thay vì yyyy-mm-dd
        idxs2 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_ngay_dong_sai_format"]))
        for i in idxs2:
            nd = records[i].get("ngay_dong_cuoi", "2024-09-01")
            if nd and nd != "None":
                try:
                    # Chuyển "2024-09-01" → "01/09/2024" (dd/mm/yyyy)
                    dt = datetime.strptime(str(nd)[:10], "%Y-%m-%d")
                    bad_format = random.choice([
                        dt.strftime("%d/%m/%Y"),       # "01/09/2024"
                        dt.strftime("%d-%m-%Y"),       # "01-09-2024"
                        dt.strftime("%m/%d/%Y"),       # "09/01/2024" (US format)
                        str(nd).replace("-", "."),     # "2024.09.01"
                    ])
                    records[i]["ngay_dong_cuoi"] = bad_format
                except (ValueError, TypeError):
                    records[i]["ngay_dong_cuoi"] = "01/01/2024"
        total_stats["ngay_format_sai"] += len(idxs2)

        # ── Lỗi 3: da_dong > hoc_phi_phai_dong ───────────────────────────
        # Thực tế: SV đóng thừa, hệ thống vendor không kiểm tra upper bound
        idxs3 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_dadong_vuot_phi"]))
        for i in idxs3:
            phi = records[i].get("hoc_phi_phai_dong", 0)
            if phi and phi > 0:
                # Đóng thêm 1 khoản tiền nữa (ví dụ: đóng nhầm 2 lần)
                extra = random.choice([phi * 0.5, phi, random.randint(500000, 2000000)])
                records[i]["da_dong"] = int(phi + extra)
                records[i]["con_no"]  = 0   # đã đóng thừa → nợ = 0 hoặc âm
        total_stats["dadong_vuot"] += len(idxs3)

        # ── Lỗi 4: hoc_ky format sai ─────────────────────────────────────
        # Thực tế: vendor API trả hoc_ky với format khác hệ thống nội bộ
        idxs4 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_hocky_sai_format"]))
        for i in idxs4:
            hk = records[i].get("hoc_ky", "HK1-2024-25")
            bad_hk = random.choice([
                hk.replace("-", "_"),          # "HK1_2024_25"
                hk.replace("HK", "Ky"),        # "Ky1-2024-25"
                hk.replace("HK", "hk"),        # "hk1-2024-25"
                hk.replace("-", "").replace("HK","HocKy"),  # "HocKy120242025"
            ])
            records[i]["hoc_ky"] = bad_hk
        total_stats["hocky_sai"] += len(idxs4)

        # ── Lỗi 5: so_tien_mien_giam âm ──────────────────────────────────
        # Thực tế: vendor API bug — trả số âm khi SV không được miễn giảm
        mien_giam_idxs = [
            i for i in all_idxs
            if records[i].get("duoc_mien_giam") == True
            and records[i].get("so_tien_mien_giam", 0) > 0
        ]
        if mien_giam_idxs:
            n_am = max(1, int(len(mien_giam_idxs) * ERROR_CONFIG["JSON_tien_mien_giam_am"]))
            chosen = random.sample(mien_giam_idxs, min(n_am, len(mien_giam_idxs)))
            for i in chosen:
                records[i]["so_tien_mien_giam"] = -abs(records[i]["so_tien_mien_giam"])
            total_stats["tien_mien_am"] += len(chosen)

        # ── Lỗi 6: thiếu trường ngay_dong_cuoi ──────────────────────────
        # Thực tế: API endpoint mới bỏ trường này, endpoint cũ vẫn có
        idxs6 = sorted(pick(all_idxs, ERROR_CONFIG["JSON_thieu_truong"]))
        for i in idxs6:
            if "ngay_dong_cuoi" in records[i]:
                del records[i]["ngay_dong_cuoi"]   # xóa trường hoàn toàn
        total_stats["thieu_truong"] += len(idxs6)

        # ── Ghi lại file ──────────────────────────────────────────────────
        if not dry_run:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  ✅ {fname}: {n} records (đã inject)")
        else:
            print(f"  🔍 [DRY RUN] {fname}: {n} records")

    # Cập nhật taichinh_all.json
    if not dry_run:
        all_records = []
        for fpath in json_files:
            with open(fpath, "r", encoding="utf-8") as f:
                all_records.extend(json.load(f))
        with open(str(JSON_DIR / "taichinh_all.json"), "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)
        print(f"  ✅ taichinh_all.json cập nhật: {len(all_records)} records")

    return total_stats


# ════════════════════════════════════════════════════════════════════════════
# BACKUP & RESTORE
# ════════════════════════════════════════════════════════════════════════════

def backup_files():
    """Sao lưu toàn bộ file CSV và JSON trước khi inject."""
    BCK_DIR.mkdir(parents=True, exist_ok=True)
    bck_csv = BCK_DIR / "csv"
    bck_json = BCK_DIR / "api_json"
    bck_csv.mkdir(exist_ok=True)
    bck_json.mkdir(exist_ok=True)

    count = 0
    for f in glob.glob(str(CSV_DIR / "*.csv")):
        shutil.copy2(f, bck_csv / Path(f).name)
        count += 1
    for f in glob.glob(str(JSON_DIR / "*.json")):
        shutil.copy2(f, bck_json / Path(f).name)
        count += 1

    print(f"  💾 Đã backup {count} files → {BCK_DIR}")
    return count


def restore_files():
    """Khôi phục file CSV và JSON từ backup."""
    if not BCK_DIR.exists():
        print(f"  ❌ Không tìm thấy backup tại {BCK_DIR}")
        print(f"     Chạy inject_errors.py trước để có backup.")
        return False

    count = 0
    for f in glob.glob(str(BCK_DIR / "csv" / "*.csv")):
        shutil.copy2(f, CSV_DIR / Path(f).name)
        count += 1
    for f in glob.glob(str(BCK_DIR / "api_json" / "*.json")):
        shutil.copy2(f, JSON_DIR / Path(f).name)
        count += 1

    print(f"  ✅ Đã restore {count} files từ backup")
    print(f"  ⚠️  Lưu ý: PostgreSQL KHÔNG được restore tự động.")
    print(f"     Để restore PG, hãy chạy lại generate_sample_data.py")
    return True


# ════════════════════════════════════════════════════════════════════════════
# IN BÁO CÁO TỔNG KẾT
# ════════════════════════════════════════════════════════════════════════════

def print_summary(pg_sv: dict, pg_diem: dict, csv_s: dict, json_s: dict, dry_run: bool):
    mode = "DRY RUN" if dry_run else "ĐÃ THỰC THI"
    print("\n" + "=" * 65)
    print(f"  📊 BÁO CÁO INJECT LỖI — [{mode}]")
    print("=" * 65)

    print("\n  NGUỒN 1 — PostgreSQL sinh_vien:")
    print(f"    email sai format         : {pg_sv.get('email_sai_format', 0):>5} bản ghi")
    print(f"    trang_thai format cũ     : {pg_sv.get('trang_thai_cu', 0):>5} bản ghi")
    print(f"    gioi_tinh format lạ      : {pg_sv.get('gioi_tinh_la', 0):>5} bản ghi")
    print(f"    ngay_sinh tương lai      : {pg_sv.get('ngay_sinh_tuong_lai', 0):>5} bản ghi")

    print("\n  NGUỒN 1 — PostgreSQL diem_hoc_phan:")
    print(f"    diem_tong_ket = NULL     : {pg_diem.get('tong_ket_null', 0):>5} bản ghi")
    print(f"    diem_chu không khớp      : {pg_diem.get('chu_khong_khop', 0):>5} bản ghi")
    print(f"    dat_mon không nhất quán  : {pg_diem.get('dat_mon_sai', 0):>5} bản ghi")
    print(f"    diem_he_4 = NULL         : {pg_diem.get('he4_null', 0):>5} bản ghi")
    print(f"    diem_cuoi_ky ngoài range : {pg_diem.get('cuoi_ky_out_range', 0):>5} bản ghi")

    print(f"\n  NGUỒN 2 — CSV ({csv_s.get('tong_records', 0):,} records gốc):")
    print(f"    diem_rl trống/N/A        : {csv_s.get('drl_trong', 0):>5} bản ghi")
    print(f"    diem_rl là chữ           : {csv_s.get('drl_chu', 0):>5} bản ghi")
    print(f"    xep_loai không khớp ĐRL  : {csv_s.get('xeploai_sai', 0):>5} bản ghi")
    print(f"    mã SV thừa khoảng trắng  : {csv_s.get('masv_thua_khoang', 0):>5} bản ghi")
    print(f"    dòng trùng lặp           : {csv_s.get('dong_trung_lap', 0):>5} bản ghi")
    print(f"    muc_tien_hb âm           : {csv_s.get('tien_am', 0):>5} bản ghi")
    print(f"    hoc_ky format sai        : {csv_s.get('hocky_sai', 0):>5} bản ghi")

    print(f"\n  NGUỒN 3 — JSON ({json_s.get('tong_records', 0):,} records gốc):")
    print(f"    con_no tính sai          : {json_s.get('cono_sai', 0):>5} bản ghi")
    print(f"    ngay_dong format sai     : {json_s.get('ngay_format_sai', 0):>5} bản ghi")
    print(f"    da_dong vượt hoc_phi     : {json_s.get('dadong_vuot', 0):>5} bản ghi")
    print(f"    hoc_ky format sai        : {json_s.get('hocky_sai', 0):>5} bản ghi")
    print(f"    so_tien_mien_giam âm     : {json_s.get('tien_mien_am', 0):>5} bản ghi")
    print(f"    thiếu field ngay_dong    : {json_s.get('thieu_truong', 0):>5} bản ghi")

    total_errors = (
        sum(pg_sv.values()) + sum(pg_diem.values()) +
        sum(v for k, v in csv_s.items() if k != "tong_records") +
        sum(v for k, v in json_s.items() if k != "tong_records")
    )

    print("\n" + "─" * 65)
    print(f"  TỔNG LỖI ĐÃ INJECT: {total_errors:,} bản ghi / điểm dữ liệu")
    print("─" * 65)

    if not dry_run:
        print("""
  Bước tiếp theo:
    1. Chạy validate:   python scripts/validate_generated_data.py
    2. Chạy ETL:        python scripts/run_etl.py
    3. ETL sẽ tự động:
       - Phát hiện và báo cáo lỗi (ge_validation.py)
       - Xử lý lỗi nhẹ (Transform step)
       - Pipeline dừng nếu lỗi nghiêm trọng (GE checkpoint fail)
    4. Xem kết quả:     http://localhost:3000 (Grafana)

  Để khôi phục dữ liệu sạch:
    python scripts/inject_errors.py --restore
    python scripts/generate_sample_data.py   (để restore PG)
        """)
    else:
        print("\n  [DRY RUN] Không có thay đổi nào được thực hiện.")
        print("  Chạy lại không có --dry-run để thực sự inject lỗi.")

    print("=" * 65)


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Inject lỗi thực tế vào dữ liệu ETL Platform PTIT",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
  python scripts/inject_errors.py                  
  python scripts/inject_errors.py --dry-run       
  python scripts/inject_errors.py --restore        
  python scripts/inject_errors.py --only csv      
  python scripts/inject_errors.py --only json    
  python scripts/inject_errors.py --only postgres  
        """
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ hiển thị sẽ inject gì, không thực sự sửa dữ liệu"
    )
    p.add_argument(
        "--restore", action="store_true",
        help="Khôi phục file CSV/JSON từ backup (PG phải chạy generate lại)"
    )
    p.add_argument(
        "--only", choices=["csv", "json", "postgres", "all"], default="all",
        help="Chỉ inject lỗi vào nguồn cụ thể (mặc định: all)"
    )
    p.add_argument(
        "--no-backup", action="store_true",
        help="Không tạo backup trước khi inject (không khuyến nghị)"
    )
    return p.parse_args()


def main():
    args = parse_args()

    print("=" * 65)
    print("  🔧 INJECT LỖI THỰC TẾ — ETL Platform PTIT Nhóm 8")
    print("=" * 65)
    print(f"  Data dir : {DATA_DIR}")
    print(f"  CSV dir  : {CSV_DIR}")
    print(f"  JSON dir : {JSON_DIR}")
    print(f"  Mode     : {'DRY RUN' if args.dry_run else 'THỰC THI'}")
    print(f"  Nguồn    : {args.only.upper()}")

    # ── Restore mode ─────────────────────────────────────────────────────
    if args.restore:
        print("\n🔄 CHẾ ĐỘ RESTORE")
        restore_files()
        return

    # ── Kiểm tra thư mục tồn tại ─────────────────────────────────────────
    if not CSV_DIR.exists():
        print(f"\n  ❌ Không tìm thấy thư mục CSV: {CSV_DIR}")
        print(f"     Hãy chạy generate_sample_data.py trước!")
        sys.exit(1)

    # ── Backup trước khi inject ───────────────────────────────────────────
    if not args.dry_run and not args.no_backup:
        print(f"\n💾 Đang backup dữ liệu gốc...")
        backup_files()

    # ── Inject từng nguồn ────────────────────────────────────────────────
    pg_sv_stats   = {}
    pg_diem_stats = {}
    csv_stats     = {}
    json_stats    = {}

    if args.only in ("postgres", "all"):
        try:
            pg_sv_stats   = inject_postgres_sinh_vien(args.dry_run)
            pg_diem_stats = inject_postgres_diem(args.dry_run)
        except Exception as e:
            print(f"\n  ⚠️  Lỗi kết nối PostgreSQL: {e}")
            print(f"      Đảm bảo docker-compose đang chạy và DB accessible.")
            print(f"      Bỏ qua PostgreSQL, tiếp tục với CSV/JSON...")

    if args.only in ("csv", "all"):
        csv_stats = inject_csv_files(args.dry_run)

    if args.only in ("json", "all"):
        json_stats = inject_json_files(args.dry_run)

    # ── Báo cáo ──────────────────────────────────────────────────────────
    print_summary(pg_sv_stats, pg_diem_stats, csv_stats, json_stats, args.dry_run)


if __name__ == "__main__":
    main()