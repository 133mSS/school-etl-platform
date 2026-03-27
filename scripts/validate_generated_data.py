"""
scripts/validate_generated_data.py
===================================
Kiểm tra tính logic, nhất quán giữa 3 nguồn dữ liệu đã sinh.

Phát hiện các trường hợp phi logic như:
  - SV thôi học HK1 nhưng vẫn có dữ liệu CSV/API ở HK2+
  - SV bảo lưu nhưng vẫn đóng tiền, có điểm rèn luyện
  - con_no != hoc_phi_phai_dong - da_dong
  - SV có trong CSV/API nhưng không tồn tại trong PostgreSQL
  - SV tốt nghiệp nhưng thiếu dữ liệu các HK trước đó

Chạy: python scripts/validate_generated_data.py
"""

import sys, os, json, glob
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text
from src.config.database import source_engine

# ═══════════════════════════════════════════
# CẤU HÌNH
# ═══════════════════════════════════════════
CSV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "csv")
JSON_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "api_json")

# Nếu data/csv chưa có, thử generated_data/csv
if not os.path.exists(CSV_DIR) or not os.listdir(CSV_DIR):
    CSV_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "generated_data", "csv")
if not os.path.exists(JSON_DIR) or not os.listdir(JSON_DIR):
    JSON_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "generated_data", "api_json")


# Thứ tự HK theo cohort (từ generate_sample_data.py)
HK_SEQ = {
    "B21": ["HK1-2021-22", "HK2-2021-22", "HK1-2022-23", "HK2-2022-23",
            "HK1-2023-24", "HK2-2023-24", "HK1-2024-25", "HK2-2024-25", "HK1-2025-26"],
    "B22": ["HK1-2022-23", "HK2-2022-23", "HK1-2023-24", "HK2-2023-24",
            "HK1-2024-25", "HK2-2024-25", "HK1-2025-26"],
    "B23": ["HK1-2023-24", "HK2-2023-24", "HK1-2024-25", "HK2-2024-25", "HK1-2025-26"],
    "B24": ["HK1-2024-25", "HK2-2024-25", "HK1-2025-26"],
}

# Số HK đã có điểm theo cohort (từ COHORT_CONFIG)
HK_DA_DIEM = {"B21": 8, "B22": 6, "B23": 4, "B24": 2}
MAX_HK_BY_NGANH = {"KE_TOAN": 8, "CNTT": 9, "DTVT": 9}


class DataValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.stats = {}

    def error(self, category, msg, examples=None):
        entry = {"category": category, "message": msg}
        if examples:
            entry["examples"] = examples[:5]  # Max 5 ví dụ
        self.errors.append(entry)

    def warn(self, category, msg, examples=None):
        entry = {"category": category, "message": msg}
        if examples:
            entry["examples"] = examples[:5]
        self.warnings.append(entry)

    def ok(self, category, msg):
        print(f"  ✅ [{category}] {msg}")

    # ═══════════════════════════════════════
    # LOAD DATA
    # ═══════════════════════════════════════

    def load_all(self):
        """Load data từ cả 3 nguồn."""
        print("📦 Loading data từ 3 nguồn...")

        # Nguồn 1: PostgreSQL
        self.sv_df = pd.read_sql("SELECT * FROM sinh_vien", source_engine)
        self.dk_df = pd.read_sql("SELECT * FROM dang_ky_hoc_phan", source_engine)
        self.diem_df = pd.read_sql(
            "SELECT d.*, dk.ma_sinh_vien, dk.ma_hoc_phan, dk.ma_hoc_ky "
            "FROM diem_hoc_phan d JOIN dang_ky_hoc_phan dk ON d.ma_dang_ky = dk.ma_dang_ky",
            source_engine,
        )
        self.hp_df = pd.read_sql("SELECT * FROM hoc_phan", source_engine)
        self.th_df = pd.read_sql("SELECT * FROM tong_hop_ket_qua", source_engine)

        print(f"  PostgreSQL: {len(self.sv_df)} SV, {len(self.dk_df)} đăng ký, {len(self.diem_df)} điểm")

        # Nguồn 2: CSV
        csv_files = sorted(glob.glob(os.path.join(CSV_DIR, "ctsv_*.csv")))
        csv_files = [f for f in csv_files if "all" not in os.path.basename(f).lower()]
        if csv_files:
            self.csv_df = pd.concat([pd.read_csv(f, dtype=str) for f in csv_files], ignore_index=True)
            self.csv_df["diem_ren_luyen"] = pd.to_numeric(self.csv_df["diem_ren_luyen"], errors="coerce")
            self.csv_df["muc_tien_hb"] = pd.to_numeric(self.csv_df["muc_tien_hb"], errors="coerce").fillna(0)
        else:
            self.csv_df = pd.DataFrame()
        print(f"  CSV:        {len(self.csv_df)} records từ {len(csv_files)} files")

        # Nguồn 3: JSON
        json_files = sorted(glob.glob(os.path.join(JSON_DIR, "taichinh_*.json")))
        json_files = [f for f in json_files if "all" not in os.path.basename(f).lower()]
        all_records = []
        for f in json_files:
            with open(f, "r", encoding="utf-8") as fp:
                all_records.extend(json.load(fp))
        self.api_df = pd.DataFrame(all_records) if all_records else pd.DataFrame()
        if not self.api_df.empty:
            for col in ["hoc_phi_phai_dong", "da_dong", "con_no", "so_tien_mien_giam"]:
                if col in self.api_df.columns:
                    self.api_df[col] = pd.to_numeric(self.api_df[col], errors="coerce").fillna(0)
        print(f"  API/JSON:   {len(self.api_df)} records từ {len(json_files)} files")

        # Build lookup
        self.sv_status = dict(zip(self.sv_df["ma_sinh_vien"], self.sv_df["trang_thai_hoc_tap"]))
        self.sv_cohort = dict(zip(self.sv_df["ma_sinh_vien"], self.sv_df["khoa_hoc"]))
        self.sv_nganh = dict(zip(self.sv_df["ma_sinh_vien"], self.sv_df["ma_nganh"]))
        self.sv_set = set(self.sv_df["ma_sinh_vien"])

        # HK mà mỗi SV có đăng ký trong PostgreSQL
        self.sv_hk_enrolled = (
            self.dk_df.groupby("ma_sinh_vien")["ma_hoc_ky"]
            .apply(set).to_dict()
        )

    # ═══════════════════════════════════════
    # CHECK 1: CROSS-SOURCE — SV tồn tại
    # ═══════════════════════════════════════

    def check_sv_exists_in_source(self):
        """Mọi SV trong CSV/API phải tồn tại trong PostgreSQL."""
        print("\n🔍 CHECK 1: SV trong CSV/API có tồn tại trong PostgreSQL?")

        # CSV
        if not self.csv_df.empty:
            csv_svs = set(self.csv_df["ma_sinh_vien"].unique())
            orphan_csv = csv_svs - self.sv_set
            if orphan_csv:
                self.error("CROSS-SOURCE", f"{len(orphan_csv)} SV trong CSV không có trong PostgreSQL", list(orphan_csv))
            else:
                self.ok("CROSS-SOURCE", f"Tất cả {len(csv_svs)} SV trong CSV đều tồn tại trong PG")

        # API
        if not self.api_df.empty:
            api_svs = set(self.api_df["ma_sinh_vien"].unique())
            orphan_api = api_svs - self.sv_set
            if orphan_api:
                self.error("CROSS-SOURCE", f"{len(orphan_api)} SV trong API không có trong PostgreSQL", list(orphan_api))
            else:
                self.ok("CROSS-SOURCE", f"Tất cả {len(api_svs)} SV trong API đều tồn tại trong PG")

    # ═══════════════════════════════════════
    # CHECK 2: HK CONSISTENCY — SV có dữ liệu đúng HK
    # ═══════════════════════════════════════

    def check_hk_consistency(self):
        """
        SV chỉ nên có dữ liệu CSV/API cho các HK mà họ có đăng ký trong PG.
        SV thôi học/bảo lưu không nên có dữ liệu sau khi rời trường.
        """
        print("\n🔍 CHECK 2: HK consistency — SV có dữ liệu ngoài HK đăng ký?")

        violations_csv = []
        violations_api = []

        # CSV: mỗi (ma_sinh_vien, hoc_ky) phải tồn tại trong dk_df
        if not self.csv_df.empty:
            for _, row in self.csv_df.iterrows():
                msv = row["ma_sinh_vien"]
                hk = row["hoc_ky"]
                enrolled_hks = self.sv_hk_enrolled.get(msv, set())
                if hk not in enrolled_hks:
                    violations_csv.append(f"{msv} có CSV ở {hk} nhưng không có đăng ký PG")

            if violations_csv:
                self.error("HK-CSV", f"{len(violations_csv)} records CSV ở HK không có đăng ký PG", violations_csv)
            else:
                self.ok("HK-CSV", "Tất cả CSV records khớp HK đăng ký trong PG")

        # API: tương tự
        if not self.api_df.empty:
            for _, row in self.api_df.iterrows():
                msv = row["ma_sinh_vien"]
                hk = row["hoc_ky"]
                enrolled_hks = self.sv_hk_enrolled.get(msv, set())
                if hk not in enrolled_hks:
                    violations_api.append(f"{msv} có API ở {hk} nhưng không có đăng ký PG")

            if violations_api:
                self.error("HK-API", f"{len(violations_api)} records API ở HK không có đăng ký PG", violations_api)
            else:
                self.ok("HK-API", "Tất cả API records khớp HK đăng ký trong PG")

    # ═══════════════════════════════════════
    # CHECK 3: THÔI HỌC / BẢO LƯU logic
    # ═══════════════════════════════════════

    def check_dropout_logic(self):
        """
        SV 'Thôi học' không nên có dữ liệu ở HK cuối cùng/sau cùng.
        SV 'Bảo lưu' tương tự.
        """
        print("\n🔍 CHECK 3: SV thôi học/bảo lưu có dữ liệu phi logic?")

        # Tìm HK cuối cùng mà mỗi SV có đăng ký
        sv_last_hk = {}
        for msv, hks in self.sv_hk_enrolled.items():
            cohort = self.sv_cohort.get(msv, "B24")
            hk_seq = HK_SEQ.get(cohort, [])
            # Tìm index lớn nhất
            max_idx = -1
            for hk in hks:
                if hk in hk_seq:
                    idx = hk_seq.index(hk)
                    max_idx = max(max_idx, idx)
            sv_last_hk[msv] = max_idx

        thoi_hoc_issues = []
        bao_luu_issues = []

        for msv in self.sv_set:
            status = self.sv_status.get(msv, "")
            cohort = self.sv_cohort.get(msv, "B24")
            nganh = self.sv_nganh.get(msv, "CNTT")
            hk_seq = HK_SEQ.get(cohort, [])
            max_hk = MAX_HK_BY_NGANH.get(nganh, 9)
            last_hk_idx = sv_last_hk.get(msv, -1)

            if status == "Thôi học":
                # SV thôi học: kiểm tra CSV/API ở các HK sau HK cuối cùng
                if not self.csv_df.empty:
                    csv_hks = set(self.csv_df[self.csv_df["ma_sinh_vien"] == msv]["hoc_ky"])
                    for hk in csv_hks:
                        if hk in hk_seq:
                            idx = hk_seq.index(hk)
                            if idx > last_hk_idx:
                                thoi_hoc_issues.append(f"{msv} thôi học nhưng có CSV ở {hk}")

                if not self.api_df.empty:
                    api_hks = set(self.api_df[self.api_df["ma_sinh_vien"] == msv]["hoc_ky"])
                    for hk in api_hks:
                        if hk in hk_seq:
                            idx = hk_seq.index(hk)
                            if idx > last_hk_idx:
                                thoi_hoc_issues.append(f"{msv} thôi học nhưng có API ở {hk}")

        if thoi_hoc_issues:
            self.error("DROPOUT", f"{len(thoi_hoc_issues)} SV thôi học có dữ liệu sau khi rời", thoi_hoc_issues)
        else:
            thoi_hoc_count = sum(1 for s in self.sv_status.values() if s == "Thôi học")
            self.ok("DROPOUT", f"{thoi_hoc_count} SV thôi học — không có dữ liệu phi logic")

    # ═══════════════════════════════════════
    # CHECK 4: TÀI CHÍNH logic
    # ═══════════════════════════════════════

    def check_financial_logic(self):
        """
        con_no phải = hoc_phi_phai_dong - da_dong
        hoc_phi_phai_dong > 0
        da_dong >= 0
        mien_giam: nếu duoc_mien_giam=True thì so_tien_mien_giam > 0
        """
        print("\n🔍 CHECK 4: Logic tài chính")

        if self.api_df.empty:
            print("  ⚠️ Không có dữ liệu API để kiểm tra")
            return

        # 4a. con_no = hoc_phi_phai_dong - da_dong
        expected_no = self.api_df["hoc_phi_phai_dong"] - self.api_df["da_dong"]
        diff = (self.api_df["con_no"] - expected_no).abs()
        mismatch = self.api_df[diff > 100]  # tolerance 100 VND
        if len(mismatch) > 0:
            examples = [
                f"{r['ma_sinh_vien']} HK {r['hoc_ky']}: "
                f"phi={r['hoc_phi_phai_dong']}, dong={r['da_dong']}, "
                f"no={r['con_no']}, expected={r['hoc_phi_phai_dong']-r['da_dong']}"
                for _, r in mismatch.head(5).iterrows()
            ]
            self.error("FINANCE", f"{len(mismatch)} records: con_no ≠ hoc_phi - da_dong", examples)
        else:
            self.ok("FINANCE", f"con_no = hoc_phi - da_dong ✓ (tất cả {len(self.api_df)} records)")

        # 4b. hoc_phi > 0
        negative_hp = self.api_df[self.api_df["hoc_phi_phai_dong"] < 0]
        if len(negative_hp) > 0:
            self.error("FINANCE", f"{len(negative_hp)} records có hoc_phi < 0")
        else:
            self.ok("FINANCE", "hoc_phi_phai_dong >= 0 ✓")

        # 4c. da_dong >= 0
        negative_dd = self.api_df[self.api_df["da_dong"] < 0]
        if len(negative_dd) > 0:
            self.error("FINANCE", f"{len(negative_dd)} records có da_dong < 0")
        else:
            self.ok("FINANCE", "da_dong >= 0 ✓")

        # 4d. Miễn giảm logic
        if "duoc_mien_giam" in self.api_df.columns and "so_tien_mien_giam" in self.api_df.columns:
            mien_giam = self.api_df[self.api_df["duoc_mien_giam"] == True]
            no_amount = mien_giam[mien_giam["so_tien_mien_giam"] <= 0]
            if len(no_amount) > 0:
                examples = [f"{r['ma_sinh_vien']} HK {r['hoc_ky']}" for _, r in no_amount.head(5).iterrows()]
                self.error("FINANCE", f"{len(no_amount)} SV được miễn giảm nhưng so_tien = 0", examples)
            else:
                self.ok("FINANCE", f"Miễn giảm logic ✓ ({len(mien_giam)} SV có miễn giảm)")

    # ═══════════════════════════════════════
    # CHECK 5: CSV — Điểm rèn luyện logic
    # ═══════════════════════════════════════

    def check_csv_logic(self):
        """
        diem_ren_luyen phải trong [0, 100]
        xep_loai_rl phải khớp với diem_ren_luyen
        Học bổng: nếu có loại thì muc_tien > 0
        Kỷ luật: nếu có hình thức thì phải có lý do
        """
        print("\n🔍 CHECK 5: Logic CSV (rèn luyện, học bổng, kỷ luật)")

        if self.csv_df.empty:
            print("  ⚠️ Không có dữ liệu CSV")
            return

        # 5a. Điểm RL trong [0, 100]
        out_range = self.csv_df[
            (self.csv_df["diem_ren_luyen"].notna()) &
            ((self.csv_df["diem_ren_luyen"] < 0) | (self.csv_df["diem_ren_luyen"] > 100))
        ]
        if len(out_range) > 0:
            self.error("CSV-RL", f"{len(out_range)} records có diem_ren_luyen ngoài [0,100]")
        else:
            self.ok("CSV-RL", "diem_ren_luyen ∈ [0, 100] ✓")

        # 5b. Xếp loại khớp điểm
        def expected_xep_loai(drl):
            if pd.isna(drl): return None
            drl = float(drl)
            if drl >= 90: return "Xuất sắc"
            if drl >= 80: return "Tốt"
            if drl >= 65: return "Khá"
            if drl >= 50: return "Trung bình"
            if drl >= 35: return "Yếu"
            return "Kém"

        mismatch_xl = []
        for _, row in self.csv_df.iterrows():
            expected = expected_xep_loai(row["diem_ren_luyen"])
            actual = row.get("xep_loai_rl", "")
            if expected and actual and str(actual).strip() != str(expected).strip():
                mismatch_xl.append(
                    f"{row['ma_sinh_vien']} HK {row['hoc_ky']}: "
                    f"ĐRL={row['diem_ren_luyen']}, expected={expected}, actual={actual}"
                )

        if mismatch_xl:
            self.warn("CSV-XL", f"{len(mismatch_xl)} records xếp loại không khớp điểm RL", mismatch_xl)
        else:
            self.ok("CSV-XL", "xep_loai_rl khớp diem_ren_luyen ✓")

        # 5c. Học bổng: có loại → phải có tiền
        if "loai_hoc_bong" in self.csv_df.columns:
            has_hb = self.csv_df[
                (self.csv_df["loai_hoc_bong"].notna()) &
                (self.csv_df["loai_hoc_bong"].str.strip() != "")
            ]
            no_money = has_hb[has_hb["muc_tien_hb"] <= 0]
            if len(no_money) > 0:
                examples = [f"{r['ma_sinh_vien']} HK {r['hoc_ky']}: {r['loai_hoc_bong']}" for _, r in no_money.head(5).iterrows()]
                self.error("CSV-HB", f"{len(no_money)} SV có học bổng nhưng muc_tien = 0", examples)
            else:
                self.ok("CSV-HB", f"Học bổng logic ✓ ({len(has_hb)} records có HB)")

        # 5d. Kỷ luật: có hình thức → phải có lý do
        if "hinh_thuc_ky_luat" in self.csv_df.columns:
            has_kl = self.csv_df[
                (self.csv_df["hinh_thuc_ky_luat"].notna()) &
                (self.csv_df["hinh_thuc_ky_luat"].str.strip() != "")
            ]
            no_reason = has_kl[
                (has_kl["ly_do_ky_luat"].isna()) | (has_kl["ly_do_ky_luat"].str.strip() == "")
            ]
            if len(no_reason) > 0:
                self.error("CSV-KL", f"{len(no_reason)} SV bị kỷ luật nhưng không có lý do")
            else:
                self.ok("CSV-KL", f"Kỷ luật logic ✓ ({len(has_kl)} records có KL)")

    # ═══════════════════════════════════════
    # CHECK 6: CROSS-SOURCE RECORD COUNT
    # ═══════════════════════════════════════

    def check_record_count_match(self):
        """
        Số records CSV và API theo HK nên bằng nhau
        (vì generator tạo song song cho cùng danh sách SV).
        """
        print("\n🔍 CHECK 6: Số records CSV vs API theo HK")

        if self.csv_df.empty or self.api_df.empty:
            print("  ⚠️ Thiếu dữ liệu để so sánh")
            return

        csv_counts = self.csv_df.groupby("hoc_ky").size()
        api_counts = self.api_df.groupby("hoc_ky").size()

        all_hks = sorted(set(csv_counts.index) | set(api_counts.index))
        mismatches = []
        for hk in all_hks:
            csv_n = csv_counts.get(hk, 0)
            api_n = api_counts.get(hk, 0)
            if csv_n != api_n:
                mismatches.append(f"{hk}: CSV={csv_n}, API={api_n}, diff={abs(csv_n-api_n)}")

        if mismatches:
            self.warn("COUNT", f"{len(mismatches)} HK có số records CSV ≠ API", mismatches)
        else:
            self.ok("COUNT", f"CSV và API có cùng số records cho tất cả {len(all_hks)} HK ✓")

    # ═══════════════════════════════════════
    # CHECK 7: SV MÃ KHỚP PATTERN
    # ═══════════════════════════════════════

    def check_sv_id_pattern(self):
        """Kiểm tra ma_sinh_vien đúng format: B21DCKT001, B22DCCN003..."""
        print("\n🔍 CHECK 7: Format mã sinh viên")

        import re
        pattern = re.compile(r"^B2[1-4]DC(KT|VT|CN)\d{3}$")
        invalid = [msv for msv in self.sv_set if not pattern.match(msv)]

        if invalid:
            self.error("FORMAT", f"{len(invalid)} SV có mã không đúng format", invalid)
        else:
            self.ok("FORMAT", f"Tất cả {len(self.sv_set)} mã SV đúng format B2xDCyy###")

    # ═══════════════════════════════════════
    # CHECK 8: GPA vs TRẠNG THÁI
    # ═══════════════════════════════════════

    def check_gpa_vs_status(self):
        """
        SV tốt nghiệp nên có GPA >= 2.0
        SV thôi học thường có GPA thấp
        """
        print("\n🔍 CHECK 8: GPA vs trạng thái học tập")

        if self.th_df.empty:
            print("  ⚠️ Không có dữ liệu tong_hop_ket_qua")
            return

        merged = self.th_df.merge(
            self.sv_df[["ma_sinh_vien", "trang_thai_hoc_tap"]],
            on="ma_sinh_vien",
        )

        # SV tốt nghiệp với GPA < 2.0
        tn_low = merged[
            (merged["trang_thai_hoc_tap"] == "Tốt nghiệp") &
            (merged["gpa_he_4"] < 2.0)
        ]
        if len(tn_low) > 0:
            examples = [f"{r['ma_sinh_vien']}: GPA={r['gpa_he_4']}" for _, r in tn_low.head(5).iterrows()]
            self.error("GPA-STATUS", f"{len(tn_low)} SV tốt nghiệp có GPA < 2.0", examples)
        else:
            tn_count = len(merged[merged["trang_thai_hoc_tap"] == "Tốt nghiệp"])
            self.ok("GPA-STATUS", f"Tất cả {tn_count} SV tốt nghiệp có GPA >= 2.0 ✓")

        # Thống kê GPA theo trạng thái
        gpa_stats = merged.groupby("trang_thai_hoc_tap")["gpa_he_4"].agg(["mean", "min", "max", "count"])
        print("  📊 GPA trung bình theo trạng thái:")
        for status, row in gpa_stats.iterrows():
            print(f"     {status:<15s}: avg={row['mean']:.2f}, min={row['min']:.2f}, max={row['max']:.2f}, n={int(row['count'])}")

    # ═══════════════════════════════════════
    # CHECK 9: DUPLICATE
    # ═══════════════════════════════════════

    def check_duplicates(self):
        """Kiểm tra trùng lặp trong CSV và API."""
        print("\n🔍 CHECK 9: Trùng lặp")

        if not self.csv_df.empty:
            csv_dupes = self.csv_df.duplicated(subset=["ma_sinh_vien", "hoc_ky"]).sum()
            if csv_dupes > 0:
                self.error("DUPE-CSV", f"{csv_dupes} records trùng (ma_sinh_vien + hoc_ky) trong CSV")
            else:
                self.ok("DUPE-CSV", "Không có trùng lặp trong CSV ✓")

        if not self.api_df.empty:
            api_dupes = self.api_df.duplicated(subset=["ma_sinh_vien", "hoc_ky"]).sum()
            if api_dupes > 0:
                self.error("DUPE-API", f"{api_dupes} records trùng trong API")
            else:
                self.ok("DUPE-API", "Không có trùng lặp trong API ✓")

    # ═══════════════════════════════════════
    # CHECK 10: ĐIỂM — giá trị hợp lệ
    # ═══════════════════════════════════════

    def check_grade_values(self):
        """Điểm phải trong [0, 10], diem_chu phải hợp lệ."""
        print("\n🔍 CHECK 10: Giá trị điểm trong PostgreSQL")

        score_cols = ["diem_chuyen_can", "diem_bai_tap", "diem_giua_ky",
                      "diem_cuoi_ky", "diem_tong_ket"]
        for col in score_cols:
            if col in self.diem_df.columns:
                out = self.diem_df[
                    (self.diem_df[col].notna()) &
                    ((self.diem_df[col] < 0) | (self.diem_df[col] > 10))
                ]
                if len(out) > 0:
                    self.error("GRADE", f"{len(out)} records có {col} ngoài [0, 10]")
                else:
                    self.ok("GRADE", f"{col} ∈ [0, 10] ✓")

        # diem_chu hợp lệ
        valid_chu = {"A+", "A", "B+", "B", "C+", "C", "D+", "D", "F"}
        if "diem_chu" in self.diem_df.columns:
            invalid = self.diem_df[
                (self.diem_df["diem_chu"].notna()) &
                (~self.diem_df["diem_chu"].isin(valid_chu))
            ]
            if len(invalid) > 0:
                self.error("GRADE", f"{len(invalid)} records có diem_chu không hợp lệ")
            else:
                self.ok("GRADE", f"diem_chu ∈ {valid_chu} ✓")

    # ═══════════════════════════════════════
    # RUN ALL
    # ═══════════════════════════════════════

    def run_all(self):
        """Chạy tất cả kiểm tra."""
        print("=" * 70)
        print("🔍 KIỂM TRA TÍNH LOGIC DỮ LIỆU ĐÃ SINH")
        print("=" * 70)

        self.load_all()

        self.check_sv_exists_in_source()       # Check 1
        self.check_hk_consistency()             # Check 2
        self.check_dropout_logic()              # Check 3
        self.check_financial_logic()            # Check 4
        self.check_csv_logic()                  # Check 5
        self.check_record_count_match()         # Check 6
        self.check_sv_id_pattern()              # Check 7
        self.check_gpa_vs_status()              # Check 8
        self.check_duplicates()                 # Check 9
        self.check_grade_values()               # Check 10

        # ══════════ TỔNG KẾT ══════════
        print("\n" + "=" * 70)
        print("📋 TỔNG KẾT")
        print("=" * 70)

        if self.errors:
            print(f"\n❌ {len(self.errors)} LỖI (cần fix):")
            for e in self.errors:
                print(f"  ❌ [{e['category']}] {e['message']}")
                if "examples" in e:
                    for ex in e["examples"]:
                        print(f"      → {ex}")

        if self.warnings:
            print(f"\n⚠️ {len(self.warnings)} CẢNH BÁO (chấp nhận được):")
            for w in self.warnings:
                print(f"  ⚠️ [{w['category']}] {w['message']}")
                if "examples" in w:
                    for ex in w["examples"]:
                        print(f"      → {ex}")

        if not self.errors and not self.warnings:
            print("\n🎉 TẤT CẢ KIỂM TRA ĐẠT — Dữ liệu hoàn toàn hợp logic!")
        elif not self.errors:
            print(f"\n✅ Không có lỗi nghiêm trọng — {len(self.warnings)} cảnh báo nhẹ")
        else:
            print(f"\n🔴 Cần fix {len(self.errors)} lỗi trước khi chạy ETL")

        print("=" * 70)


if __name__ == "__main__":
    validator = DataValidator()
    validator.run_all()