"""
=============================================
TẦNG LOAD — Nạp dữ liệu vào Data Warehouse
=============================================
Đồng bộ với warehouse_models.py v2.0:
  - Tạo bảng bằng WarehouseBase.metadata.create_all()
  - Dimension: Upsert + SCD Type 2 (DimSinhVien)
  - Fact: Lookup surrogate key → Insert
  - Agg: Tổng hợp cuối cùng
"""

from typing import Dict, List, Optional, Tuple
from datetime import date

import pandas as pd
import numpy as np
from sqlalchemy import text

from src.config.database import warehouse_engine, WarehouseSession
from src.config.settings import ETL_BATCH_SIZE
from src.models.warehouse_models import (
    WarehouseBase,
    DimDate, DimSinhVien, DimHocPhan, DimGiangVien, DimHocKy,
    FactHocTap, FactDangKy, FactCtsv, FactTaiChinh,
    AggStudentSummary,
)
from src.etl.transform import TransformedData
from src.utils.logger import get_logger

logger = get_logger("etl.load")


class DataLoader:
    """
    Nạp TransformedData vào Data Warehouse.
    Sử dụng ORM models từ warehouse_models.py làm schema chính.
    """

    def __init__(self):
        self.engine = warehouse_engine
        self.batch_size = ETL_BATCH_SIZE
        # Cache surrogate keys: {natural_key: surrogate_key}
        self._sv_key_cache: Dict[str, int] = {}
        self._hp_key_cache: Dict[str, int] = {}
        self._gv_key_cache: Dict[str, int] = {}
        self._hk_key_cache: Dict[str, int] = {}
        self._hp_tc_cache:  Dict[int, int] = {}   # hoc_phan_key → so_tin_chi

    # ═══════════════════════════════════════════
    # ENTRY POINTS
    # ═══════════════════════════════════════════

    def load_all(self, data: TransformedData) -> dict:
        """Nạp toàn bộ dữ liệu đã transform vào warehouse."""
        logger.info("📥 BẮT ĐẦU LOAD VÀO DATA WAREHOUSE")
        logger.info("=" * 70)

        stats = {}

        # 1. Tạo schema nếu chưa có
        self._ensure_schema()

        # 2. Load Dimensions (thứ tự phụ thuộc)
        logger.info("── Bước 1: Load Dimension Tables ──")
        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        # 3. Build surrogate key caches
        logger.info("── Bước 2: Build Surrogate Key Cache ──")
        self._build_key_caches()

        # 4. Load Facts
        logger.info("── Bước 3: Load Fact Tables ──")
        stats["fact_hoc_tap"]   = self._load_fact_hoc_tap(data.fact_diem)
        stats["fact_ctsv"]      = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"] = self._load_fact_tai_chinh(data.fact_tai_chinh)

        # 5. Load Aggregation
        logger.info("── Bước 4: Load Aggregation ──")
        stats["agg_student_summary"] = self._load_agg_summary(data.fact_tong_hop_sv)

        # Summary
        logger.info("=" * 70)
        logger.info("✅ LOAD HOÀN TẤT")
        total = 0
        for name, count in stats.items():
            logger.info(f"   {name:<25s}: {count:>8,} records")
            total += count
        logger.info(f"   {'TỔNG':<25s}: {total:>8,} records")
        logger.info("=" * 70)

        return stats

    def load_incremental(self, data: TransformedData, ma_hoc_ky: str) -> dict:
        """Load incremental cho 1 học kỳ."""
        logger.info(f"📥 INCREMENTAL LOAD — Học kỳ: {ma_hoc_ky}")

        stats = {}
        self._ensure_schema()

        # Dimensions — upsert
        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        self._build_key_caches()

        # Xóa dữ liệu cũ của HK → insert mới
        hk_key = self._hk_key_cache.get(ma_hoc_ky)
        if hk_key:
            self._delete_fact_by_hk(hk_key, ma_hoc_ky)

        stats["fact_hoc_tap"]        = self._load_fact_hoc_tap(data.fact_diem)
        stats["fact_ctsv"]           = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"]      = self._load_fact_tai_chinh(data.fact_tai_chinh)
        stats["agg_student_summary"] = self._load_agg_summary(data.fact_tong_hop_sv)

        logger.info(f"✅ INCREMENTAL LOAD HOÀN TẤT — {ma_hoc_ky}")
        return stats

    # ═══════════════════════════════════════════
    # SCHEMA
    # ═══════════════════════════════════════════

    def _ensure_schema(self):
        """Tạo tất cả bảng warehouse từ ORM models."""
        logger.info("  Tạo schema warehouse từ warehouse_models.py...")
        WarehouseBase.metadata.create_all(self.engine)
        logger.info("  Schema warehouse OK ✓")

    # ═══════════════════════════════════════════
    # SURROGATE KEY CACHE
    # ═══════════════════════════════════════════

    def _build_key_caches(self):
        """Load toàn bộ mapping natural_key → surrogate_key."""
        logger.info("  Building surrogate key caches...")

        # DimSinhVien: ma_sinh_vien → sinh_vien_key (chỉ bản hiện tại)
        df = pd.read_sql(
            "SELECT sinh_vien_key, ma_sinh_vien FROM dim_sinh_vien "
            "WHERE la_ban_hien_tai = TRUE",
            self.engine,
        )
        self._sv_key_cache = dict(zip(df["ma_sinh_vien"], df["sinh_vien_key"]))

        # DimHocPhan: ma_hoc_phan → hoc_phan_key + so_tin_chi cache
        df = pd.read_sql(
            "SELECT hoc_phan_key, ma_hoc_phan, so_tin_chi FROM dim_hoc_phan",
            self.engine,
        )
        self._hp_key_cache = dict(zip(df["ma_hoc_phan"], df["hoc_phan_key"]))
        self._hp_tc_cache  = dict(zip(df["hoc_phan_key"], df["so_tin_chi"]))

        # DimGiangVien: ma_giang_vien → giang_vien_key
        df = pd.read_sql(
            "SELECT giang_vien_key, ma_giang_vien FROM dim_giang_vien",
            self.engine,
        )
        self._gv_key_cache = dict(zip(df["ma_giang_vien"], df["giang_vien_key"]))

        # DimHocKy: ma_hoc_ky → hoc_ky_key
        df = pd.read_sql(
            "SELECT hoc_ky_key, ma_hoc_ky FROM dim_hoc_ky",
            self.engine,
        )
        self._hk_key_cache = dict(zip(df["ma_hoc_ky"], df["hoc_ky_key"]))

        logger.info(
            f"    SV={len(self._sv_key_cache)}, "
            f"HP={len(self._hp_key_cache)}, "
            f"GV={len(self._gv_key_cache)}, "
            f"HK={len(self._hk_key_cache)}"
        )

    def _lookup_sv_key(self, ma_sv: str) -> Optional[int]:
        return self._sv_key_cache.get(ma_sv)

    def _lookup_hp_key(self, ma_hp: str) -> Optional[int]:
        return self._hp_key_cache.get(ma_hp)

    def _lookup_gv_key(self, ma_gv: str) -> Optional[int]:
        return self._gv_key_cache.get(ma_gv) if ma_gv else None

    def _lookup_hk_key(self, ma_hk: str) -> Optional[int]:
        return self._hk_key_cache.get(ma_hk)

    # ═══════════════════════════════════════════
    # LOAD DIMENSIONS
    # ═══════════════════════════════════════════

    def _load_dim_hoc_ky(self, df: pd.DataFrame) -> int:
        """Load DimHocKy — upsert theo ma_hoc_ky."""
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_hk = row.get("ma_hoc_ky")
                if not ma_hk:
                    continue

                existing = session.query(DimHocKy).filter_by(
                    ma_hoc_ky=ma_hk
                ).first()

                if existing:
                    existing.nam_hoc      = row.get("nam_hoc", existing.nam_hoc)
                    existing.hoc_ky       = row.get("hoc_ky", existing.hoc_ky)
                    existing.ngay_bat_dau = self._to_date(row.get("ngay_bat_dau"))
                    existing.ngay_ket_thuc = self._to_date(row.get("ngay_ket_thuc"))
                    existing.nam_bat_dau  = row.get("nam_bat_dau")
                    existing.nam_ket_thuc = row.get("nam_ket_thuc")
                else:
                    obj = DimHocKy(
                        ma_hoc_ky=ma_hk,
                        nam_hoc=row.get("nam_hoc", ""),
                        hoc_ky=row.get("hoc_ky", ""),
                        ngay_bat_dau=self._to_date(row.get("ngay_bat_dau")),
                        ngay_ket_thuc=self._to_date(row.get("ngay_ket_thuc")),
                        nam_bat_dau=row.get("nam_bat_dau"),
                        nam_ket_thuc=row.get("nam_ket_thuc"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_ky       → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_ky | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_giang_vien(self, df: pd.DataFrame) -> int:
        """Load DimGiangVien — upsert theo ma_giang_vien."""
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_gv = row.get("ma_giang_vien")
                if not ma_gv:
                    continue

                existing = session.query(DimGiangVien).filter_by(
                    ma_giang_vien=ma_gv
                ).first()

                if existing:
                    existing.ho_ten              = row.get("ho_ten", existing.ho_ten)
                    existing.chuc_danh           = row.get("chuc_danh", existing.chuc_danh)
                    existing.trang_thai_cong_tac = row.get("trang_thai_cong_tac", existing.trang_thai_cong_tac)
                    existing.ma_khoa             = row.get("ma_khoa", existing.ma_khoa)
                    existing.ten_khoa            = row.get("ten_khoa", existing.ten_khoa)
                else:
                    obj = DimGiangVien(
                        ma_giang_vien=ma_gv,
                        ho=row.get("ho", ""),
                        ten=row.get("ten", ""),
                        ho_ten=row.get("ho_ten", ""),
                        email=row.get("email"),
                        chuc_danh=row.get("chuc_danh"),
                        trang_thai_cong_tac=row.get("trang_thai_cong_tac"),
                        ma_khoa=row.get("ma_khoa"),
                        ten_khoa=row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_giang_vien   → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_giang_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_hoc_phan(self, df: pd.DataFrame) -> int:
        """Load DimHocPhan — upsert theo ma_hoc_phan."""
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_hp = row.get("ma_hoc_phan")
                if not ma_hp:
                    continue

                existing = session.query(DimHocPhan).filter_by(
                    ma_hoc_phan=ma_hp
                ).first()

                if existing:
                    existing.ten_mon   = row.get("ten_mon", existing.ten_mon)
                    existing.so_tin_chi = row.get("so_tin_chi", existing.so_tin_chi)
                    existing.ma_khoa   = row.get("ma_khoa", existing.ma_khoa)
                    existing.ten_khoa  = row.get("ten_khoa", existing.ten_khoa)
                else:
                    obj = DimHocPhan(
                        ma_hoc_phan=ma_hp,
                        ma_mon=row.get("ma_mon"),
                        ten_mon=row.get("ten_mon", ""),
                        so_tin_chi=row.get("so_tin_chi"),
                        so_gio_ly_thuyet=row.get("so_gio_ly_thuyet"),
                        so_gio_thuc_hanh=row.get("so_gio_thuc_hanh"),
                        hoc_ky_de_xuat=row.get("hoc_ky_de_xuat"),
                        bat_buoc=row.get("bat_buoc"),
                        ma_khoa=row.get("ma_khoa"),
                        ten_khoa=row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_phan     → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_phan | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_sinh_vien(self, df: pd.DataFrame) -> int:
        """
        Load DimSinhVien — SCD Type 2.

        Nếu SV đã có và dữ liệu thay đổi:
          1. Đóng bản ghi cũ (ngay_het_hieu_luc, la_ban_hien_tai=False)
          2. Insert bản ghi mới (phien_ban += 1)
        Nếu SV chưa có:
          Insert bản ghi mới (phien_ban=1)
        """
        if df.empty:
            return 0

        session  = WarehouseSession()
        inserted = 0
        updated  = 0

        # Các cột trigger SCD2
        scd2_cols = ["trang_thai_hoc_tap", "ma_nganh", "ma_lop", "ma_khoa"]

        try:
            for _, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                if not ma_sv:
                    continue

                current = session.query(DimSinhVien).filter(
                    DimSinhVien.ma_sinh_vien == ma_sv,
                    DimSinhVien.la_ban_hien_tai == True,
                ).first()

                if current is None:
                    # INSERT mới
                    obj = DimSinhVien(
                        ma_sinh_vien=ma_sv,
                        ho=row.get("ho"),
                        ten=row.get("ten"),
                        ho_ten=row.get("ho_ten", ""),
                        ngay_sinh=self._to_date(row.get("ngay_sinh")),
                        gioi_tinh=row.get("gioi_tinh"),
                        email=row.get("email"),
                        khoa_hoc=row.get("khoa_hoc"),
                        trang_thai_hoc_tap=row.get("trang_thai_hoc_tap"),
                        ma_nganh=row.get("ma_nganh"),
                        ten_nganh=row.get("ten_nganh"),
                        ma_khoa=row.get("ma_khoa"),
                        ten_khoa=row.get("ten_khoa"),
                        ma_co_van=row.get("ma_co_van"),
                        ten_co_van=row.get("ten_co_van"),
                        ma_lop=row.get("ma_lop"),
                        ten_lop=row.get("ten_lop"),
                        la_ban_hien_tai=True,
                        phien_ban=1,
                    )
                    session.add(obj)
                    inserted += 1

                else:
                    # Kiểm tra thay đổi SCD2
                    changed = any(
                        getattr(current, col, None) != row.get(col)
                        and row.get(col) is not None
                        for col in scd2_cols
                    )

                    if changed:
                        # Đóng bản ghi cũ
                        current.la_ban_hien_tai  = False
                        current.ngay_het_hieu_luc = date.today()

                        # Insert bản ghi mới
                        new_obj = DimSinhVien(
                            ma_sinh_vien=ma_sv,
                            ho=row.get("ho"),
                            ten=row.get("ten"),
                            ho_ten=row.get("ho_ten", ""),
                            ngay_sinh=self._to_date(row.get("ngay_sinh")),
                            gioi_tinh=row.get("gioi_tinh"),
                            email=row.get("email"),
                            khoa_hoc=row.get("khoa_hoc"),
                            trang_thai_hoc_tap=row.get("trang_thai_hoc_tap"),
                            ma_nganh=row.get("ma_nganh"),
                            ten_nganh=row.get("ten_nganh"),
                            ma_khoa=row.get("ma_khoa"),
                            ten_khoa=row.get("ten_khoa"),
                            ma_co_van=row.get("ma_co_van"),
                            ten_co_van=row.get("ten_co_van"),
                            ma_lop=row.get("ma_lop"),
                            ten_lop=row.get("ten_lop"),
                            la_ban_hien_tai=True,
                            phien_ban=current.phien_ban + 1,
                        )
                        session.add(new_obj)
                        updated += 1
                    else:
                        # Cập nhật cột không phải SCD2 (Type 1)
                        current.ho_ten    = row.get("ho_ten",   current.ho_ten)
                        current.email     = row.get("email",    current.email)
                        current.ten_nganh = row.get("ten_nganh", current.ten_nganh)
                        current.ten_khoa  = row.get("ten_khoa",  current.ten_khoa)
                        current.ten_lop   = row.get("ten_lop",   current.ten_lop)

            session.commit()
            logger.info(
                f"  dim_sinh_vien    → {len(df):>6,} records "
                f"(new: {inserted}, SCD2 update: {updated})"
            )
            return inserted + updated

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_sinh_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    # ═══════════════════════════════════════════
    # LOAD FACTS
    # ═══════════════════════════════════════════

    def _load_fact_hoc_tap(self, df: pd.DataFrame) -> int:
        """Load FactHocTap từ fact_diem (đã transform)."""
        if df.empty:
            logger.info("  fact_hoc_tap     → SKIP (empty)")
            return 0

        session = WarehouseSession()
        count = 0

        try:
            for _, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                ma_hp = row.get("ma_hoc_phan")
                ma_hk = row.get("ma_hoc_ky")
                ma_gv = row.get("ma_giang_vien")

                sv_key = self._lookup_sv_key(ma_sv)
                hp_key = self._lookup_hp_key(ma_hp)
                hk_key = self._lookup_hk_key(ma_hk)
                gv_key = self._lookup_gv_key(ma_gv)

                if not all([sv_key, hp_key, hk_key]):
                    continue

                existing = session.query(FactHocTap).filter(
                    FactHocTap.ma_sinh_vien == ma_sv,
                    FactHocTap.ma_hoc_phan  == ma_hp,
                    FactHocTap.hoc_ky_key   == hk_key,
                ).first()

                if existing:
                    existing.diem_chuyen_can = self._to_decimal(row.get("diem_chuyen_can"))
                    existing.diem_bai_tap    = self._to_decimal(row.get("diem_bai_tap"))
                    existing.diem_giua_ky    = self._to_decimal(row.get("diem_giua_ky"))
                    existing.diem_cuoi_ky    = self._to_decimal(row.get("diem_cuoi_ky"))
                    existing.diem_tong_ket   = self._to_decimal(row.get("diem_tong_ket"))
                    existing.diem_chu        = row.get("diem_chu")
                    existing.diem_he_4       = self._to_decimal(row.get("diem_he_4"))
                    existing.dat_mon         = row.get("dat_mon")
                    existing.hoc_lai         = row.get("hoc_lai")
                else:
                    so_tc    = self._hp_tc_cache.get(hp_key)
                    diem_he4 = self._to_decimal(row.get("diem_he_4"))
                    diem_cl  = (float(diem_he4) * so_tc) if diem_he4 and so_tc else None

                    obj = FactHocTap(
                        sinh_vien_key=sv_key,
                        hoc_phan_key=hp_key,
                        giang_vien_key=gv_key,
                        hoc_ky_key=hk_key,
                        ma_sinh_vien=ma_sv,
                        ma_hoc_phan=ma_hp,
                        ma_dang_ky=row.get("ma_dang_ky"),
                        diem_chuyen_can=self._to_decimal(row.get("diem_chuyen_can")),
                        diem_bai_tap=self._to_decimal(row.get("diem_bai_tap")),
                        diem_giua_ky=self._to_decimal(row.get("diem_giua_ky")),
                        diem_cuoi_ky=self._to_decimal(row.get("diem_cuoi_ky")),
                        diem_tong_ket=self._to_decimal(row.get("diem_tong_ket")),
                        diem_chu=row.get("diem_chu"),
                        diem_he_4=diem_he4,
                        dat_mon=row.get("dat_mon"),
                        hoc_lai=row.get("hoc_lai"),
                        so_tin_chi=so_tc,
                        diem_chat_luong=diem_cl,
                        nguon_du_lieu="postgresql",
                    )
                    session.add(obj)
                    count += 1

                if count % self.batch_size == 0:
                    session.flush()

            session.commit()
            logger.info(f"  fact_hoc_tap     → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_hoc_tap | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_fact_ctsv(self, df: pd.DataFrame) -> int:
        """Load FactCtsv từ fact_ren_luyen (CSV Phòng CTSV)."""
        if df.empty:
            logger.info("  fact_ctsv        → SKIP (empty)")
            return 0

        session = WarehouseSession()
        count = 0

        try:
            for _, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                ma_hk = row.get("hoc_ky")

                sv_key = self._lookup_sv_key(ma_sv)
                hk_key = self._lookup_hk_key(ma_hk)

                if not all([sv_key, hk_key]):
                    continue

                existing = session.query(FactCtsv).filter(
                    FactCtsv.ma_sinh_vien == ma_sv,
                    FactCtsv.hoc_ky_key  == hk_key,
                ).first()

                if existing:
                    existing.diem_rl      = self._to_int(row.get("diem_ren_luyen"))
                    existing.xep_loai_rl  = row.get("xep_loai_rl")
                    existing.loai_hoc_bong = row.get("loai_hoc_bong")
                    existing.muc_tien_hb  = self._to_int(row.get("muc_tien_hb")) or 0
                    existing.hinh_thuc_kl = row.get("hinh_thuc_ky_luat")
                    existing.ly_do_kl     = row.get("ly_do_ky_luat")
                    existing.co_hoc_bong  = bool(row.get("co_hoc_bong", False))
                    existing.bi_ky_luat   = bool(row.get("bi_ky_luat", False))
                else:
                    obj = FactCtsv(
                        sinh_vien_key=sv_key,
                        hoc_ky_key=hk_key,
                        ma_sinh_vien=ma_sv,
                        ma_hoc_ky=ma_hk,
                        diem_rl=self._to_int(row.get("diem_ren_luyen")),
                        xep_loai_rl=row.get("xep_loai_rl"),
                        loai_hoc_bong=row.get("loai_hoc_bong"),
                        muc_tien_hb=self._to_int(row.get("muc_tien_hb")) or 0,
                        hinh_thuc_kl=row.get("hinh_thuc_ky_luat"),
                        ly_do_kl=row.get("ly_do_ky_luat"),
                        co_hoc_bong=bool(row.get("co_hoc_bong", False)),
                        bi_ky_luat=bool(row.get("bi_ky_luat", False)),
                        nguon_du_lieu="csv_ctsv",
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  fact_ctsv        → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_ctsv | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_fact_tai_chinh(self, df: pd.DataFrame) -> int:
        """Load FactTaiChinh từ fact_tai_chinh (API Portal)."""
        if df.empty:
            logger.info("  fact_tai_chinh   → SKIP (empty)")
            return 0

        session = WarehouseSession()
        count = 0

        try:
            for _, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                ma_hk = row.get("hoc_ky")

                sv_key = self._lookup_sv_key(ma_sv)
                hk_key = self._lookup_hk_key(ma_hk)

                if not all([sv_key, hk_key]):
                    continue

                existing = session.query(FactTaiChinh).filter(
                    FactTaiChinh.ma_sinh_vien == ma_sv,
                    FactTaiChinh.hoc_ky_key  == hk_key,
                ).first()

                if existing:
                    existing.hoc_phi_phai_dong = self._to_int(row.get("hoc_phi_phai_dong")) or 0
                    existing.da_dong           = self._to_int(row.get("da_dong")) or 0
                    existing.con_no            = self._to_int(row.get("con_no")) or 0
                    existing.duoc_mien_giam    = bool(row.get("duoc_mien_giam", False))
                    existing.ly_do_mien_giam   = row.get("ly_do_mien_giam")
                    existing.so_tien_mien_giam = self._to_int(row.get("so_tien_mien_giam")) or 0
                    existing.ngay_dong_cuoi    = self._to_date(row.get("ngay_dong_cuoi"))
                else:
                    obj = FactTaiChinh(
                        sinh_vien_key=sv_key,
                        hoc_ky_key=hk_key,
                        ma_sinh_vien=ma_sv,
                        ma_hoc_ky=ma_hk,
                        hoc_phi_phai_dong=self._to_int(row.get("hoc_phi_phai_dong")) or 0,
                        da_dong=self._to_int(row.get("da_dong")) or 0,
                        con_no=self._to_int(row.get("con_no")) or 0,
                        duoc_mien_giam=bool(row.get("duoc_mien_giam", False)),
                        ly_do_mien_giam=row.get("ly_do_mien_giam"),
                        so_tien_mien_giam=self._to_int(row.get("so_tien_mien_giam")) or 0,
                        ngay_dong_cuoi=self._to_date(row.get("ngay_dong_cuoi")),
                        nguon_du_lieu="api_portal",
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  fact_tai_chinh   → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_tai_chinh | Lỗi: {e}")
            return 0
        finally:
            session.close()

    # ═══════════════════════════════════════════
    # LOAD AGGREGATION
    # ═══════════════════════════════════════════

    def _load_agg_summary(self, df: pd.DataFrame) -> int:
        """Load AggStudentSummary — tổng hợp 3 nguồn."""
        if df.empty:
            logger.info("  agg_student_summary → SKIP (empty)")
            return 0

        session = WarehouseSession()
        count = 0

        try:
            if "ma_hoc_ky" in df.columns:
                latest = df.sort_values("ma_hoc_ky").groupby("ma_sinh_vien").last().reset_index()
            else:
                latest = df.drop_duplicates(subset=["ma_sinh_vien"], keep="last")

            for _, row in latest.iterrows():
                ma_sv  = row.get("ma_sinh_vien")
                sv_key = self._lookup_sv_key(ma_sv)
                if not sv_key:
                    continue

                hk_key   = self._lookup_hk_key(row.get("ma_hoc_ky"))
                gpa4     = row.get("gpa_hoc_ky_he4")
                xep_loai = self._classify_gpa(gpa4) if gpa4 else None

                muc_rui_ro = "Thap"
                if row.get("nguy_co_bo_hoc"):
                    muc_rui_ro = "Rat cao"
                elif row.get("canh_bao_hoc_vu"):
                    muc_rui_ro = "Cao"
                elif gpa4 and gpa4 < 2.0:
                    muc_rui_ro = "Trung binh"

                existing = session.query(AggStudentSummary).filter_by(
                    ma_sinh_vien=ma_sv
                ).first()

                if existing:
                    existing.gpa_he_4              = self._to_decimal(gpa4)
                    existing.gpa_he_10             = self._to_decimal(row.get("gpa_hoc_ky_he10"))
                    existing.xep_loai_hoc_luc      = xep_loai
                    existing.tong_tin_chi_dang_ky  = self._to_int(row.get("tong_tin_chi")) or 0
                    existing.so_mon_khong_dat      = self._to_int(row.get("so_mon_rot")) or 0
                    existing.tong_mon_dang_ky      = self._to_int(row.get("so_mon_hoc")) or 0
                    existing.diem_rl_trung_binh    = self._to_decimal(row.get("diem_ren_luyen"))
                    existing.xep_loai_rl_gan_nhat  = row.get("xep_loai_rl")
                    existing.tong_no_hoc_phi       = self._to_int(row.get("con_no")) or 0
                    existing.co_no_hoc_phi         = bool(row.get("con_no", 0) > 0)
                    existing.duoc_mien_giam        = bool(row.get("duoc_mien_giam", False))
                    existing.muc_do_rui_ro         = muc_rui_ro
                    existing.canh_bao_hoc_vu       = bool(row.get("canh_bao_hoc_vu", False))
                    existing.hoc_ky_key_gan_nhat   = hk_key
                else:
                    obj = AggStudentSummary(
                        sinh_vien_key=sv_key,
                        ma_sinh_vien=ma_sv,
                        gpa_he_4=self._to_decimal(gpa4),
                        gpa_he_10=self._to_decimal(row.get("gpa_hoc_ky_he10")),
                        xep_loai_hoc_luc=xep_loai,
                        tong_tin_chi_dang_ky=self._to_int(row.get("tong_tin_chi")) or 0,
                        so_mon_khong_dat=self._to_int(row.get("so_mon_rot")) or 0,
                        tong_mon_dang_ky=self._to_int(row.get("so_mon_hoc")) or 0,
                        diem_rl_trung_binh=self._to_decimal(row.get("diem_ren_luyen")),
                        xep_loai_rl_gan_nhat=row.get("xep_loai_rl"),
                        tong_no_hoc_phi=self._to_int(row.get("con_no")) or 0,
                        co_no_hoc_phi=bool(row.get("con_no", 0) > 0),
                        duoc_mien_giam=bool(row.get("duoc_mien_giam", False)),
                        muc_do_rui_ro=muc_rui_ro,
                        canh_bao_hoc_vu=bool(row.get("canh_bao_hoc_vu", False)),
                        hoc_ky_key_gan_nhat=hk_key,
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  agg_student_summary → {len(latest):>6,} records")
            return len(latest)

        except Exception as e:
            session.rollback()
            logger.error(f"  agg_student_summary | Lỗi: {e}")
            return 0
        finally:
            session.close()

    # ═══════════════════════════════════════════
    # DELETE FOR INCREMENTAL
    # ═══════════════════════════════════════════

    def _delete_fact_by_hk(self, hk_key: int, ma_hoc_ky: str):
        """Xóa dữ liệu fact cũ theo học kỳ."""
        tables_and_cols = [
            ("fact_hoc_tap",   "hoc_ky_key"),
            ("fact_ctsv",      "hoc_ky_key"),
            ("fact_tai_chinh", "hoc_ky_key"),
        ]
        with self.engine.begin() as conn:
            for table, col in tables_and_cols:
                try:
                    result = conn.execute(
                        text(f"DELETE FROM {table} WHERE {col} = :hk_key"),
                        {"hk_key": hk_key},
                    )
                    if result.rowcount > 0:
                        logger.info(f"  {table} | Xóa {result.rowcount} records cũ (HK: {ma_hoc_ky})")
                except Exception as e:
                    logger.debug(f"  {table} | Skip delete: {e}")

    # ═══════════════════════════════════════════
    # UTILITIES
    # ═══════════════════════════════════════════

    @staticmethod
    def _to_date(val) -> Optional[date]:
        """Convert to Python date safely."""
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return None
        if isinstance(val, date):
            return val
        if isinstance(val, pd.Timestamp):
            return val.date()
        try:
            return pd.to_datetime(val).date()
        except Exception:
            return None

    @staticmethod
    def _to_decimal(val):
        """Convert to numeric safely."""
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        try:
            return round(float(val), 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_int(val) -> Optional[int]:
        """Convert to int safely."""
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _classify_gpa(gpa4: float) -> str:
        """Xếp loại học lực theo GPA hệ 4."""
        if gpa4 >= 3.6:
            return "Xuat sac"
        elif gpa4 >= 3.2:
            return "Gioi"
        elif gpa4 >= 2.5:
            return "Kha"
        elif gpa4 >= 2.0:
            return "Trung binh"
        elif gpa4 >= 1.0:
            return "Yeu"
        else:
            return "Kem"