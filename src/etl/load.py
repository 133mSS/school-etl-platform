from src.etl.aggregation import DataAggregator
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
    def __init__(self):
        self.engine     = warehouse_engine
        self.batch_size = ETL_BATCH_SIZE

        self._sv_key_cache: Dict[str, int] = {}
        self._hp_key_cache: Dict[str, int] = {}
        self._gv_key_cache: Dict[str, int] = {}
        self._hk_key_cache: Dict[str, int] = {}
        self._hp_tc_cache:  Dict[int, int]  = {}

    # ────────────────────────────────────────────────────────────────
    # LOAD ALL
    # ────────────────────────────────────────────────────────────────

    def load_all(self, data: TransformedData) -> dict:
        logger.info("==BẮT ĐẦU LOAD VÀO DATA WAREHOUSE==")
        logger.info("=" * 70)

        stats = {}
        self._ensure_schema()

        logger.info("── Bước 1: Load Dimension Tables ──")
        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        logger.info("── Bước 2: Build Surrogate Key Cache ──")
        self._build_key_caches()

        logger.info("── Bước 3: Load Fact Tables ──")
        stats["fact_hoc_tap"]   = self._load_fact_hoc_tap(data.fact_diem)
        # FIX: Thêm load fact_dang_ky (trước đây bị bỏ qua hoàn toàn)
        stats["fact_dang_ky"]   = self._load_fact_dang_ky(data.fact_dang_ky)
        stats["fact_ctsv"]      = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"] = self._load_fact_tai_chinh(data.fact_tai_chinh)

        logger.info("── Bước 4: Build Aggregation ──")
        aggregator = DataAggregator()
        stats["agg_student_summary"] = aggregator.run_all()

        logger.info("=" * 70)
        logger.info("LOAD HOÀN TẤT")
        total = 0
        for name, count in stats.items():
            logger.info(f"  {name:<25s}: {count:>8,} records")
            total += count
        logger.info(f"  {'TỔNG':<25s}: {total:>8,} records")
        logger.info("=" * 70)

        return stats

    def load_incremental(self, data: TransformedData, ma_hoc_ky: str) -> dict:
        logger.info(f"INCREMENTAL LOAD — Học kỳ: {ma_hoc_ky}")

        stats = {}
        self._ensure_schema()

        stats["dim_hoc_ky"]     = self._load_dim_hoc_ky(data.dim_thoi_gian)
        stats["dim_giang_vien"] = self._load_dim_giang_vien(data.dim_giang_vien)
        stats["dim_hoc_phan"]   = self._load_dim_hoc_phan(data.dim_hoc_phan)
        stats["dim_sinh_vien"]  = self._load_dim_sinh_vien(data.dim_sinh_vien)

        self._build_key_caches()

        hk_key = self._hk_key_cache.get(ma_hoc_ky)
        if hk_key:
            self._delete_fact_by_hk(hk_key, ma_hoc_ky)

        stats["fact_hoc_tap"]        = self._load_fact_hoc_tap(data.fact_diem)
        stats["fact_dang_ky"]        = self._load_fact_dang_ky(data.fact_dang_ky)  # FIX
        stats["fact_ctsv"]           = self._load_fact_ctsv(data.fact_ren_luyen)
        stats["fact_tai_chinh"]      = self._load_fact_tai_chinh(data.fact_tai_chinh)

        aggregator = DataAggregator()
        stats["agg_student_summary"] = aggregator.run_all()

        logger.info(f"INCREMENTAL LOAD HOÀN TẤT — {ma_hoc_ky}")
        return stats

    # ────────────────────────────────────────────────────────────────
    # INTERNAL HELPERS
    # ────────────────────────────────────────────────────────────────

    def _ensure_schema(self):
        logger.info("Tạo schema warehouse từ warehouse_models.py...")
        WarehouseBase.metadata.create_all(self.engine)
        logger.info("  Schema warehouse OK")

    def _build_key_caches(self):
        logger.info("Building surrogate key caches...")

        df = pd.read_sql(
            "SELECT sinh_vien_key, ma_sinh_vien FROM dim_sinh_vien "
            "WHERE la_ban_hien_tai = TRUE",
            self.engine,
        )
        self._sv_key_cache = dict(zip(df["ma_sinh_vien"], df["sinh_vien_key"]))

        df = pd.read_sql(
            "SELECT hoc_phan_key, ma_hoc_phan, so_tin_chi FROM dim_hoc_phan",
            self.engine,
        )
        self._hp_key_cache = dict(zip(df["ma_hoc_phan"], df["hoc_phan_key"]))
        self._hp_tc_cache  = dict(zip(df["hoc_phan_key"], df["so_tin_chi"]))

        df = pd.read_sql(
            "SELECT giang_vien_key, ma_giang_vien FROM dim_giang_vien",
            self.engine,
        )
        self._gv_key_cache = dict(zip(df["ma_giang_vien"], df["giang_vien_key"]))

        df = pd.read_sql(
            "SELECT hoc_ky_key, ma_hoc_ky FROM dim_hoc_ky",
            self.engine,
        )
        self._hk_key_cache = dict(zip(df["ma_hoc_ky"], df["hoc_ky_key"]))

        logger.info(
            f"  SV={len(self._sv_key_cache)}, "
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

    # ────────────────────────────────────────────────────────────────
    # DIMENSION LOADERS
    # ────────────────────────────────────────────────────────────────

    def _load_dim_hoc_ky(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session = WarehouseSession()
        count = 0
        try:
            for _, row in df.iterrows():
                ma_hk = row.get("ma_hoc_ky")
                if not ma_hk:
                    continue

                existing = session.query(DimHocKy).filter_by(ma_hoc_ky=ma_hk).first()

                if existing:
                    existing.nam_hoc       = row.get("nam_hoc", existing.nam_hoc)
                    existing.hoc_ky        = row.get("hoc_ky", existing.hoc_ky)
                    existing.ngay_bat_dau  = self._to_date(row.get("ngay_bat_dau"))
                    existing.ngay_ket_thuc = self._to_date(row.get("ngay_ket_thuc"))
                    existing.nam_bat_dau   = row.get("nam_bat_dau")
                    existing.nam_ket_thuc  = row.get("nam_ket_thuc")
                else:
                    obj = DimHocKy(
                        ma_hoc_ky     = ma_hk,
                        nam_hoc       = row.get("nam_hoc", ""),
                        hoc_ky        = row.get("hoc_ky", ""),
                        ngay_bat_dau  = self._to_date(row.get("ngay_bat_dau")),
                        ngay_ket_thuc = self._to_date(row.get("ngay_ket_thuc")),
                        nam_bat_dau   = row.get("nam_bat_dau"),
                        nam_ket_thuc  = row.get("nam_ket_thuc"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_ky     → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_ky | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_giang_vien(self, df: pd.DataFrame) -> int:
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
                    existing.trang_thai_cong_tac = row.get(
                        "trang_thai_cong_tac", existing.trang_thai_cong_tac
                    )
                    existing.ma_khoa  = row.get("ma_khoa",  existing.ma_khoa)
                    existing.ten_khoa = row.get("ten_khoa", existing.ten_khoa)
                else:
                    obj = DimGiangVien(
                        ma_giang_vien       = ma_gv,
                        ho                  = row.get("ho", ""),
                        ten                 = row.get("ten", ""),
                        ho_ten              = row.get("ho_ten", ""),
                        email               = row.get("email"),
                        chuc_danh           = row.get("chuc_danh"),
                        trang_thai_cong_tac = row.get("trang_thai_cong_tac"),
                        ma_khoa             = row.get("ma_khoa"),
                        ten_khoa            = row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_giang_vien → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_giang_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_hoc_phan(self, df: pd.DataFrame) -> int:
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
                    existing.ten_mon    = row.get("ten_mon",    existing.ten_mon)
                    existing.so_tin_chi = row.get("so_tin_chi", existing.so_tin_chi)
                    existing.ma_khoa    = row.get("ma_khoa",    existing.ma_khoa)
                    existing.ten_khoa   = row.get("ten_khoa",   existing.ten_khoa)
                else:
                    obj = DimHocPhan(
                        ma_hoc_phan      = ma_hp,
                        ma_mon           = row.get("ma_mon"),
                        ten_mon          = row.get("ten_mon", ""),
                        so_tin_chi       = row.get("so_tin_chi"),
                        so_gio_ly_thuyet = row.get("so_gio_ly_thuyet"),
                        so_gio_thuc_hanh = row.get("so_gio_thuc_hanh"),
                        hoc_ky_de_xuat   = row.get("hoc_ky_de_xuat"),
                        bat_buoc         = row.get("bat_buoc"),
                        ma_khoa          = row.get("ma_khoa"),
                        ten_khoa         = row.get("ten_khoa"),
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  dim_hoc_phan   → {len(df):>6,} records (new: {count})")
            return len(df)

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_hoc_phan | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_dim_sinh_vien(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0

        session      = WarehouseSession()
        inserted     = 0
        updated      = 0
        cascaded_facts = 0

        scd2_cols = ["trang_thai_hoc_tap", "ma_nganh", "ma_lop", "ma_khoa"]

        # FIX: Tính today một lần trước vòng lặp thay vì trong mỗi iteration
        today = date.today()

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
                    obj = DimSinhVien(
                        ma_sinh_vien       = ma_sv,
                        ho                 = row.get("ho"),
                        ten                = row.get("ten"),
                        ho_ten             = row.get("ho_ten", ""),
                        ngay_sinh          = self._to_date(row.get("ngay_sinh")),
                        gioi_tinh          = row.get("gioi_tinh"),
                        email              = row.get("email"),
                        khoa_hoc           = row.get("khoa_hoc"),
                        trang_thai_hoc_tap = row.get("trang_thai_hoc_tap"),
                        ma_nganh           = row.get("ma_nganh"),
                        ten_nganh          = row.get("ten_nganh"),
                        ma_khoa            = row.get("ma_khoa"),
                        ten_khoa           = row.get("ten_khoa"),
                        ma_co_van          = row.get("ma_co_van"),
                        ten_co_van         = row.get("ten_co_van"),
                        ma_lop             = row.get("ma_lop"),
                        ten_lop            = row.get("ten_lop"),
                        la_ban_hien_tai    = True,
                        phien_ban          = 1,
                        ngay_hieu_luc      = today,
                        ngay_het_hieu_luc  = None,
                    )
                    session.add(obj)
                    inserted += 1
                else:
                    changed = self._check_scd2_changed(current, row, scd2_cols)

                    if changed:
                        old_key = current.sinh_vien_key
                        current.la_ban_hien_tai   = False
                        current.ngay_het_hieu_luc = today

                        new_obj = DimSinhVien(
                            ma_sinh_vien       = ma_sv,
                            ho                 = row.get("ho"),
                            ten                = row.get("ten"),
                            ho_ten             = row.get("ho_ten", ""),
                            ngay_sinh          = self._to_date(row.get("ngay_sinh")),
                            gioi_tinh          = row.get("gioi_tinh"),
                            email              = row.get("email"),
                            khoa_hoc           = row.get("khoa_hoc"),
                            trang_thai_hoc_tap = row.get("trang_thai_hoc_tap"),
                            ma_nganh           = row.get("ma_nganh"),
                            ten_nganh          = row.get("ten_nganh"),
                            ma_khoa            = row.get("ma_khoa"),
                            ten_khoa           = row.get("ten_khoa"),
                            ma_co_van          = row.get("ma_co_van"),
                            ten_co_van         = row.get("ten_co_van"),
                            ma_lop             = row.get("ma_lop"),
                            ten_lop            = row.get("ten_lop"),
                            la_ban_hien_tai    = True,
                            phien_ban          = current.phien_ban + 1,
                            ngay_hieu_luc      = today,
                            ngay_het_hieu_luc  = None,
                        )
                        session.add(new_obj)
                        session.flush()

                        new_key = new_obj.sinh_vien_key
                        n = self._cascade_sv_key_update(session, old_key, new_key)
                        cascaded_facts += n
                        updated += 1
                    else:
                        current.ho_ten     = row.get("ho_ten",     current.ho_ten)
                        current.email      = row.get("email",      current.email)
                        current.ten_nganh  = row.get("ten_nganh",  current.ten_nganh)
                        current.ten_khoa   = row.get("ten_khoa",   current.ten_khoa)
                        current.ten_lop    = row.get("ten_lop",    current.ten_lop)
                        current.ten_co_van = row.get("ten_co_van", current.ten_co_van)
                        current.ma_co_van  = row.get("ma_co_van",  current.ma_co_van)

            session.commit()
            logger.info(
                f"  dim_sinh_vien → {len(df):>6,} records "
                f"(new: {inserted}, SCD2: {updated}, "
                f"fact cascade: {cascaded_facts})"
            )
            return inserted + updated

        except Exception as e:
            session.rollback()
            logger.error(f"  dim_sinh_vien | Lỗi: {e}")
            return 0
        finally:
            session.close()

    # ────────────────────────────────────────────────────────────────
    # SCD TYPE 2 HELPERS
    # ────────────────────────────────────────────────────────────────

    @staticmethod
    def _normalize_scd2_val(val):
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        s = str(val).strip()
        if s == "" or s.lower() in ("none", "nan"):
            return None
        return s

    def _check_scd2_changed(self, current, row, scd2_cols) -> bool:
        for col in scd2_cols:
            old_val = self._normalize_scd2_val(getattr(current, col, None))
            new_val = self._normalize_scd2_val(row.get(col))
            if new_val is not None and old_val != new_val:
                return True
        return False

    def _cascade_sv_key_update(
        self, session, old_key: int, new_key: int
    ) -> int:
        total = 0
        fact_models = [
            FactHocTap,
            FactDangKy,
            FactCtsv,
            FactTaiChinh,
            AggStudentSummary,
        ]
        for Model in fact_models:
            try:
                n = (
                    session.query(Model)
                    .filter(Model.sinh_vien_key == old_key)
                    .update(
                        {Model.sinh_vien_key: new_key},
                        synchronize_session=False,
                    )
                )
                total += n
            except Exception:
                pass
        return total

    # ────────────────────────────────────────────────────────────────
    # FACT LOADERS
    # ────────────────────────────────────────────────────────────────

    def _load_fact_hoc_tap(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("  fact_hoc_tap   → SKIP (empty)")
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
                        sinh_vien_key   = sv_key,
                        hoc_phan_key    = hp_key,
                        giang_vien_key  = gv_key,
                        hoc_ky_key      = hk_key,
                        ma_sinh_vien    = ma_sv,
                        ma_hoc_phan     = ma_hp,
                        ma_dang_ky      = row.get("ma_dang_ky"),
                        diem_chuyen_can = self._to_decimal(row.get("diem_chuyen_can")),
                        diem_bai_tap    = self._to_decimal(row.get("diem_bai_tap")),
                        diem_giua_ky    = self._to_decimal(row.get("diem_giua_ky")),
                        diem_cuoi_ky    = self._to_decimal(row.get("diem_cuoi_ky")),
                        diem_tong_ket   = self._to_decimal(row.get("diem_tong_ket")),
                        diem_chu        = row.get("diem_chu"),
                        diem_he_4       = diem_he4,
                        dat_mon         = row.get("dat_mon"),
                        hoc_lai         = row.get("hoc_lai"),
                        so_tin_chi      = so_tc,
                        diem_chat_luong = diem_cl,
                        nguon_du_lieu   = "postgresql",
                    )
                    session.add(obj)
                    count += 1

                # FIX: Flush chỉ khi count > 0 để tránh flush thừa lúc count=0
                if count > 0 and count % self.batch_size == 0:
                    session.flush()

            session.commit()
            logger.info(f"  fact_hoc_tap   → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_hoc_tap | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_fact_dang_ky(self, df: pd.DataFrame) -> int:
        """
        FIX: Method mới — load fact_dang_ky.
        Trước đây bảng này luôn trống vì không có code load.
        fact_dang_ky lưu TẤT CẢ lượt đăng ký (kể cả chưa thi),
        khác fact_hoc_tap chỉ có bản ghi đã có điểm.
        """
        if df.empty:
            logger.info("  fact_dang_ky   → SKIP (empty)")
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

                existing = session.query(FactDangKy).filter(
                    FactDangKy.ma_sinh_vien == ma_sv,
                    FactDangKy.ma_hoc_phan  == ma_hp,
                    FactDangKy.hoc_ky_key   == hk_key,
                ).first()

                if existing:
                    # Cập nhật trạng thái nếu đã tồn tại
                    existing.trang_thai   = row.get("trang_thai", existing.trang_thai)
                    existing.ngay_dang_ky = self._to_date(row.get("ngay_dang_ky"))
                else:
                    so_tc = self._hp_tc_cache.get(hp_key)
                    obj = FactDangKy(
                        sinh_vien_key  = sv_key,
                        hoc_phan_key   = hp_key,
                        giang_vien_key = gv_key,
                        hoc_ky_key     = hk_key,
                        ma_sinh_vien   = ma_sv,
                        ma_hoc_phan    = ma_hp,
                        ma_dang_ky     = self._to_int(row.get("ma_dang_ky")),
                        trang_thai     = row.get("trang_thai", "Đã đăng ký"),
                        so_tin_chi     = so_tc,
                        ngay_dang_ky   = self._to_date(row.get("ngay_dang_ky")),
                        nguon_du_lieu  = "postgresql",
                    )
                    session.add(obj)
                    count += 1

                if count > 0 and count % self.batch_size == 0:
                    session.flush()

            session.commit()
            logger.info(f"  fact_dang_ky   → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_dang_ky | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_fact_ctsv(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("  fact_ctsv      → SKIP (empty)")
            return 0

        expected_cols = [
            "ma_sinh_vien", "hoc_ky", "diem_ren_luyen",
            "xep_loai_rl", "loai_hoc_bong", "muc_tien_hb",
            "hinh_thuc_ky_luat", "ly_do_ky_luat",
        ]
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            logger.warning(f"  fact_ctsv | Thiếu cột: {missing}")

        session = WarehouseSession()
        count   = 0
        skipped = 0

        try:
            for idx, row in df.iterrows():
                ma_sv = row.get("ma_sinh_vien")
                ma_hk = row.get("hoc_ky")

                sv_key = self._lookup_sv_key(ma_sv)
                hk_key = self._lookup_hk_key(ma_hk)

                if not all([sv_key, hk_key]):
                    continue

                raw_diem_rl   = row.get("diem_ren_luyen")
                raw_muc_tien  = row.get("muc_tien_hb")
                raw_xep_loai  = row.get("xep_loai_rl")
                raw_loai_hb   = row.get("loai_hoc_bong")
                raw_hinh_thuc = row.get("hinh_thuc_ky_luat")
                raw_ly_do     = row.get("ly_do_ky_luat")

                diem_rl_val  = self._to_int(raw_diem_rl)
                muc_tien_val = self._to_int(raw_muc_tien) or 0

                if raw_diem_rl is not None and diem_rl_val is None:
                    try:
                        float(raw_diem_rl)
                    except (ValueError, TypeError):
                        skipped += 1
                        if skipped <= 5:
                            logger.warning(
                                f"  fact_ctsv | Row {idx}: "
                                f"diem_ren_luyen='{raw_diem_rl}' không phải số. Skip."
                            )
                        continue

                xep_loai_rl_val = str(raw_xep_loai)  if raw_xep_loai  and not pd.isna(raw_xep_loai)  else None
                loai_hb_val     = str(raw_loai_hb)    if raw_loai_hb    and not pd.isna(raw_loai_hb)    else None
                hinh_thuc_val   = str(raw_hinh_thuc)  if raw_hinh_thuc  and not pd.isna(raw_hinh_thuc)  else None
                ly_do_val       = str(raw_ly_do)       if raw_ly_do      and not pd.isna(raw_ly_do)      else None

                co_hoc_bong_val = bool(loai_hb_val)
                bi_ky_luat_val  = bool(hinh_thuc_val)

                try:
                    existing = session.query(FactCtsv).filter(
                        FactCtsv.ma_sinh_vien == ma_sv,
                        FactCtsv.hoc_ky_key   == hk_key,
                    ).first()

                    if existing:
                        existing.diem_rl       = diem_rl_val
                        existing.xep_loai_rl   = xep_loai_rl_val
                        existing.loai_hoc_bong = loai_hb_val
                        existing.muc_tien_hb   = muc_tien_val
                        existing.hinh_thuc_kl  = hinh_thuc_val
                        existing.ly_do_kl      = ly_do_val
                        existing.co_hoc_bong   = co_hoc_bong_val
                        existing.bi_ky_luat    = bi_ky_luat_val
                    else:
                        obj = FactCtsv(
                            sinh_vien_key  = sv_key,
                            hoc_ky_key     = hk_key,
                            ma_sinh_vien   = ma_sv,
                            ma_hoc_ky      = ma_hk,
                            diem_rl        = diem_rl_val,
                            xep_loai_rl    = xep_loai_rl_val,
                            loai_hoc_bong  = loai_hb_val,
                            muc_tien_hb    = muc_tien_val,
                            hinh_thuc_kl   = hinh_thuc_val,
                            ly_do_kl       = ly_do_val,
                            co_hoc_bong    = co_hoc_bong_val,
                            bi_ky_luat     = bi_ky_luat_val,
                            nguon_du_lieu  = "csv_ctsv",
                        )
                        session.add(obj)
                        count += 1

                except Exception as row_err:
                    logger.warning(
                        f"  fact_ctsv | Row {idx} lỗi: {row_err} "
                        f"| ma_sv={ma_sv}, diem_rl={raw_diem_rl}"
                    )
                    continue

            session.commit()
            if skipped > 0:
                logger.warning(f"  fact_ctsv | Skipped {skipped} rows (dữ liệu không hợp lệ)")
            logger.info(f"  fact_ctsv      → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_ctsv | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _load_fact_tai_chinh(self, df: pd.DataFrame) -> int:
        if df.empty:
            logger.info("  fact_tai_chinh → SKIP (empty)")
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
                    FactTaiChinh.hoc_ky_key   == hk_key,
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
                        sinh_vien_key     = sv_key,
                        hoc_ky_key        = hk_key,
                        ma_sinh_vien      = ma_sv,
                        ma_hoc_ky         = ma_hk,
                        hoc_phi_phai_dong = self._to_int(row.get("hoc_phi_phai_dong")) or 0,
                        da_dong           = self._to_int(row.get("da_dong")) or 0,
                        con_no            = self._to_int(row.get("con_no")) or 0,
                        duoc_mien_giam    = bool(row.get("duoc_mien_giam", False)),
                        ly_do_mien_giam   = row.get("ly_do_mien_giam"),
                        so_tien_mien_giam = self._to_int(row.get("so_tien_mien_giam")) or 0,
                        ngay_dong_cuoi    = self._to_date(row.get("ngay_dong_cuoi")),
                        nguon_du_lieu     = "api_portal",
                    )
                    session.add(obj)
                    count += 1

            session.commit()
            logger.info(f"  fact_tai_chinh → {count:>6,} records")
            return count

        except Exception as e:
            session.rollback()
            logger.error(f"  fact_tai_chinh | Lỗi: {e}")
            return 0
        finally:
            session.close()

    def _delete_fact_by_hk(self, hk_key: int, ma_hoc_ky: str):
        # FIX: Thêm fact_dang_ky vào danh sách xóa khi load incremental
        tables_and_cols = [
            ("fact_hoc_tap",   "hoc_ky_key"),
            ("fact_dang_ky",   "hoc_ky_key"),
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
                        logger.info(
                            f"  {table} | Xóa {result.rowcount} records cũ (HK: {ma_hoc_ky})"
                        )
                except Exception as e:
                    logger.debug(f"  {table} | Skip delete: {e}")

    # ────────────────────────────────────────────────────────────────
    # TYPE CONVERSION HELPERS
    # ────────────────────────────────────────────────────────────────

    @staticmethod
    def _to_date(val) -> Optional[date]:
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
        if val is None:
            return None
        if isinstance(val, float) and np.isnan(val):
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None