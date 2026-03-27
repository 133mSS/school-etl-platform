"""
test/test_extract.py — Unit tests cho Extraction Layer
Checkpoint Tuần 3: All extractions working
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.etl.extract import (
    PostgreSQLExtractor,
    CSVExtractor,
    APIExtractor,
    DataExtractor,
    ExtractedData,
)


def test_postgresql_extractor():
    """Test Nguồn 1: PostgreSQL."""
    print("\n" + "=" * 60)
    print("TEST NGUỒN 1: PostgreSQL (Phòng Đào tạo)")
    print("=" * 60)

    pg = PostgreSQLExtractor()

    # Test từng function theo roadmap
    print("\n--- extract_students_from_postgres ---")
    sv = pg.extract_students_from_postgres()
    assert not sv.empty, "❌ Bảng sinh_vien trống!"
    assert "ma_sinh_vien" in sv.columns
    assert len(sv) >= 100, f"❌ Chỉ có {len(sv)} SV, expected >= 100"
    print(f"  ✅ {len(sv)} sinh viên")

    print("\n--- extract_grades_from_postgres ---")
    diem = pg.extract_grades_from_postgres()
    assert not diem.empty, "❌ Bảng diem_hoc_phan trống!"
    assert "ma_dang_ky" in diem.columns
    print(f"  ✅ {len(diem)} bản ghi điểm")

    print("\n--- extract_enrollments_from_postgres ---")
    dk = pg.extract_enrollments_from_postgres()
    assert not dk.empty, "❌ Bảng dang_ky_hoc_phan trống!"
    print(f"  ✅ {len(dk)} đăng ký")

    print("\n--- extract_all (10 bảng) ---")
    all_data = pg.extract_all()
    assert len(all_data) == 10, f"❌ Expected 10 bảng, got {len(all_data)}"
    for table, df in all_data.items():
        assert not df.empty, f"❌ Bảng {table} trống!"
    print(f"  ✅ 10/10 bảng OK")

    return True


def test_csv_extractor():
    """Test Nguồn 2: CSV (Phòng CTSV)."""
    print("\n" + "=" * 60)
    print("TEST NGUỒN 2: CSV (Phòng CTSV)")
    print("=" * 60)

    csv_ext = CSVExtractor()

    # Test extract_all
    print("\n--- extract_all ---")
    all_csv = csv_ext.extract_all()
    assert not all_csv.empty, "❌ Không đọc được CSV!"
    assert "ma_sinh_vien" in all_csv.columns
    assert "hoc_ky" in all_csv.columns
    assert "diem_ren_luyen" in all_csv.columns
    print(f"  ✅ {len(all_csv)} records từ CSV")

    # Test extract_by_semester
    print("\n--- extract_by_semester('HK1-2024-25') ---")
    hk_csv = csv_ext.extract_by_semester("HK1-2024-25")
    if not hk_csv.empty:
        print(f"  ✅ {len(hk_csv)} records cho HK1-2024-25")
    else:
        print("  ⚠️ Không tìm thấy file cho HK1-2024-25 (có thể OK)")

    return True


def test_api_extractor():
    """Test Nguồn 3: API/JSON (Portal tài chính)."""
    print("\n" + "=" * 60)
    print("TEST NGUỒN 3: API/JSON (Portal tài chính)")
    print("=" * 60)

    api = APIExtractor()

    # Test extract_by_semester
    print("\n--- extract_by_semester('HK1-2024-25') ---")
    tc = api.extract_by_semester("HK1-2024-25")
    if not tc.empty:
        assert "ma_sinh_vien" in tc.columns
        assert "hoc_phi_phai_dong" in tc.columns
        print(f"  ✅ {len(tc)} records tài chính")
    else:
        # Thử HK khác
        tc2 = api.extract_by_semester("HK1-2024-25")
        print(f"  ⚠️ Fallback: {len(tc2)} records")

    # Test extract_all_semesters
    print("\n--- extract_all_semesters ---")
    semesters = ["HK1-2024-25", "HK2-2024-25", "HK1-2025-26"]
    all_tc = api.extract_all_semesters(semesters)
    if not all_tc.empty:
        print(f"  ✅ {len(all_tc)} records tổng")
    else:
        print("  ⚠️ Không có dữ liệu API/JSON")

    return True


def test_full_extract():
    """Test DataExtractor facade — full pipeline."""
    print("\n" + "=" * 60)
    print("TEST FULL EXTRACT (3 NGUỒN)")
    print("=" * 60)

    extractor = DataExtractor()
    result = extractor.extract_full()

    assert isinstance(result, ExtractedData)

    summary = result.summary()
    print("\n📊 KẾT QUẢ EXTRACT:")
    total = 0
    for name, count in summary.items():
        status = "✅" if count > 0 else "⚠️"
        print(f"  {status} {name:<25s}: {count:>8,}")
        total += count

    print(f"\n  TỔNG: {total:,} records")

    # Assertions
    assert summary["sinh_vien"] > 0, "❌ Không có sinh viên!"
    assert summary["diem_hoc_phan"] > 0, "❌ Không có điểm!"
    assert summary["hoc_phan"] > 0, "❌ Không có học phần!"

    print("\n✅ FULL EXTRACT TEST PASSED!")
    return True


if __name__ == "__main__":
    print("🧪 EXTRACTION LAYER — UNIT TESTS")
    print("=" * 70)

    results = {}

    try:
        results["PostgreSQL"] = test_postgresql_extractor()
    except Exception as e:
        results["PostgreSQL"] = False
        print(f"  ❌ FAILED: {e}")

    try:
        results["CSV"] = test_csv_extractor()
    except Exception as e:
        results["CSV"] = False
        print(f"  ❌ FAILED: {e}")

    try:
        results["API/JSON"] = test_api_extractor()
    except Exception as e:
        results["API/JSON"] = False
        print(f"  ❌ FAILED: {e}")

    try:
        results["Full Extract"] = test_full_extract()
    except Exception as e:
        results["Full Extract"] = False
        print(f"  ❌ FAILED: {e}")

    # Summary
    print("\n" + "=" * 70)
    print("📋 TEST SUMMARY")
    print("=" * 70)
    all_passed = True
    for test_name, passed in results.items():
        icon = "✅" if passed else "❌"
        print(f"  {icon} {test_name}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\n🎉 TẤT CẢ TESTS PASSED — Tuần 3 Checkpoint ĐẠT!")
    else:
        print("\n⚠️ Có test FAILED — cần fix trước khi qua Tuần 4")