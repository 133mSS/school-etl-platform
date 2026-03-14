from src.etl.extract import extract_table
from src.etl.transform import (
    transform_sinh_vien, transform_giang_vien,
    transform_diem, calculate_gpa
)
from src.etl.load import run_full_load

# Extract
df_hk   = extract_table("hoc_ky_nam_hoc")
df_gv   = extract_table("giang_vien")
df_hp   = extract_table("hoc_phan")
df_sv   = extract_table("sinh_vien")
df_diem_raw = extract_table("diem_hoc_phan")
df_dk   = extract_table("dang_ky_hoc_phan")

# Transform
df_sv_t  = transform_sinh_vien(df_sv)
df_gv_t  = transform_giang_vien(df_gv)
df_diem  = transform_diem(df_diem_raw, df_dk, df_hp)
df_gpa   = calculate_gpa(df_diem)

# Load
run_full_load(df_hk, df_gv_t, df_hp, df_sv_t, df_diem, df_gpa)