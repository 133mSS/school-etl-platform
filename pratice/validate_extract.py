import pandas as pd
import numpy as np
from pandas import DataFrame

GRADE_SCALE = [
    (9.0, 10.01, "A+", 4.0),
    (8.5, 9.0,   "A",  3.7),
    (8.0, 8.5,   "B+", 3.5),
    (7.0, 8.0,   "B",  3.0),
    (6.5, 7.0,   "C+", 2.5),
    (5.5, 6.5,   "C",  2.0),
    (5.0, 5.5,   "D+", 1.5),
    (4.0, 5.0,   "D",  1.0),
    (0.0, 4.0,   "F",  0.0),
]
df_diem_raw = pd.DataFrame({
    "ma_dang_ky":      [1, 2, 3, 4, 5],
    "diem_chuyen_can": [9.0, 8.0, None, 7.0, 6.0],
    "diem_bai_tap":    [8.5, 7.5, 8.0,  None, 5.0],
    "diem_giua_ky":    [7.0, 6.5, 7.5,  6.0, 4.5],
    "diem_cuoi_ky":    [8.0, 7.0, 6.5,  5.5, 3.5],
    "diem_tong_ket":   [None, None, None, None, None],
})

def tinh_diem_tong_ket(diemdf: pd.DataFrame):
    score_cols =["diem_chuyen_can","diem_bai_tap","diem_giua_ky","diem_cuoi_ky"]
    mask = df_diem_raw[score_cols].notna().all(axis=1)

    df_diem_raw.loc[mask, "diem_tong_ket"] = (
        df_diem_raw.loc[mask, "diem_chuyen_can"] * 0.1
        + df_diem_raw.loc[mask, "diem_bai_tap"] * 0.1
        + df_diem_raw.loc[mask, "diem_giua_ky"] * 0.2
        + df_diem_raw.loc[mask, "diem_cuoi_ky"] * 0.6).round(2)
    return mask
def map_grade(score):
    if pd.isna(score):
        return pd.Series([None,None])

    for lower, upper, letter, gpa4 in GRADE_SCALE:
        if lower <= score < upper:
            return pd.Series([letter, gpa4])

    return pd.Series(["F",0.0])
mask = tinh_diem_tong_ket(df_diem_raw)
df_diem_raw[["diem_chu", "diem_he_4"]] = (
    df_diem_raw["diem_tong_ket"].apply(map_grade)
)


df_diem_raw["dat_mon"] =( df_diem_raw["diem_tong_ket"] >= 4.0).astype("boolean")

df_diem_raw.loc[df_diem_raw["diem_tong_ket"].isna(),"dat_mon"] = None
so_du_diem = mask.sum()

so_dat = (df_diem_raw["dat_mon"] == True).sum()
so_khong_dat = (df_diem_raw["dat_mon"] == False).sum()

so_null = df_diem_raw["diem_tong_ket"].isna().sum()

print(df_diem_raw[[
    "ma_dang_ky",
    "diem_tong_ket",
    "diem_chu",
    "diem_he_4",
    "dat_mon"
]])

print("\nThống kê:")
print("Đủ điểm:", so_du_diem)
print("Đạt:", so_dat)
print("Không đạt:", so_khong_dat)
print("Thiếu điểm:", so_null)