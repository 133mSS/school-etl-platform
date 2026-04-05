import pandas as pd

def transform_sinh_vien(df_sv, df_nganh, df_khoa):
    df_sv["ho_ten"] = df_sv["ho"].str.strip() + " " + df_sv["ten"].str.strip()

    df = df_sv.merge(df_nganh, on="ma_nganh", how="left")

    df = df.merge(df_khoa,on="ma_khoa",how="left")

    df = df[[
        "ma_sinh_vien",
        "ho_ten",
        "email",
        "khoa_hoc",
        "trang_thai_hoc_tap",
        "ten_nganh",
        "ten_khoa"]]

    df = df.drop_duplicates(subset=["ma_sinh_vien"])

    return df
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
def map_grade(score):
    if pd.isna(score):
        return None,None

    for lower,upper,dc, ds in  GRADE_SCALE:
        if lower <= score < upper:
            return dc,ds

    return "F", 0.0
def transform_diem(df_diem, df_dk):
    df = df_diem.merge(df_dk, on="ma_dang_ky",how="left")

    score_cols = ["diem"]
    df["diem_tong_ket"] = df["diem_tong_ket"].fillna(
        df[["diem_qua_trinh", "diem_thi"]].mean(axis=1))