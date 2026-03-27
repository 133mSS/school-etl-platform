
-- POPULATE DIM_DATE (PTIT SYNCED)

INSERT INTO dim_date (
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_num,
    month_name,
    quarter,
    year,
    is_weekend,
    academic_year,
    academic_term
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                                  AS date_key,
    d                                                            AS full_date,
    EXTRACT(ISODOW FROM d)::INT                                  AS day_of_week,
    
    CASE EXTRACT(ISODOW FROM d)
        WHEN 1 THEN 'Thứ Hai'
        WHEN 2 THEN 'Thứ Ba'
        WHEN 3 THEN 'Thứ Tư'
        WHEN 4 THEN 'Thứ Năm'
        WHEN 5 THEN 'Thứ Sáu'
        WHEN 6 THEN 'Thứ Bảy'
        WHEN 7 THEN 'Chủ Nhật'
    END                                                          AS day_name,
    EXTRACT(DAY FROM d)::INT                                     AS day_of_month,
    EXTRACT(DOY FROM d)::INT                                     AS day_of_year,
    EXTRACT(WEEK FROM d)::INT                                    AS week_of_year,
    EXTRACT(MONTH FROM d)::INT                                   AS month_num,
   
    'Tháng ' || EXTRACT(MONTH FROM d)::TEXT                      AS month_name,
    EXTRACT(QUARTER FROM d)::INT                                 AS quarter,
    EXTRACT(YEAR FROM d)::INT                                    AS year,
    EXTRACT(ISODOW FROM d) IN (6, 7)                             AS is_weekend,

    
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 9
        THEN 'Năm học ' || EXTRACT(YEAR FROM d)::TEXT 
             || '-' || (EXTRACT(YEAR FROM d) + 1)::TEXT
        ELSE 'Năm học ' || (EXTRACT(YEAR FROM d) - 1)::TEXT 
             || '-' || EXTRACT(YEAR FROM d)::TEXT
    END                                                          AS academic_year,

    
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 9 OR EXTRACT(MONTH FROM d) = 1
            THEN 'Học kỳ 1'
        WHEN EXTRACT(MONTH FROM d) BETWEEN 2 AND 6
            THEN 'Học kỳ 2'
        ELSE 'Học kỳ Hè'
    END                                                          AS academic_term

FROM generate_series(
    '2020-01-01'::DATE,
    '2029-12-31'::DATE,
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_key) DO NOTHING;

-- Verify
DO $$
DECLARE
    v_count    INT;
    v_min_date DATE;
    v_max_date DATE;
BEGIN
    SELECT COUNT(*), MIN(full_date), MAX(full_date)
    INTO v_count, v_min_date, v_max_date
    FROM dim_date;

    RAISE NOTICE '========================================';
    RAISE NOTICE '   populate_dim_date.sql (PTIT SYNCED) DONE';
    RAISE NOTICE '   Tổng số dòng : %', v_count;
    RAISE NOTICE '   Thời gian    : % → %', v_min_date, v_max_date;
    RAISE NOTICE '   Trạng thái   : Đồng nhất Tiếng Việt thành công';
    RAISE NOTICE '========================================';
END $$;