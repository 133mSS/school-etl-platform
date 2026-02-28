-- =============================================
-- POPULATE DIM_DATE
-- Range: 2020-01-01 → 2029-12-31 (~3652 rows)
-- Chạy SAU create_dimensions.sql
-- Alphabet order: populate_ > create_ → chạy CUỐI CÙNG ✅
-- User: warehouse_user | DB: school_warehouse | Port: 5435
-- =============================================

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
    TO_CHAR(d, 'YYYYMMDD')::INT                              AS date_key,
    d                                                         AS full_date,
    EXTRACT(ISODOW FROM d)::INT                               AS day_of_week,
    TRIM(TO_CHAR(d, 'Day'))                                   AS day_name,
    EXTRACT(DAY FROM d)::INT                                  AS day_of_month,
    EXTRACT(DOY FROM d)::INT                                  AS day_of_year,
    EXTRACT(WEEK FROM d)::INT                                 AS week_of_year,
    EXTRACT(MONTH FROM d)::INT                                AS month_num,
    TRIM(TO_CHAR(d, 'Month'))                                 AS month_name,
    EXTRACT(QUARTER FROM d)::INT                              AS quarter,
    EXTRACT(YEAR FROM d)::INT                                 AS year,
    EXTRACT(ISODOW FROM d) IN (6, 7)                          AS is_weekend,

    -- Academic year: Sep Y → Aug Y+1
    CASE
        WHEN EXTRACT(MONTH FROM d) >= 9
        THEN EXTRACT(YEAR FROM d)::TEXT
             || '-' || (EXTRACT(YEAR FROM d) + 1)::TEXT
        ELSE (EXTRACT(YEAR FROM d) - 1)::TEXT
             || '-' || EXTRACT(YEAR FROM d)::TEXT
    END                                                       AS academic_year,

    -- Academic term:
    --   Tháng 9-12 → HK1 | Tháng 1-3  → HK2
    --   Tháng 4-6  → HK3 | Tháng 7-8  → Hè
    CASE
        WHEN EXTRACT(MONTH FROM d) BETWEEN 9 AND 12
            THEN 'HK1 ' || EXTRACT(YEAR FROM d)::TEXT
                         || '-' || (EXTRACT(YEAR FROM d) + 1)::TEXT
        WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 3
            THEN 'HK2 ' || (EXTRACT(YEAR FROM d) - 1)::TEXT
                         || '-' || EXTRACT(YEAR FROM d)::TEXT
        WHEN EXTRACT(MONTH FROM d) BETWEEN 4 AND 6
            THEN 'HK3 ' || (EXTRACT(YEAR FROM d) - 1)::TEXT
                         || '-' || EXTRACT(YEAR FROM d)::TEXT
        ELSE 'He '   || (EXTRACT(YEAR FROM d) - 1)::TEXT
                     || '-' || EXTRACT(YEAR FROM d)::TEXT
    END                                                       AS academic_term

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
    RAISE NOTICE '✅ populate_dim_date.sql DONE';
    RAISE NOTICE '   Rows  : %', v_count;
    RAISE NOTICE '   Range : % → %', v_min_date, v_max_date;
    RAISE NOTICE '========================================';
END $$;