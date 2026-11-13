-- LAYER: BRONZE
-- PURPOSE: Raw course enrollment data ingestion

CREATE OR REFRESH STREAMING TABLE bronze_course_enrollments

COMMENT 'Bronze layer: Raw course enrollment records with ingestion metadata'

AS

SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS loaded_at

FROM STREAM read_files(
    '/Volumes/online_learning_sdp/default/onlinelearning_volume/',
    format => 'csv',
    header => true,
    inferColumnTypes => false
);

-- sliver clean layer

CREATE OR REFRESH MATERIALIZED VIEW silver_enrollments_clean (

    CONSTRAINT valid_progress
    EXPECT (progress_pct BETWEEN 0 AND 100)
    ON VIOLATION DROP ROW,

    CONSTRAINT valid_price
    EXPECT (price_paid >= 0)
    ON VIOLATION DROP ROW

)

AS

WITH cleaned AS (

    SELECT DISTINCT

        TRIM(enrollment_id) AS enrollment_id,

        NULLIF(TRIM(learner_id), '') AS learner_id,

        LOWER(TRIM(learner_email)) AS learner_email,

        COALESCE(
            NULLIF(INITCAP(TRIM(learner_city)), ''),
            'Unknown'
        ) AS learner_city,

        TRIM(age_group) AS age_group,

        TRIM(course_id) AS course_id,

        TRIM(course_name) AS course_name,

        COALESCE(
            NULLIF(INITCAP(TRIM(category)), ''),
            'Unknown'
        ) AS category,

        INITCAP(TRIM(course_level)) AS course_level,

        TRIM(instructor_name) AS instructor_name,

        TRY_CAST(enrollment_date AS DATE)
            AS enrollment_date,

        INITCAP(TRIM(enrollment_channel))
            AS enrollment_channel,

        TRY_CAST(list_price AS DECIMAL(12,2))
            AS list_price,

        TRY_CAST(discount_amount AS DECIMAL(12,2))
            AS discount_amount,

        COALESCE(
            TRY_CAST(price_paid AS DECIMAL(12,2)),
            TRY_CAST(list_price AS DECIMAL(12,2))
            - TRY_CAST(discount_amount AS DECIMAL(12,2))
        ) AS price_paid,

        TRIM(payment_method) AS payment_method,

        TRY_CAST(progress_pct AS INT)
            AS progress_pct,

        TRY_CAST(hours_watched AS DECIMAL(10,2))
            AS hours_watched,

        TRY_CAST(course_rating AS DECIMAL(4,2))
            AS course_rating,

        INITCAP(TRIM(certificate_issued))
            AS certificate_issued

    FROM online_learning_sdp.default.bronze_course_enrollments

)

SELECT
    *,

    CASE
        WHEN progress_pct = 100 THEN 'Completed'
        WHEN progress_pct > 0 THEN 'In Progress'
        ELSE 'Not Started'
    END AS learner_status

FROM cleaned;

-- sliver rejected layer

CREATE OR REFRESH MATERIALIZED VIEW silver_enrollments_rejected

AS

SELECT
    *,

    CASE
        WHEN TRY_CAST(progress_pct AS INT) < 0
          OR TRY_CAST(progress_pct AS INT) > 100
        THEN 'Progress is outside 0 to 100'

        ELSE 'Price paid is negative'

    END AS reject_reason

FROM online_learning_sdp.default.bronze_course_enrollments

WHERE TRY_CAST(progress_pct AS INT) < 0
   OR TRY_CAST(progress_pct AS INT) > 100
   OR TRY_CAST(price_paid AS DECIMAL(12,2)) < 0;
   
   
-- 1. MONTHLY ENROLLMENTS

CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_enrollments
AS
SELECT
    DATE_TRUNC('MONTH', enrollment_date) AS enrollment_month,
    COUNT(*) AS enrollments,
    COUNT(DISTINCT learner_email) AS unique_learners,
    SUM(price_paid) AS revenue,
    SUM(discount_amount) AS total_discount,
    SUM(
        CASE WHEN learner_status = 'Completed'
        THEN 1 ELSE 0 END
    ) AS completions,
    SUM(hours_watched) AS hours_watched
FROM online_learning_sdp.default.silver_enrollments_clean
GROUP BY DATE_TRUNC('MONTH', enrollment_date);


-- 2. CATEGORY SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_category_summary
AS
SELECT
    category,
    COUNT(*) AS enrollments,
    SUM(price_paid) AS revenue,

    SUM(
        CASE WHEN learner_status = 'Completed'
        THEN 1 ELSE 0 END
    ) AS completed,

    SUM(
        CASE WHEN learner_status = 'In Progress'
        THEN 1 ELSE 0 END
    ) AS in_progress,

    SUM(
        CASE WHEN learner_status = 'Not Started'
        THEN 1 ELSE 0 END
    ) AS not_started,

    ROUND(
        SUM(
            CASE WHEN learner_status = 'Completed'
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*),
        2
    ) AS completion_rate_pct,

    ROUND(AVG(progress_pct), 2) AS avg_progress_pct

FROM online_learning_sdp.default.silver_enrollments_clean
GROUP BY category;


-- 3. COURSE SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_course_summary
AS
SELECT
    category,
    course_name,
    course_level,
    instructor_name,

    COUNT(*) AS enrollments,
    SUM(price_paid) AS revenue,

    ROUND(
        SUM(
            CASE WHEN learner_status = 'Completed'
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*),
        2
    ) AS completion_rate_pct,

    ROUND(AVG(course_rating), 2) AS avg_rating,
    SUM(hours_watched) AS hours_watched

FROM online_learning_sdp.default.silver_enrollments_clean
GROUP BY
    category,
    course_name,
    course_level,
    instructor_name;


-- 4. CHANNEL SUMMARY

CREATE OR REFRESH MATERIALIZED VIEW gold_channel_summary
AS
SELECT
    enrollment_channel,
    COUNT(*) AS enrollments,
    SUM(price_paid) AS revenue,

    ROUND(
        SUM(
            CASE WHEN learner_status = 'Completed'
            THEN 1 ELSE 0 END
        ) * 100.0 / COUNT(*),
        2
    ) AS completion_rate_pct

FROM online_learning_sdp.default.silver_enrollments_clean
GROUP BY enrollment_channel;


-- 5. DATA QUALITY AND RECONCILIATION

CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality
AS
SELECT
    COUNT(*) AS bronze_rows,

    COUNT(*) - COUNT(DISTINCT enrollment_id)
        AS duplicate_rows,

    SUM(
        CASE
            WHEN TRY_CAST(progress_pct AS INT) < 0
              OR TRY_CAST(progress_pct AS INT) > 100
            THEN 1 ELSE 0
        END
    ) AS invalid_progress_rows,

    SUM(
        CASE
            WHEN TRY_CAST(price_paid AS DECIMAL(12,2)) < 0
            THEN 1 ELSE 0
        END
    ) AS negative_price_rows,

    SUM(
        CASE
            WHEN category IS NULL OR TRIM(category) = ''
            THEN 1 ELSE 0
        END
    ) AS missing_category_rows,

    SUM(
        CASE
            WHEN learner_city IS NULL OR TRIM(learner_city) = ''
            THEN 1 ELSE 0
        END
    ) AS missing_city_rows,

    SUM(
        CASE
            WHEN price_paid IS NULL OR TRIM(price_paid) = ''
            THEN 1 ELSE 0
        END
    ) AS missing_price_rows,

    COUNT(DISTINCT learner_email)
        AS learners_before_email_fix,

    COUNT(DISTINCT LOWER(TRIM(learner_email)))
        AS learners_after_email_fix,

    (
        SELECT COUNT(*)
        FROM online_learning_sdp.default.silver_enrollments_clean
    ) AS clean_rows,

    (
        SELECT COUNT(*)
        FROM online_learning_sdp.default.silver_enrollments_rejected
    ) AS rejected_rows,

    CASE
        WHEN COUNT(*) =
            (
                SELECT COUNT(*)
                FROM online_learning_sdp.default.silver_enrollments_clean
            )
            +
            (
                SELECT COUNT(*)
                FROM online_learning_sdp.default.silver_enrollments_rejected
            )
            +
            (
                COUNT(*) - COUNT(DISTINCT enrollment_id)
            )
        THEN 'Yes'
        ELSE 'No'
    END AS reconciliation_passed

FROM online_learning_sdp.default.bronze_course_enrollments;