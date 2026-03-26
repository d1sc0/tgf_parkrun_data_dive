-- Finish-time stats by age range and gender for parkrun_data.results.
-- Groups by derived gender bucket and normalized age range (e.g. 45-49, 11-14).

WITH parsed AS (
  SELECT
    age_category,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(age_category, '')), r'^[VJ]M') THEN 'male'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(age_category, '')), r'^[VJ]W') THEN 'female'
      ELSE 'unknown'
    END AS gender_bucket,
    CASE
      WHEN REGEXP_CONTAINS(COALESCE(age_category, ''), r'\d{2}-\d{2}')
        THEN REGEXP_EXTRACT(age_category, r'(\d{2}-\d{2})')
      ELSE 'UNKNOWN'
    END AS age_range,
    CASE
      WHEN finish_time IS NULL THEN NULL
      WHEN ARRAY_LENGTH(SPLIT(finish_time, ':')) != 3 THEN NULL
      ELSE
        SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(0)] AS INT64) * 3600
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(1)] AS INT64) * 60
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(2)] AS INT64)
    END AS finish_seconds
  FROM parkrun_data.results
),
agg AS (
  SELECT
    gender_bucket,
    age_range,
    COUNT(*) AS finisher_count,
    MIN(finish_seconds) AS fastest_seconds,
    MAX(finish_seconds) AS slowest_seconds,
    AVG(finish_seconds) AS average_seconds
  FROM parsed
  WHERE finish_seconds IS NOT NULL
  GROUP BY gender_bucket, age_range
)
SELECT
  gender_bucket,
  age_range,
  finisher_count,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(fastest_seconds, 3600) AS INT64),
    CAST(DIV(MOD(fastest_seconds, 3600), 60) AS INT64),
    CAST(MOD(fastest_seconds, 60) AS INT64)
  ) AS fastest_finish_time,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(CAST(ROUND(average_seconds) AS INT64), 3600) AS INT64),
    CAST(DIV(MOD(CAST(ROUND(average_seconds) AS INT64), 3600), 60) AS INT64),
    CAST(MOD(CAST(ROUND(average_seconds) AS INT64), 60) AS INT64)
  ) AS average_finish_time,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(slowest_seconds, 3600) AS INT64),
    CAST(DIV(MOD(slowest_seconds, 3600), 60) AS INT64),
    CAST(MOD(slowest_seconds, 60) AS INT64)
  ) AS slowest_finish_time
FROM agg
ORDER BY
  CASE gender_bucket
    WHEN 'male' THEN 1
    WHEN 'female' THEN 2
    ELSE 3
  END,
  age_range;