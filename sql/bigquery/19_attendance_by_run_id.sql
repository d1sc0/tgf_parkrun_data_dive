-- Attendance summary per run_id for parkrun_data.results.
-- Includes event_date, total finishers, gender totals, and age-category breakdowns.

WITH base AS (
  SELECT
    run_id,
    event_date,
    age_category,
    CASE
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(age_category, '')), r'^[VJ]M') THEN 'male'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(age_category, '')), r'^[VJ]W') THEN 'female'
      ELSE 'unknown'
    END AS gender_bucket
  FROM parkrun_data.results
  WHERE run_id IS NOT NULL
),
run_totals AS (
  SELECT
    run_id,
    event_date,
    COUNT(*) AS total_finishers,
    COUNTIF(gender_bucket = 'male') AS male_finishers,
    COUNTIF(gender_bucket = 'female') AS female_finishers,
    COUNTIF(gender_bucket = 'unknown') AS unknown_gender_finishers
  FROM base
  GROUP BY run_id, event_date
),
age_counts AS (
  SELECT
    run_id,
    event_date,
    COALESCE(NULLIF(TRIM(age_category), ''), 'UNKNOWN') AS age_category,
    COUNT(*) AS finisher_count
  FROM base
  GROUP BY run_id, event_date, age_category
),
age_gender_counts AS (
  SELECT
    run_id,
    event_date,
    COALESCE(NULLIF(TRIM(age_category), ''), 'UNKNOWN') AS age_category,
    gender_bucket,
    COUNT(*) AS finisher_count
  FROM base
  GROUP BY run_id, event_date, age_category, gender_bucket
),
age_counts_struct AS (
  SELECT
    run_id,
    event_date,
    ARRAY_AGG(
      STRUCT(age_category, finisher_count)
      ORDER BY age_category
    ) AS finishers_by_age_category
  FROM age_counts
  GROUP BY run_id, event_date
),
age_gender_struct AS (
  SELECT
    run_id,
    event_date,
    ARRAY_AGG(
      STRUCT(age_category, gender_bucket, finisher_count)
      ORDER BY age_category, gender_bucket
    ) AS finishers_by_age_category_and_gender
  FROM age_gender_counts
  GROUP BY run_id, event_date
)
SELECT
  r.run_id,
  r.event_date,
  r.total_finishers,
  r.male_finishers,
  r.female_finishers,
  r.unknown_gender_finishers,
  a.finishers_by_age_category,
  g.finishers_by_age_category_and_gender
FROM run_totals r
LEFT JOIN age_counts_struct a
  USING (run_id, event_date)
LEFT JOIN age_gender_struct g
  USING (run_id, event_date)
ORDER BY r.run_id DESC;