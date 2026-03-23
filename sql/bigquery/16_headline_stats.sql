-- Headline stats across parkrun_data results tables.
-- Returns a single row with all requested metrics.

WITH
parkrun_base AS (
  SELECT
    run_id,
    athlete_id,
    event_date,
    finish_time,
    was_pb,
    club_name,
    home_run_name
  FROM parkrun_data.results
),
junior_base AS (
  SELECT
    run_id,
    athlete_id,
    event_date,
    finish_time,
    was_pb,
    club_name,
    home_run_name
  FROM parkrun_data.junior_results
),
all_results AS (
  SELECT athlete_id, club_name, home_run_name FROM parkrun_base
  UNION ALL
  SELECT athlete_id, club_name, home_run_name FROM junior_base
),
all_volunteers AS (
  SELECT athlete_id FROM parkrun_data.volunteers
  UNION ALL
  SELECT athlete_id FROM parkrun_data.junior_volunteers
),
parkrun_finishers_by_week AS (
  SELECT event_date, COUNT(*) AS finishers
  FROM parkrun_base
  GROUP BY event_date
),
junior_finishers_by_week AS (
  SELECT event_date, COUNT(*) AS finishers
  FROM junior_base
  GROUP BY event_date
),
parkrun_times AS (
  SELECT
    CASE
      WHEN finish_time IS NULL THEN NULL
      WHEN ARRAY_LENGTH(SPLIT(finish_time, ':')) != 3 THEN NULL
      ELSE
        SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(0)] AS INT64) * 3600
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(1)] AS INT64) * 60
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(2)] AS INT64)
    END AS seconds
  FROM parkrun_base
),
junior_times AS (
  SELECT
    CASE
      WHEN finish_time IS NULL THEN NULL
      WHEN ARRAY_LENGTH(SPLIT(finish_time, ':')) != 3 THEN NULL
      ELSE
        SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(0)] AS INT64) * 3600
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(1)] AS INT64) * 60
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(2)] AS INT64)
    END AS seconds
  FROM junior_base
),
parkrun_aggs AS (
  SELECT
    COUNT(*) AS records,
    COUNT(DISTINCT run_id) AS total_events,
    COUNTIF(was_pb = TRUE) AS pb_count
  FROM parkrun_base
),
junior_aggs AS (
  SELECT
    COUNT(*) AS records,
    COUNT(DISTINCT run_id) AS total_events,
    COUNTIF(was_pb = TRUE) AS pb_count
  FROM junior_base
),
parkrun_time_aggs AS (
  SELECT
    MIN(seconds) AS fastest_seconds,
    MAX(seconds) AS slowest_seconds,
    AVG(seconds) AS mean_seconds,
    SUM(seconds) AS total_seconds
  FROM parkrun_times
  WHERE seconds IS NOT NULL
),
junior_time_aggs AS (
  SELECT
    MIN(seconds) AS fastest_seconds,
    MAX(seconds) AS slowest_seconds,
    AVG(seconds) AS mean_seconds,
    SUM(seconds) AS total_seconds
  FROM junior_times
  WHERE seconds IS NOT NULL
),
athlete_aggs AS (
  SELECT
    COUNT(DISTINCT athlete_id) AS unique_athletes,
    COUNT(DISTINCT IFNULL(NULLIF(TRIM(club_name), ''), NULL)) AS distinct_clubs,
    COUNT(DISTINCT IFNULL(NULLIF(TRIM(home_run_name), ''), NULL)) AS distinct_home_parkruns
  FROM all_results
  WHERE athlete_id IS NOT NULL
),
volunteer_aggs AS (
  SELECT COUNT(DISTINCT athlete_id) AS unique_volunteers
  FROM all_volunteers
  WHERE athlete_id IS NOT NULL
),
weekly_aggs AS (
  SELECT
    (SELECT AVG(finishers) FROM parkrun_finishers_by_week) AS avg_parkrun_finishers_per_week,
    (SELECT AVG(finishers) FROM junior_finishers_by_week) AS avg_junior_finishers_per_week
)
SELECT
  -- 1-2
  p.total_events AS total_parkrun_events,
  j.total_events AS total_junior_events,

  -- 3-4
  p.records * 5.0 AS parkrun_total_distance_km,
  j.records * 2.0 AS junior_total_distance_km,

  -- 5-6
  ROUND(SAFE_DIVIDE(p.records * 5.0, 384400.0) * 100, 4) AS parkrun_pct_to_moon,
  ROUND(SAFE_DIVIDE(j.records * 2.0, 384400.0) * 100, 4) AS junior_pct_to_moon,

  -- 7-8
  a.unique_athletes,
  v.unique_volunteers,

  -- 9-10
  ROUND(w.avg_parkrun_finishers_per_week, 2) AS avg_parkrun_finishers_per_week,
  ROUND(w.avg_junior_finishers_per_week, 2) AS avg_junior_finishers_per_week,

  -- 11 parkrun fastest/slowest/mean
  FORMAT('%02d:%02d:%02d',
    CAST(pt.fastest_seconds / 3600 AS INT64),
    CAST(MOD(pt.fastest_seconds, 3600) / 60 AS INT64),
    CAST(MOD(pt.fastest_seconds, 60) AS INT64)
  ) AS parkrun_fastest_time,
  FORMAT('%02d:%02d:%02d',
    CAST(pt.slowest_seconds / 3600 AS INT64),
    CAST(MOD(pt.slowest_seconds, 3600) / 60 AS INT64),
    CAST(MOD(pt.slowest_seconds, 60) AS INT64)
  ) AS parkrun_slowest_time,
  FORMAT('%02d:%02d:%02d',
    CAST(CAST(ROUND(pt.mean_seconds) AS INT64) / 3600 AS INT64),
    CAST(MOD(CAST(ROUND(pt.mean_seconds) AS INT64), 3600) / 60 AS INT64),
    CAST(MOD(CAST(ROUND(pt.mean_seconds) AS INT64), 60) AS INT64)
  ) AS parkrun_mean_time,

  -- 12 junior fastest/slowest/mean
  FORMAT('%02d:%02d:%02d',
    CAST(jt.fastest_seconds / 3600 AS INT64),
    CAST(MOD(jt.fastest_seconds, 3600) / 60 AS INT64),
    CAST(MOD(jt.fastest_seconds, 60) AS INT64)
  ) AS junior_fastest_time,
  FORMAT('%02d:%02d:%02d',
    CAST(jt.slowest_seconds / 3600 AS INT64),
    CAST(MOD(jt.slowest_seconds, 3600) / 60 AS INT64),
    CAST(MOD(jt.slowest_seconds, 60) AS INT64)
  ) AS junior_slowest_time,
  FORMAT('%02d:%02d:%02d',
    CAST(CAST(ROUND(jt.mean_seconds) AS INT64) / 3600 AS INT64),
    CAST(MOD(CAST(ROUND(jt.mean_seconds) AS INT64), 3600) / 60 AS INT64),
    CAST(MOD(CAST(ROUND(jt.mean_seconds) AS INT64), 60) AS INT64)
  ) AS junior_mean_time,

  -- 13-14 (sum of finish times divided by 24h)
  ROUND(SAFE_DIVIDE(pt.total_seconds, 86400.0), 2) AS parkrun_total_finish_time_days,
  ROUND(SAFE_DIVIDE(jt.total_seconds, 86400.0), 2) AS junior_total_finish_time_days,

  -- 15-16
  p.pb_count AS parkrun_pb_count,
  j.pb_count AS junior_pb_count,

  -- 17-18
  a.distinct_clubs,
  a.distinct_home_parkruns
FROM parkrun_aggs p
CROSS JOIN junior_aggs j
CROSS JOIN parkrun_time_aggs pt
CROSS JOIN junior_time_aggs jt
CROSS JOIN athlete_aggs a
CROSS JOIN volunteer_aggs v
CROSS JOIN weekly_aggs w;
