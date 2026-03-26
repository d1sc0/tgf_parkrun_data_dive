-- Per-run finish time stats for parkrun_data.results.
-- Returns fastest, slowest, and average finish times per run_id with event_date.

WITH parsed AS (
  SELECT
    run_id,
    event_date,
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
    run_id,
    event_date,
    MIN(finish_seconds) AS fastest_seconds,
    MAX(finish_seconds) AS slowest_seconds,
    AVG(finish_seconds) AS average_seconds
  FROM parsed
  WHERE finish_seconds IS NOT NULL
  GROUP BY run_id, event_date
)
SELECT
  run_id,
  event_date,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(fastest_seconds, 3600) AS INT64),
    CAST(DIV(MOD(fastest_seconds, 3600), 60) AS INT64),
    CAST(MOD(fastest_seconds, 60) AS INT64)
  ) AS fastest_finish_time,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(slowest_seconds, 3600) AS INT64),
    CAST(DIV(MOD(slowest_seconds, 3600), 60) AS INT64),
    CAST(MOD(slowest_seconds, 60) AS INT64)
  ) AS slowest_finish_time,
  FORMAT(
    '%02d:%02d:%02d',
    CAST(DIV(CAST(ROUND(average_seconds) AS INT64), 3600) AS INT64),
    CAST(DIV(MOD(CAST(ROUND(average_seconds) AS INT64), 3600), 60) AS INT64),
    CAST(MOD(CAST(ROUND(average_seconds) AS INT64), 60) AS INT64)
  ) AS average_finish_time
FROM agg
ORDER BY run_id DESC;