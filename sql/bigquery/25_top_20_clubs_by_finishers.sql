-- Top 20 clubs by number of result rows.
-- Includes summary-style metrics analogous to athlete summaries.

WITH base AS (
  SELECT
    club_name,
    athlete_id,
    first_name,
    last_name,
    home_run_name,
    parkrun_club_membership,
    volunteer_club_membership,
    run_total,
    vol_count,
    was_genuine_pb,
    finish_time,
    updated,
    event_date
  FROM parkrun_data.results
  WHERE COALESCE(TRIM(club_name), '') <> ''
),
scored AS (
  SELECT
    *,
    CASE
      WHEN finish_time IS NULL THEN NULL
      WHEN ARRAY_LENGTH(SPLIT(finish_time, ':')) != 3 THEN NULL
      ELSE
        SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(0)] AS INT64) * 3600
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(1)] AS INT64) * 60
        + SAFE_CAST(SPLIT(finish_time, ':')[OFFSET(2)] AS INT64)
    END AS finish_seconds
  FROM base
),
agg AS (
  SELECT
    club_name,
    COUNT(*) AS finishers_count,
    COUNT(DISTINCT athlete_id) AS unique_athletes,
    MIN(finish_seconds) AS fastest_seconds,
    AVG(finish_seconds) AS average_seconds,
    MAX(finish_seconds) AS slowest_seconds,
    COUNTIF(was_genuine_pb = TRUE) AS genuine_pb_count,
    MAX(parkrun_club_membership) AS highest_parkrun_club_membership_number,
    MAX(volunteer_club_membership) AS highest_volunteer_club_membership_number,
    MAX(run_total) AS highest_run_total,
    MAX(vol_count) AS highest_volunteer_count,
    MAX(event_date) AS latest_event_date,
    ARRAY_AGG(
      STRUCT(first_name, last_name, home_run_name)
      ORDER BY updated DESC NULLS LAST
      LIMIT 1
    )[OFFSET(0)] AS latest_profile_sample
  FROM scored
  GROUP BY club_name
),
ranked AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY finishers_count DESC, club_name) AS rank_position,
    club_name,
    finishers_count,
    unique_athletes,
    FORMAT(
      '%02d:%02d:%02d',
      CAST(DIV(fastest_seconds, 3600) AS INT64),
      CAST(DIV(MOD(fastest_seconds, 3600), 60) AS INT64),
      CAST(MOD(fastest_seconds, 60) AS INT64)
    ) AS fastest_time,
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
    ) AS slowest_finish_time,
    genuine_pb_count,
    highest_parkrun_club_membership_number,
    highest_volunteer_club_membership_number,
    highest_run_total,
    highest_volunteer_count,
    latest_event_date,
    latest_profile_sample.first_name AS latest_first_name,
    latest_profile_sample.last_name AS latest_last_name,
    latest_profile_sample.home_run_name AS latest_home_parkrun
  FROM agg
)
SELECT *
FROM ranked
WHERE rank_position <= 20
ORDER BY rank_position;
