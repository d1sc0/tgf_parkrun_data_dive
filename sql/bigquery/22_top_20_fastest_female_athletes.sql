-- Top 20 fastest female athletes based on best recorded finish_time.
-- Includes fields aligned with athlete summary output.

WITH base AS (
  SELECT
    athlete_id,
    first_name,
    last_name,
    club_name,
    home_run_name,
    parkrun_club_membership,
    volunteer_club_membership,
    run_total,
    vol_count,
    was_genuine_pb,
    finish_time,
    age_category,
    updated
  FROM parkrun_data.results
  WHERE athlete_id IS NOT NULL
    AND REGEXP_CONTAINS(UPPER(COALESCE(age_category, '')), r'^[VJ]W')
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
    athlete_id,
    COUNT(*) AS appearances_in_results,
    MIN(finish_seconds) AS fastest_seconds,
    COUNTIF(was_genuine_pb = TRUE) AS genuine_pb_count,
    MAX(parkrun_club_membership) AS highest_parkrun_club_membership_number,
    MAX(volunteer_club_membership) AS highest_volunteer_club_membership_number,
    MAX(run_total) AS highest_run_total,
    MAX(vol_count) AS highest_volunteer_count,
    ARRAY_AGG(
      STRUCT(
        first_name,
        last_name,
        club_name,
        home_run_name,
        parkrun_club_membership,
        run_total,
        vol_count
      )
      ORDER BY updated DESC NULLS LAST
      LIMIT 1
    )[OFFSET(0)] AS latest_profile
  FROM scored
  GROUP BY athlete_id
),
ranked AS (
  SELECT
    athlete_id,
    latest_profile.first_name AS first_name,
    latest_profile.last_name AS last_name,
    latest_profile.club_name AS club_name,
    latest_profile.home_run_name AS home_parkrun,
    latest_profile.parkrun_club_membership AS parkrun_club_membership,
    latest_profile.run_total AS total_run_count,
    latest_profile.vol_count AS total_vol_count,
    highest_parkrun_club_membership_number,
    highest_volunteer_club_membership_number,
    highest_run_total,
    highest_volunteer_count,
    genuine_pb_count,
    FORMAT(
      '%02d:%02d:%02d',
      CAST(DIV(fastest_seconds, 3600) AS INT64),
      CAST(DIV(MOD(fastest_seconds, 3600), 60) AS INT64),
      CAST(MOD(fastest_seconds, 60) AS INT64)
    ) AS fastest_time,
    appearances_in_results,
    ROW_NUMBER() OVER (ORDER BY fastest_seconds ASC, athlete_id) AS rank_position
  FROM agg
  WHERE fastest_seconds IS NOT NULL
)
SELECT *
FROM ranked
WHERE rank_position <= 20
ORDER BY rank_position;
