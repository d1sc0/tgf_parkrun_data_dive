WITH base AS (
  SELECT
    athlete_id,
    first_name,
    last_name,
    club_name,
    home_run_name,
    parkrun_club_membership,
    run_total,
    vol_count,
    finish_time,
    updated
  FROM parkrun_data.results
  WHERE athlete_id IS NOT NULL
),
parsed AS (
  SELECT
    *,
    SPLIT(finish_time, ':') AS time_parts
  FROM base
),
scored AS (
  SELECT
    *,
    CASE
      WHEN ARRAY_LENGTH(time_parts) = 3 THEN
        SAFE_CAST(time_parts[OFFSET(0)] AS INT64) * 3600
        + SAFE_CAST(time_parts[OFFSET(1)] AS INT64) * 60
        + SAFE_CAST(time_parts[OFFSET(2)] AS INT64)
      ELSE NULL
    END AS finish_seconds
  FROM parsed
),
agg AS (
  SELECT
    athlete_id,
    COUNT(*) AS appearances_in_results,
    MIN(finish_seconds) AS fastest_seconds,
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
)
SELECT
  athlete_id,
  latest_profile.first_name AS first_name,
  latest_profile.last_name AS last_name,
  latest_profile.club_name AS club_name,
  latest_profile.home_run_name AS home_parkrun,
  latest_profile.parkrun_club_membership AS parkrun_club_membership,
  latest_profile.run_total AS total_run_count,
  latest_profile.vol_count AS total_vol_count,
  CASE
    WHEN fastest_seconds IS NULL THEN NULL
    ELSE FORMAT(
      '%02d:%02d:%02d',
      CAST(DIV(fastest_seconds, 3600) AS INT64),
      CAST(DIV(MOD(fastest_seconds, 3600), 60) AS INT64),
      CAST(MOD(fastest_seconds, 60) AS INT64)
    )
  END AS fastest_time,
  appearances_in_results
FROM agg
ORDER BY appearances_in_results DESC, athlete_id;
