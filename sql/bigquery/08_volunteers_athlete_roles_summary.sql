WITH base AS (
  SELECT
    athlete_id,
    first_name,
    last_name,
    event_date,
    task_name
  FROM parkrun_data.volunteers
  WHERE athlete_id IS NOT NULL
),
profile AS (
  SELECT
    athlete_id,
    ARRAY_AGG(
      STRUCT(first_name, last_name)
      ORDER BY event_date DESC NULLS LAST
      LIMIT 1
    )[OFFSET(0)] AS latest_name,
    COUNT(*) AS appearances_in_volunteers
  FROM base
  GROUP BY athlete_id
),
roles AS (
  SELECT
    athlete_id,
    STRING_AGG(DISTINCT TRIM(role), ', ' ORDER BY TRIM(role)) AS roles_assigned
  FROM base,
  UNNEST(SPLIT(COALESCE(task_name, ''), ',')) AS role
  WHERE TRIM(role) <> ''
  GROUP BY athlete_id
),
athlete_metrics AS (
  SELECT
    athlete_id,
    MAX(parkrun_club_membership) AS highest_parkrun_club_membership_number,
    MAX(volunteer_club_membership) AS highest_volunteer_club_membership_number,
    MAX(run_total) AS highest_run_total,
    MAX(vol_count) AS highest_volunteer_count,
    COUNTIF(was_genuine_pb = TRUE) AS genuine_pb_count
  FROM parkrun_data.results
  WHERE athlete_id IS NOT NULL
  GROUP BY athlete_id
)
SELECT
  p.athlete_id,
  p.latest_name.first_name AS first_name,
  p.latest_name.last_name AS last_name,
  p.appearances_in_volunteers,
  COALESCE(r.roles_assigned, 'No role recorded') AS roles_assigned,
  m.highest_parkrun_club_membership_number,
  m.highest_volunteer_club_membership_number,
  m.highest_run_total,
  m.highest_volunteer_count,
  m.genuine_pb_count
FROM profile p
LEFT JOIN roles r USING (athlete_id)
LEFT JOIN athlete_metrics m USING (athlete_id)
ORDER BY p.appearances_in_volunteers DESC, p.athlete_id;
