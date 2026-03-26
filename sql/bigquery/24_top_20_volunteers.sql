-- Top 20 volunteers by number of volunteer rows.
-- Reuses volunteer athlete-role summary fields for consistency.

SELECT
  ROW_NUMBER() OVER (
    ORDER BY v.appearances_in_volunteers DESC, v.athlete_id
  ) AS rank_position,
  v.athlete_id,
  COALESCE(a.first_name, v.first_name) AS first_name,
  COALESCE(a.last_name, v.last_name) AS last_name,
  a.club_name,
  a.home_parkrun,
  a.parkrun_club_membership,
  a.total_run_count,
  a.total_vol_count,
  COALESCE(a.highest_parkrun_club_membership_number, v.highest_parkrun_club_membership_number) AS highest_parkrun_club_membership_number,
  COALESCE(a.highest_volunteer_club_membership_number, v.highest_volunteer_club_membership_number) AS highest_volunteer_club_membership_number,
  COALESCE(a.highest_run_total, v.highest_run_total) AS highest_run_total,
  COALESCE(a.highest_volunteer_count, v.highest_volunteer_count) AS highest_volunteer_count,
  COALESCE(a.genuine_pb_count, v.genuine_pb_count) AS genuine_pb_count,
  a.fastest_time,
  v.roles_assigned AS volunteer_roles,
  v.appearances_in_volunteers
FROM parkrun_data._08_volunteers_athlete_roles_summary v
LEFT JOIN parkrun_data._06_results_athlete_summary a
  ON a.athlete_id = v.athlete_id
ORDER BY v.appearances_in_volunteers DESC, v.athlete_id
LIMIT 20;
