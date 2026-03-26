-- Top 20 attendees by number of result rows.
-- Reuses athlete summary fields for consistency.

SELECT
  ROW_NUMBER() OVER (
    ORDER BY appearances_in_results DESC, athlete_id
  ) AS rank_position,
  athlete_id,
  first_name,
  last_name,
  club_name,
  home_parkrun,
  parkrun_club_membership,
  total_run_count,
  total_vol_count,
  highest_parkrun_club_membership_number,
  highest_volunteer_club_membership_number,
  highest_run_total,
  highest_volunteer_count,
  genuine_pb_count,
  fastest_time,
  appearances_in_results
FROM parkrun_data._06_results_athlete_summary
ORDER BY appearances_in_results DESC, athlete_id
LIMIT 20;
