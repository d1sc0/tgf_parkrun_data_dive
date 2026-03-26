BigQuery query pack

This folder contains reusable SQL queries for row counts, athlete summaries, volunteer summaries, duplicate checks, and daily QA.

Also included:

- 16_headline_stats.sql: one-row headline metrics dashboard query.

Publish all SQL files in this folder as BigQuery views:

`npm run publish:views`

Optional environment override for view destination dataset:

- BIGQUERY_VIEWS_DATASET_ID (defaults to BIGQUERY_DATASET_ID)

Generated view names

When you run npm run publish:views, each SQL file is published as a view using this mapping:

- 01_results_rows_by_run_id.sql -> \_01_results_rows_by_run_id
- 02_junior_results_rows_by_run_id.sql -> \_02_junior_results_rows_by_run_id
- 03_volunteers_rows_by_run_id.sql -> \_03_volunteers_rows_by_run_id
- 04_junior_volunteers_rows_by_run_id.sql -> \_04_junior_volunteers_rows_by_run_id
- 05_total_rows_all_tables.sql -> \_05_total_rows_all_tables
- 06_results_athlete_summary.sql -> \_06_results_athlete_summary
- 07_junior_results_athlete_summary.sql -> \_07_junior_results_athlete_summary
- 08_volunteers_athlete_roles_summary.sql -> \_08_volunteers_athlete_roles_summary
- 09_junior_volunteers_athlete_roles_summary.sql -> \_09_junior_volunteers_athlete_roles_summary
- 10_duplicate_rows_detailed.sql -> \_10_duplicate_rows_detailed
- 11_duplicate_rows_summary.sql -> \_11_duplicate_rows_summary
- 12_daily_qa_latest_date_rows.sql -> \_12_daily_qa_latest_date_rows
- 13_daily_qa_day_over_day_deltas.sql -> \_13_daily_qa_day_over_day_deltas
- 14_daily_qa_null_rates.sql -> \_14_daily_qa_null_rates
- 15_daily_qa_latest_run_completeness.sql -> \_15_daily_qa_latest_run_completeness
- 16_headline_stats.sql -> \_16_headline_stats
- 17_missing_positions.sql -> \_17_missing_positions
- 18_run_time_stats_by_run_id.sql -> \_18_run_time_stats_by_run_id
- 19_attendance_by_run_id.sql -> \_19_attendance_by_run_id
- 20_finish_time_stats_by_age_range_gender.sql -> \_20_finish_time_stats_by_age_range_gender
- 21_top_20_fastest_male_athletes.sql -> \_21_top_20_fastest_male_athletes
- 22_top_20_fastest_female_athletes.sql -> \_22_top_20_fastest_female_athletes
- 23_top_20_attendees.sql -> \_23_top_20_attendees
- 24_top_20_volunteers.sql -> \_24_top_20_volunteers
- 25_top_20_clubs_by_finishers.sql -> \_25_top_20_clubs_by_finishers
- 26_top_20_home_parkruns_by_finishers.sql -> \_26_top_20_home_parkruns_by_finishers

Current summary metrics additions:

- 06/07 athlete summary views include:
  - highest_parkrun_club_membership_number
  - highest_volunteer_club_membership_number
  - highest_run_total
  - highest_volunteer_count
  - genuine_pb_count

- 08/09 volunteer athlete summary views include the same highest metrics and genuine_pb_count joined by athlete_id from results/junior_results.

- 16 headline stats view includes:
  - parkrun_pb_count, junior_pb_count
  - parkrun_genuine_pb_count, junior_genuine_pb_count

Recommended run order:

1. 12_daily_qa_latest_date_rows.sql
2. 13_daily_qa_day_over_day_deltas.sql
3. 14_daily_qa_null_rates.sql
4. 11_duplicate_rows_summary.sql
5. 10_duplicate_rows_detailed.sql (only if summary shows duplicates)

Parameter notes:

- 15_daily_qa_latest_run_completeness.sql requires:
  - @target_event_number (INT64)

Table assumptions:

- parkrun_data.results
- parkrun_data.junior_results
- parkrun_data.volunteers
- parkrun_data.junior_volunteers

If your dataset differs, update table references in each query file.
