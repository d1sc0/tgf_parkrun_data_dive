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

- 01_results_rows_by_run_id.sql -> v_01_results_rows_by_run_id
- 02_junior_results_rows_by_run_id.sql -> v_02_junior_results_rows_by_run_id
- 03_volunteers_rows_by_run_id.sql -> v_03_volunteers_rows_by_run_id
- 04_junior_volunteers_rows_by_run_id.sql -> v_04_junior_volunteers_rows_by_run_id
- 05_total_rows_all_tables.sql -> v_05_total_rows_all_tables
- 06_results_athlete_summary.sql -> v_06_results_athlete_summary
- 07_junior_results_athlete_summary.sql -> v_07_junior_results_athlete_summary
- 08_volunteers_athlete_roles_summary.sql -> v_08_volunteers_athlete_roles_summary
- 09_junior_volunteers_athlete_roles_summary.sql -> v_09_junior_volunteers_athlete_roles_summary
- 10_duplicate_rows_detailed.sql -> v_10_duplicate_rows_detailed
- 11_duplicate_rows_summary.sql -> v_11_duplicate_rows_summary
- 12_daily_qa_latest_date_rows.sql -> v_12_daily_qa_latest_date_rows
- 13_daily_qa_day_over_day_deltas.sql -> v_13_daily_qa_day_over_day_deltas
- 14_daily_qa_null_rates.sql -> v_14_daily_qa_null_rates
- 15_daily_qa_latest_run_completeness.sql -> v_15_daily_qa_latest_run_completeness
- 16_headline_stats.sql -> v_16_headline_stats

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
