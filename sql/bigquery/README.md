BigQuery query pack

This folder contains reusable SQL queries for row counts, athlete summaries, volunteer summaries, duplicate checks, and daily QA. It also includes 9 optimized dashboard views that power the Astro SSR components.

**Dashboard Views (20-28):**
Each Astro dashboard component queries a corresponding view to keep complex SQL logic server-side and component code focused on formatting/filtering:

- 20_dashboard_headline_stats.sql: Aggregated metrics for the HeadlineStats widget.
- 21_dashboard_course_records.sql: Record tables for overall, gender, and age-category course records.
- 22_dashboard_visitor_stats.sql: Home-run visitor summary used by the visitor map and related widgets.
- 23_dashboard_volunteer_milestones.sql: Volunteer milestone tracker data with next-target calculations.
- 24_dashboard_attendance_tracker.sql: Attendance breakdown by event, gender, and age grouping.
- 25_dashboard_performance_tracker.sql: Per-event performance metrics by gender and age grouping.
- 26_dashboard_run_report.sql: Per-run report view with current metrics, previous-run comparisons, and nested detail arrays.
- 27_dashboard_top_lists.sql: Nested top-list datasets matching the TopLists dashboard component.
- 28_dashboard_volunteer_tracker.sql: Volunteer credits, roles, and finisher totals by event.

Publish all SQL files in this folder as BigQuery views:

`npm run publish:views`

Optional environment override for view destination dataset:

- BIGQUERY_VIEWS_DATASET_ID (defaults to BIGQUERY_DATASET_ID)

## Architecture: View-Based Components

Dashboard components follow a **separation of concerns** pattern:

1. **Views handle business logic:** Complex aggregations, rankings, time transformations, and struct/array nesting are precomputed in SQL.
2. **Components handle presentation:** Astro components query views with simple `SELECT *` statements, then format/filter results for the UI.
3. **Benefits:**
   - Views are reusable across multiple components and downstream tools.
   - Component code stays lean and focused on rendering logic.
   - SQL logic is version-controlled and can be tested independently.
   - Changes to aggregation logic don't require component redeployment.

Example (VolunteerMilestones component):

```typescript
// Simple query—all complexity is in the view
const milestoneQuery = `
  SELECT * FROM \`${projectId}.${viewsDatasetId}._23_dashboard_volunteer_milestones\`
  ORDER BY last_vol_date DESC, remaining ASC
`;
const volunteers = await runQuery(milestoneQuery);
// Component then sorts/filters/renders the results
```

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
- 17_missing_positions.sql -> \_17_missing_positions
- 18_run_time_stats_by_run_id.sql -> \_18_run_time_stats_by_run_id
- 19_attendance_by_run_id.sql -> \_19_attendance_by_run_id
- 20_dashboard_headline_stats.sql -> \_20_dashboard_headline_stats
- 21_dashboard_course_records.sql -> \_21_dashboard_course_records
- 22_dashboard_visitor_stats.sql -> \_22_dashboard_visitor_stats
- 23_dashboard_volunteer_milestones.sql -> \_23_dashboard_volunteer_milestones
- 24_dashboard_attendance_tracker.sql -> \_24_dashboard_attendance_tracker
- 25_dashboard_performance_tracker.sql -> \_25_dashboard_performance_tracker
- 26_dashboard_run_report.sql -> \_26_dashboard_run_report
- 27_dashboard_top_lists.sql -> \_27_dashboard_top_lists
- 28_dashboard_volunteer_tracker.sql -> \_28_dashboard_volunteer_tracker

Current summary metrics additions:

- 06/07 athlete summary views include:
  - highest_parkrun_club_membership_number
  - highest_volunteer_club_membership_number
  - highest_run_total
  - highest_volunteer_count
  - genuine_pb_count

- 08/09 volunteer athlete summary views include the same highest metrics and genuine_pb_count joined by athlete_id from results/junior_results.

- 20 dashboard headline stats view includes:
  - total events, finishers, and distance
  - journey to the moon progress calculation
  - unique athletes and volunteers
  - fastest, slowest, and mean finish times

Recommended run order for quality checks:

1. 11_duplicate_rows_summary.sql
2. 10_duplicate_rows_detailed.sql (only if summary shows duplicates)
3. 17_missing_positions.sql
4. 05_total_rows_all_tables.sql

Table assumptions:

- parkrun_data.results
- parkrun_data.junior_results
- parkrun_data.volunteers
- parkrun_data.junior_volunteers

If your dataset differs, update table references in each query file.
