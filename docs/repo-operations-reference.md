# Repo Operations Reference

Purpose: practical non-sensitive reference for maintainers and automation agents.

## Safety and Sensitivity Rules

- Never commit credentials, tokens, or key material.
- Keep .env local only.
- Commit only variable names, never variable values.
- Treat API responses as potentially sensitive and avoid storing raw dumps unless needed.

## Top-Level File Map

- sync_parkrun.js
  - Main sync runner (incremental/latest and run-scoped modes).
  - Writes main and optionally junior tables.
- sync_all_data.js
  - Full loader for historical pulls with pagination and insert progress logging.
- utilities/backfill-missing.js
  - Targeted backfill for specific run_id + missing_position records.
- utilities/compare-bq-vs-eventhistory.js
  - Compares BigQuery counts vs Parkrun event runs API.
  - Outputs JSON and text reports.
- utilities/publish-bigquery-views.js
  - Publishes SQL files in sql/bigquery as BigQuery views.
- utilities/create-bigquery-tables.js
  - Creates/ensures BigQuery tables used by the pipeline.
- utilities/sync-event-coordinates.js
  - Syncs Parkrun coordinate feed into `parkrun_data.event_coordinates`.
  - Uses BigQuery load job with `WRITE_TRUNCATE` for safe repeat runs.
- utilities/sync-weather-history.js
  - Syncs weather snapshots into `parkrun_data.event_weather` using Open-Meteo archive data.
  - Supports backfill mode (`npm run sync:weather`) and latest-only mode (`npm run sync:weather:latest`).
- dashboard/src/lib/cache.ts
  - In-memory result caching utility with TTL support for SSR performance optimization.
  - Exports: getCached(key), setCached(key, data, ttlMinutes), clearCache(key), clearAllCache()
  - Currently used by RunReport.astro and TopLists.astro to reduce BigQuery queries during traffic peaks (6-hour TTL).
- sql/bigquery/\*.sql
  - QA, summary, duplicate detection, and reporting views.

## BigQuery Data Model

Dataset default:

- parkrun_data

Core tables:

- results
- volunteers
- junior_results
- junior_volunteers

Supporting table:

- event_coordinates
- event_weather

Common key fields:

- run_id
- event_number
- event_date

Important convention:

- athlete_id 2214 indicates Parkrun Unknown ATHLETE placeholder.

## View Pack

Source folder:

- sql/bigquery

View naming:

- utilities/publish-bigquery-views.js converts SQL filenames into view ids.
- Current prefix behavior supports underscore-prefixed numeric names (for example _01_...).

Notable SQL files:

- 01_results_rows_by_run_id.sql
  - Includes row_count and volunteer_row_count by run_id.
- 06_results_athlete_summary.sql
  - Includes latest profile fields, fastest_time, appearances, and highest observed profile metrics:
    - highest_parkrun_club_membership_number
    - highest_volunteer_club_membership_number
    - highest_run_total
    - highest_volunteer_count
    - genuine_pb_count
- 07_junior_results_athlete_summary.sql
  - Junior equivalent of 06 with the same metric columns.
- 08_volunteers_athlete_roles_summary.sql
  - Volunteer roles summary plus joined highest profile metrics and genuine_pb_count from results.
- 09_junior_volunteers_athlete_roles_summary.sql
  - Junior volunteer equivalent of 08.
- 16_headline_stats.sql
  - Headline metrics including PB and genuine PB totals:
    - parkrun_pb_count, junior_pb_count
    - parkrun_genuine_pb_count, junior_genuine_pb_count
- 17_missing_positions.sql
  - Detects missing finish positions in results.
- 22_dashboard_visitor_stats.sql
  - Produces visitor map rows with coordinate matching from `event_coordinates`.
  - Some rows can remain unmatched (NULL coordinates) for retired/renamed events.

## Parkrun API Endpoints in Use

Authentication:

- POST /user_auth.php

Main event data:

- GET /v1/events/{eventId}/runs
  - Run list and run-level counts.
- GET /v1/events/{eventId}/runs/{runId}/results
  - Result rows for one run.
- GET /v1/events/{eventId}/runs/{runId}/volunteers
  - Volunteer rows for one run.
- GET /v1/events/{eventId}/results
  - Broad paginated results endpoint (used in some flows).
- GET /v1/volunteers?eventNumber={eventId}
  - Broad paginated volunteers endpoint.

## Retry, Timeout, and Backoff Approaches

HTTP 403 handling (important):

- utilities/backfill-missing.js:
  - Waits 100 seconds on 403 and re-authenticates before retry.
- utilities/compare-bq-vs-eventhistory.js:
  - Waits 100 seconds on 403 for auth and runs fetches, then retries once.
- sync_all_data.js:
  - Uses configurable 403 retry wait via GET_ALL_RETRY_403_MS (default 100000 ms).

Pagination approach:

- Page size generally 100.
- Use Content-Range metadata where available to determine total rows.

Operational preference:

- For targeted fixes, use run-scoped endpoints (runs/{runId}/...) to avoid full-history stress.

## Reporting Outputs

Comparison script outputs:

- JSON summary: utilities/compare-bq-output.json (or --out path)
- Text report: utilities/compare-bq-report.txt (or --text-out path)

Text report sections:

- Missing events summary
- Finishers differences table
- Volunteers differences table
- Repeated for parkrun and junior comparisons

## Useful Run Commands

- npm run setup:bq
- npm run publish:views
- npm run sync:coordinates
- npm run sync:weather
- npm run sync:weather:latest
- npm run dev
- npm run backfill -- --input missing.json
- npm run compare:bq

## GitHub Actions Weekly Job Notes

- Workflow: `.github/workflows/weekly-sync.yml`
- Weekly schedule: Monday 05:00 UTC (plus manual dispatch)
- Current sequence:
  - `npm run setup:bq`
  - `npm run sync:coordinates`
  - `npm run dev`
  - `npm run sync:weather:latest`

## Ongoing Maintenance Notes

- Prefer run-scoped backfills for isolated missing rows.
- Re-run utilities/compare-bq-vs-eventhistory.js after data corrections.
- Dashboard date display convention is `dd-mm-yyyy`.
- For scripted or agent-driven terminal runs, prefix commands with DISABLE_AUTO_UPDATE=true to prevent interactive oh-my-zsh update prompts.
- Keep temporary probe/debug scripts out of long-term repo state.
- Update this document when file responsibilities or endpoint usage changes.
