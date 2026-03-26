# Repo Operations Reference

Purpose: practical non-sensitive reference for maintainers and automation agents.

## Safety and Sensitivity Rules

- Never commit credentials, tokens, or key material.
- Keep .env local only.
- Commit only variable names, never variable values.
- Treat API responses as potentially sensitive and avoid storing raw dumps unless needed.

## Top-Level File Map

- index.js
  - Main sync runner (incremental/latest and run-scoped modes).
  - Writes main and optionally junior tables.
- get_all_data.js
  - Full loader for historical pulls with pagination and insert progress logging.
- backfill-missing.js
  - Targeted backfill for specific run_id + missing_position records.
- compare-bq-vs-eventhistory.js
  - Compares BigQuery counts vs Parkrun event runs API.
  - Outputs JSON and text reports.
- publish-bigquery-views.js
  - Publishes SQL files in sql/bigquery as BigQuery views.
- create-bigquery-tables.js
  - Creates/ensures BigQuery tables used by the pipeline.
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

- publish-bigquery-views.js converts SQL filenames into view ids.
- Current prefix behavior supports underscore-prefixed numeric names (for example _01_...).

Notable SQL files:

- 01_results_rows_by_run_id.sql
  - Includes row_count and volunteer_row_count by run_id.
- 16_headline_stats.sql
  - Headline metrics including total finishers fields.
- 17_missing_positions.sql
  - Detects missing finish positions in results.

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

- backfill-missing.js:
  - Waits 100 seconds on 403 and re-authenticates before retry.
- compare-bq-vs-eventhistory.js:
  - Waits 100 seconds on 403 for auth and runs fetches, then retries once.
- get_all_data.js:
  - Uses configurable 403 retry wait via GET_ALL_RETRY_403_MS (default 100000 ms).

Pagination approach:

- Page size generally 100.
- Use Content-Range metadata where available to determine total rows.

Operational preference:

- For targeted fixes, use run-scoped endpoints (runs/{runId}/...) to avoid full-history stress.

## Reporting Outputs

Comparison script outputs:

- JSON summary: compare-bq-output.json (or --out path)
- Text report: compare-bq-report.txt (or --text-out path)

Text report sections:

- Missing events summary
- Finishers differences table
- Volunteers differences table
- Repeated for parkrun and junior comparisons

## Useful Run Commands

- npm run setup:bq
- npm run publish:views
- npm run dev
- npm run backfill -- --input missing.json
- npm run compare:bq -- --out compare-bq-output.json --text-out compare-bq-report.txt

## Ongoing Maintenance Notes

- Prefer run-scoped backfills for isolated missing rows.
- Re-run compare-bq-vs-eventhistory.js after data corrections.
- Keep temporary probe/debug scripts out of long-term repo state.
- Update this document when file responsibilities or endpoint usage changes.
