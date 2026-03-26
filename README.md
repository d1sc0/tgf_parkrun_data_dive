# tgf-parkrun-processor

Syncs Parkrun event results and historical volunteer data into BigQuery for analysis.

Script layout:

- Main sync entrypoint: `sync_parkrun.js`
- Utility scripts live under `utilities/` (setup, view publishing, backfill, compare)

## Maintainer Reference

- [docs/repo-operations-reference.md](docs/repo-operations-reference.md)

## How it works

1. Authenticates with the Parkrun API using your event account credentials.
2. Fetches result rows and volunteer history rows for the configured venue ID.
3. In incremental mode, refreshes only recent dates.
4. Before insert, deletes existing rows for the same event number and event date(s) so data is overwritten, not duplicated.
5. Writes into BigQuery tables for main event and, optionally, junior event.
6. Processes run results first, then processes volunteers.
7. Marks API placeholder athletes using an explicit flag for easier analysis.

Results unknown-athlete field:

- is_unknown_athlete: true when `athlete_id = 2214` (Parkrun API "Unknown ATHLETE" placeholder), false otherwise

Volunteer role fields:

- run_id: run instance number from the API row
- task_id: first volunteer role ID for the row
- task_ids: comma-separated list of all volunteer role IDs for the row
- task_name: comma-separated role names resolved using a two-step approach: direct name fields on the volunteer API row are preferred (`TaskName`, `VolunteerRoleName`, `VolunteerRole` and lowercase variants); run-scoped roster metadata (`/v1/events/{eventId}/runs/{runId}/rosters`) is used as a fallback when no direct name is present

## BigQuery tables

Default table names:

- Main results: results
- Main volunteers: volunteers
- Junior results: junior_results
- Junior volunteers: junior_volunteers

## Environment variables

Use .env.example as a template.

Core config:

- GCP_PROJECT_ID
- GOOGLE_CREDENTIALS_PATH
- BIGQUERY_DATASET_ID
- BIGQUERY_RESULTS_TABLE
- BIGQUERY_VOLUNTEERS_TABLE
- BIGQUERY_JUNIOR_RESULTS_TABLE
- BIGQUERY_JUNIOR_VOLUNTEERS_TABLE
- PARKRUN_EVENT_ID
- PARKRUN_USERNAME
- PARKRUN_PASSWORD
- JUNIOR_EVENT_ID
- JUNIOR_USERNAME
- JUNIOR_PASSWORD

Runtime flags:

- RUN_JUNIOR
- FETCH_LATEST_ONLY
- TARGET_EVENT_NUMBER
- START_EVENT_NUMBER
- SCRAPE_ALL_EVENTS
- SCRAPE_MAX_EVENTS
- RUN_FETCH_DELAY_MS

## Runtime behavior and flags

- SCRAPE_ALL_EVENTS=true
  - Full history mode.
  - When `FETCH_LATEST_ONLY=false` and no `TARGET_EVENT_NUMBER` is set, full history is fetched run-by-run via `/v1/events/{eventId}/runs` then direct run endpoints for better reliability.

- SCRAPE_ALL_EVENTS=false (or unset)
  - Incremental mode.
  - Loads rows with EventDate >= latest date currently stored, then overwrites that date slice.

- FETCH_LATEST_ONLY=true
  - Fetches the newest run directly via `/v1/events/{eventId}/runs?limit=1`, then loads that run's results/volunteers from run-scoped endpoints.
  - Best option for fast weekly sync.

- TARGET_EVENT_NUMBER=<RunId>
  - Fetches only one specific event instance (RunId), for example 232.
  - Uses direct run-scoped endpoints (`/v1/events/{eventId}/runs/{runId}/results` and `/v1/events/{eventId}/runs/{runId}/volunteers`) to avoid wide history pagination.

- START_EVENT_NUMBER=<RunId>
  - Full run mode starts at this RunId and skips older runs.
  - Useful for resuming a backfill without reprocessing the earliest history.
  - Applies only when run-scoped history is active (`SCRAPE_ALL_EVENTS=true`, `FETCH_LATEST_ONLY=false`, and `TARGET_EVENT_NUMBER` unset).

- RUN_FETCH_DELAY_MS=250
  - Milliseconds delay between run-scoped requests during full-history mode.
  - Increase this (for example 500-1000) if Parkrun starts returning 403/429 during large backfills.

- SCRAPE_MAX_EVENTS=1
  - Keeps only the newest one event date from fetched rows before writing.

## Local usage

Install dependencies:

npm install

Create/check dataset and tables:

npm run setup:bq

Run sync:

npm run dev

Run latest-only sync (local one-off):

FETCH_LATEST_ONLY=true SCRAPE_MAX_EVENTS=1 RUN_JUNIOR=false npm run dev

## BigQuery query pack

Reusable BigQuery SQL files are available in:

- [sql/bigquery](sql/bigquery)

Includes:

- row counts by `run_id` for all tables
- total row count checks
- athlete summaries for `results` and `junior_results`
- volunteer role summaries for `volunteers` and `junior_volunteers`
- duplicate detection queries
- daily QA checks (latest-date rows, day-over-day deltas, null-rate checks, latest-run completeness)
- headline one-row stats query (`16_headline_stats.sql`)

Athlete summary view notes:

- `06_results_athlete_summary.sql` and `07_junior_results_athlete_summary.sql` now include:
  - `highest_parkrun_club_membership_number`
  - `highest_volunteer_club_membership_number`
  - `highest_run_total`
  - `highest_volunteer_count`
  - `genuine_pb_count`

Volunteer athlete summary view notes:

- `08_volunteers_athlete_roles_summary.sql` and `09_junior_volunteers_athlete_roles_summary.sql` include the same highest observed profile metrics and `genuine_pb_count` joined by `athlete_id` from results tables.

Headline stats notes:

- `16_headline_stats.sql` now includes both classic PB and genuine PB totals:
  - `parkrun_pb_count`, `junior_pb_count`
  - `parkrun_genuine_pb_count`, `junior_genuine_pb_count`

Publish all SQL files as BigQuery views:

`npm run publish:views`

Optional override for destination dataset:

- `BIGQUERY_VIEWS_DATASET_ID` (defaults to `BIGQUERY_DATASET_ID`)

## On-demand full loader

Use `sync_all_data.js` when you want a separate full reload process that is independent of `npm run dev` flags.

How `sync_all_data.js` works:

1. Authenticates to Parkrun API.
2. Loads all results for the configured event via `/v1/events/{eventId}/results` pagination.
3. Writes results to BigQuery page-by-page (does not wait for all pages in memory).
4. Loads all volunteers for the configured event via `/v1/volunteers?eventNumber={eventId}` pagination.
5. Writes volunteers to BigQuery page-by-page.
6. Repeats for junior event only when `RUN_JUNIOR=true`.
7. Uses retry logic for API errors, with a dedicated delay for HTTP 403.

Write strategy:

- Results are written first, then volunteers.
- For each event, the script first attempts to delete all existing rows for that event number from the target table.
- If delete is blocked by BigQuery streaming buffer, it falls back to key-based dedupe before insert.

Run command:

node sync_all_data.js

Useful environment variables for `sync_all_data.js`:

- `RUN_JUNIOR` (default `false`):
  - `false` runs only main event tables.
  - `true` runs main + junior tables.
- `VOLUNTEERS_ONLY` (default `false`):
  - `true` skips results entirely and only reloads the volunteers table. Useful for repairing volunteer role names without touching results.
- `GET_ALL_PAGE_CONCURRENCY` (default `1`): number of API pages fetched in parallel.
- `GET_ALL_START_OFFSET` (default `0`): resume/pickup offset for pagination; normalized to page size (100).
- `GET_ALL_RETRY_403_MS` (default `100000`): wait time before retrying a request that returns HTTP 403.
- `GET_ALL_PROGRESS_EVERY_PAGES` (default `10`): how often cumulative insert progress is logged.

Example runs:

- Main event only:
  - `RUN_JUNIOR=false node sync_all_data.js`
- Main + junior:
  - `RUN_JUNIOR=true node sync_all_data.js`
- Volunteers only (repair volunteer role names):
  - `VOLUNTEERS_ONLY=true node sync_all_data.js`
- Volunteers only for main + junior:
  - `VOLUNTEERS_ONLY=true RUN_JUNIOR=true node sync_all_data.js`
- Resume from offset 3500:
  - `GET_ALL_START_OFFSET=3500 node sync_all_data.js`
- Increase throughput carefully:
  - `GET_ALL_PAGE_CONCURRENCY=2 node sync_all_data.js`

Direct utility script paths (if not using npm scripts):

- `node utilities/create-bigquery-tables.js`
- `node utilities/publish-bigquery-views.js`
- `node utilities/backfill-missing.js --input missing.json`
- `node utilities/compare-bq-vs-eventhistory.js --out utilities/compare-bq-output.json --text-out utilities/compare-bq-report.txt`

## GitHub Actions weekly schedule

Workflow file:

.github/workflows/weekly-sync.yml

Current behavior in workflow:

- Runs every Monday at 05:00 UTC.
- Also supports manual run via workflow_dispatch.
- Runs in latest-only mode:
  - FETCH_LATEST_ONLY=true
  - SCRAPE_MAX_EVENTS=1
  - RUN_JUNIOR=false

### Required GitHub repository secrets

- GCP_SERVICE_ACCOUNT_KEY
  - Full JSON content of your service-account key file.
- GCP_PROJECT_ID
- PARKRUN_CLIENT_ID
- PARKRUN_CLIENT_SECRET
- PARKRUN_EVENT_ID
- PARKRUN_USERNAME
- PARKRUN_PASSWORD
- JUNIOR_EVENT_ID
- JUNIOR_USERNAME
- JUNIOR_PASSWORD

Note: JUNIOR\_\* secrets are included in the workflow env for convenience even though RUN_JUNIOR is false. You can remove them from the workflow if you do not plan to run junior jobs.

## Notes

- Cron in GitHub Actions uses UTC.
- If Parkrun API rate-limits, rerun later or reduce frequency.
- Overwrite logic prevents duplicates by deleting existing event/date slice before insert.
