# tgf-parkrun-processor

A specialized ETL pipeline and mobile-first analytics dashboard for TGF parkrun event data to provide key stats for the core team. This project extracts results and volunteer history from the Parkrun API and visualizes insights using an Astro SSR dashboard.

## 🏗 Architecture

- **ETL (Node.js/CommonJS):** Periodically fetches data from Parkrun and pushes to BigQuery.
- **Dashboard (Astro/ESM):** A server-side rendered (SSR) frontend that queries BigQuery views directly.
- **Data Layer (BigQuery):** 28 published SQL views that serve as a single source of truth:
  - **Core views:** Row counts, athlete summaries, volunteer summaries, duplicate detection, and QA checks.
  - **Dashboard views:** 9 optimized views (\_20–\_28) precompute aggregations, rankings, and transformations for each dashboard component. Components execute lightweight `SELECT *` queries and handle only display logic.

## 📱 Dashboard Features

- **Privacy First:** Personally Identifiable Information (PII) like athlete names are never stored in the repository or as static assets. They are fetched from BigQuery at request time.
- **Mobile Optimized:** Built with a "hamburger-first" navigation and card-based layouts for effective use at the finish line.
- **Performance:** Zero-JS baseline using Astro's Islands architecture combined with pre-computed BigQuery views for fast server-side rendering.
- **Separation of Concerns:** Views handle all complex aggregation/ranking/time logic; components focus on filtering, formatting, and user interactions.

## 🚀 Quick Start

### Syncing Data

```bash
npm install
npm run dev # Runs the incremental sync
```

### Running the Dashboard

```bash
npm run dashboard # Starts Astro dev server
```

## Maintainer Reference

- [docs/repo-operations-reference.md](docs/repo-operations-reference.md)
- [docs/event-coordinates-optimization.md](docs/event-coordinates-optimization.md)
- [docs/ssr-result-caching-optimization.md](docs/ssr-result-caching-optimization.md)
- [docs/weather-data-optimization.md](docs/weather-data-optimization.md)
- [docs/dashboard-ui-patterns.md](docs/dashboard-ui-patterns.md)

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
- task_name: role name resolved using a three-step approach: (1) direct name fields on the volunteer API row (`TaskName`, `VolunteerRoleName`, `VolunteerRole` and lowercase variants); (2) date-scoped roster metadata (`/v1/events/{eventId}/rosters/{yyyymmdd}`) looked up by `athleteId` + `eventdate` for a direct per-person match; (3) `Role {id}` fallback if still unresolved

## BigQuery Architecture

**Core Tables:**

- Main results: results
- Main volunteers: volunteers
- Junior results: junior_results
- Junior volunteers: junior_volunteers

**Published Views (28 total):**

- **Support views (01-19):** Row counts, athlete summaries, volunteer summaries, duplicate detection, run-time stats, daily QA checks.
- **Dashboard views (20-28):** Optimized for Astro SSR components:
  - \_20: HeadlineStats
  - \_21: Course Records
  - \_22: Visitor Stats (Home Run Map)
  - \_23: Volunteer Milestones
  - \_24: Attendance Tracker
  - \_25: Performance Tracker
  - \_26: Run Report
  - \_27: Top Lists
  - \_28: Volunteer Tracker

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
- BIGQUERY_WEATHER_TABLE
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

Weather cache config:

- WEATHER_LATITUDE
- WEATHER_LONGITUDE
- WEATHER_HOUR_UTC

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

Sync event coordinates used by the visitor map:

npm run sync:coordinates

Backfill weather cache used by Run Report:

npm run sync:weather

Run sync:

npm run dev

Run latest-only sync (local one-off):

FETCH_LATEST_ONLY=true SCRAPE_MAX_EVENTS=1 RUN_JUNIOR=false npm run dev

## 🚀 Deployment

The analytics dashboard is optimized for **Firebase App Hosting** using Server-Side Rendering (SSR).

### Deployment Workflow

Deployment is managed directly by **Firebase App Hosting**. When you link your repository in the Firebase Console, Firebase creates its own internal pipeline. Every push to the `main` branch triggers an automatic build and rollout. You do **not** need a manual deployment workflow file in `.github/workflows/`.

### Firebase Configuration

The following files in the repository root control the deployment:

- **`firebase.json`**: Primary hosting and rewrite configuration.
- **`apphosting.yaml`**: Backend resource settings (CPU, Memory, Concurrency) for the Cloud Run instance.
- **Environment Variables**: You must configure `GCP_PROJECT_ID` as an environment variable in the Firebase Console (App Hosting settings) so the SSR components can query BigQuery.

**IAM Permissions:** Ensure the App Hosting backend service account has the `BigQuery Data Viewer` and `BigQuery Job User` roles assigned in the Google Cloud Console.

### SSR Result Caching

The dashboard implements in-memory result caching with a 6-hour TTL (360 minutes) to reduce BigQuery query volume during traffic peaks:

- **RunReport.astro:** Caches latest run stats (`runReport_{run_id}`) and weather data (`weather_{run_id}`) per run ID
- **TopLists.astro:** Caches global top-20 leaderboards (`topLists_global`) as a single entry
- **Cache Behavior:** Queries bypass cache on first request and populate for subsequent requests within the TTL window; expired entries are automatically purged
- **Cache Invalidation:** The in-memory cache is cleared on application restart. For manual cache invalidation during data syncs, the dashboard will restart as part of the deployment process
- **TTL Rationale:** 6-hour TTL is safe because data only refreshes weekly on Monday, so cached data is rarely more than 6 days stale
- **Impact:** 80-90% reduction in BigQuery queries during typical sessions, meaningful cost savings on slot usage

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
- `RUN_FETCH_DELAY_MS` (default `0`): milliseconds delay between roster fetches during volunteer processing. One request is made per unique event date, so rate-limiting is rarely needed.

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
- Syncs event coordinates before data sync:
  - npm run sync:coordinates
- Runs data sync in latest-only mode:
  - FETCH_LATEST_ONLY=true
  - SCRAPE_MAX_EVENTS=1
  - RUN_JUNIOR=false
- Syncs weather for latest run after data sync:
  - npm run sync:weather:latest

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
