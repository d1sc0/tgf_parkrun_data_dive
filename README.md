# tgf-parkrun-processor

Syncs Parkrun event results and historical volunteer data into BigQuery for analysis.

## How it works

1. Authenticates with the Parkrun API using your event account credentials.
2. Fetches result rows and volunteer history rows for the configured venue ID.
3. In incremental mode, refreshes only recent dates.
4. Before insert, deletes existing rows for the same event number and event date(s) so data is overwritten, not duplicated.
5. Writes into BigQuery tables for main event and, optionally, junior event.

Volunteer role fields:

- task_id: first volunteer role ID for the row
- task_ids: comma-separated list of all volunteer role IDs for the row
- task_name: comma-separated role names resolved from roster task metadata

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
- SCRAPE_ALL_EVENTS
- SCRAPE_MAX_EVENTS

## Runtime behavior and flags

- SCRAPE_ALL_EVENTS=true
  - Full history mode.

- SCRAPE_ALL_EVENTS=false (or unset)
  - Incremental mode.
  - Loads rows with EventDate >= latest date currently stored, then overwrites that date slice.

- FETCH_LATEST_ONLY=true
  - API fetch stops once the newest event date is fully collected.
  - Best option for fast weekly sync.

- TARGET_EVENT_NUMBER=<RunId>
  - Fetches only one specific event instance (RunId), for example 232.

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
