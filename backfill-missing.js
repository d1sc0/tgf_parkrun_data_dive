/**
 * backfill-missing.js
 *
 * Fetches and inserts specific missing result positions into BigQuery.
 *
 * Usage:
 *   node backfill-missing.js --input missing.json
 *
 * Input file format (missing.json):
 *   [
 *     { "run_id": "221", "event_date": "2026-01-03", "missing_position": "209" },
 *     { "run_id": "228", "event_date": "2026-02-21", "missing_position": "223" }
 *   ]
 *
 * You can generate the input from the v_17_missing_positions BigQuery view.
 */
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const qs = require('querystring');
const { BigQuery } = require('@google-cloud/bigquery');

// ─── Config ──────────────────────────────────────────────────────────────────

const {
  GCP_PROJECT_ID,
  GOOGLE_CREDENTIALS_PATH,
  PARKRUN_CLIENT_ID,
  PARKRUN_CLIENT_SECRET,
  BIGQUERY_DATASET_ID = 'parkrun_data',
  BIGQUERY_RESULTS_TABLE = 'results',
  PARKRUN_USERNAME,
  PARKRUN_PASSWORD,
  PARKRUN_EVENT_ID,
} = process.env;

const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_USER_AGENT = 'parkrun/1.2.7 CFNetwork/1121.2.2 Darwin/19.3.0';
const PARKRUN_VERSION = '2.0.1';

const bq = new BigQuery({
  projectId: GCP_PROJECT_ID,
  keyFilename: path.resolve(GOOGLE_CREDENTIALS_PATH),
});

// ─── Auth + API helpers ───────────────────────────────────────────────────────

async function parkrunAuth() {
  const body = qs.stringify({
    username: PARKRUN_USERNAME.trim(),
    password: PARKRUN_PASSWORD.trim(),
    scope: 'app',
    grant_type: 'password',
  });
  const res = await axios.post(`${PARKRUN_API_BASE}/user_auth.php`, body, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'User-Agent': PARKRUN_USER_AGENT,
      'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
    },
    auth: { username: PARKRUN_CLIENT_ID, password: PARKRUN_CLIENT_SECRET },
  });
  if (!res.data?.access_token) throw new Error('Auth failed: no access_token');
  return res.data.access_token;
}

function makeClient(token) {
  return axios.create({
    baseURL: PARKRUN_API_BASE,
    headers: {
      'User-Agent': PARKRUN_USER_AGENT,
      'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
    },
    params: { access_token: token, scope: 'app', expandedDetails: true },
  });
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Fetch all pages from a run-scoped results endpoint.
 * clientRef is { current: axiosInstance } so retry logic can swap in a fresh client.
 * On HTTP 403, waits 100 seconds, re-authenticates, and retries once.
 */
async function fetchAllPages(clientRef, url, { retryCount = 0 } = {}) {
  let firstRes;
  try {
    firstRes = await clientRef.current.get(url, {
      params: { limit: 100, offset: 0 },
    });
  } catch (err) {
    if (err?.response?.status === 403 && retryCount < 1) {
      console.warn(
        `  403 on first page — waiting 100s then re-authenticating...`,
      );
      await sleep(100_000);
      const newToken = await parkrunAuth();
      clientRef.current = makeClient(newToken);
      return fetchAllPages(clientRef, url, { retryCount: retryCount + 1 });
    }
    throw err;
  }

  const range = firstRes.data['Content-Range']?.ResultsRange?.[0];
  let rows = firstRes.data.data?.Results || [];

  if (!range) return rows;

  const total = parseInt(range.max, 10) || 0;
  const remaining = total - rows.length;
  if (remaining <= 0) return rows;

  const offsets = [];
  for (let offset = rows.length; offset < total; offset += 100) {
    offsets.push(offset);
  }

  // Fetch all remaining pages concurrently
  let responses;
  try {
    responses = await Promise.all(
      offsets.map(offset =>
        clientRef.current.get(url, { params: { limit: 100, offset } }),
      ),
    );
  } catch (err) {
    if (err?.response?.status === 403 && retryCount < 1) {
      console.warn(
        `  403 on paginated fetch — waiting 100s then re-authenticating...`,
      );
      await sleep(100_000);
      const newToken = await parkrunAuth();
      clientRef.current = makeClient(newToken);
      return fetchAllPages(clientRef, url, { retryCount: retryCount + 1 });
    }
    throw err;
  }

  for (const res of responses) {
    rows = rows.concat(res.data.data?.Results || []);
  }

  return rows;
}

// ─── Row mapping (mirrors get_all_data.js) ───────────────────────────────────

function parseBool(val) {
  if (val == null || val === '') return null;
  if (typeof val === 'boolean') return val;
  const s = String(val).trim().toLowerCase();
  return s === '1' || s === 'y' || s === 'true';
}

function parseNullableInt(val) {
  if (val == null || val === '') return null;
  const n = parseInt(val, 10);
  return Number.isFinite(n) ? n : null;
}

function toDateString(val) {
  if (!val) return null;
  const s = typeof val === 'string' ? val : val.toISOString();
  return s.split('T')[0];
}

function mapResultRow(raw) {
  const rawAthleteId = parseNullableInt(raw.AthleteID);
  const isUnknown = rawAthleteId == null || rawAthleteId === 2214;
  const athleteId = rawAthleteId ?? 2214;

  return {
    run_id: parseNullableInt(raw.RunId),
    athlete_id: athleteId,
    event_date: toDateString(raw.EventDate),
    event_name: raw.EventLongName || null,
    event_number: parseNullableInt(raw.EventNumber),
    finish_position: parseNullableInt(raw.FinishPosition),
    gender_position: parseNullableInt(raw.GenderPosition),
    first_name: raw.FirstName || null,
    last_name: raw.LastName || null,
    age_category: raw.AgeCategory || null,
    age_grading: raw.AgeGrading != null ? parseFloat(raw.AgeGrading) : null,
    finish_time: raw.RunTime || null,
    was_pb: parseBool(raw.WasPbRun),
    was_genuine_pb: parseBool(raw.GenuinePB),
    was_first_run_at_event: parseBool(raw.FirstTimer),
    is_unknown_athlete: isUnknown,
    club_name: raw.ClubName || null,
    home_run_name: raw.HomeRunName || null,
    run_total: parseNullableInt(raw.RunTotal),
    vol_count: parseNullableInt(raw.volcount),
    parkrun_club_membership: parseNullableInt(raw.parkrunClubMembership),
    volunteer_club_membership: parseNullableInt(raw.volunteerClubMembership),
    junior_run_total: parseNullableInt(raw.JuniorRunTotal),
    junior_club_membership: parseNullableInt(raw.JuniorClubMembership),
    series_id: parseNullableInt(raw.SeriesID),
    updated: raw.Updated || null,
  };
}

// ─── BQ helpers ──────────────────────────────────────────────────────────────

async function insertRows(rows) {
  const table = bq.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_RESULTS_TABLE);
  const bqRows = rows.map((row, idx) => ({
    insertId: `backfill-${row.run_id}-${row.finish_position}-${idx}`,
    json: row,
  }));
  await table.insert(bqRows, {
    raw: true,
    skipInvalidRows: false,
    ignoreUnknownValues: false,
  });
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  // Parse --input argument
  const inputIdx = process.argv.indexOf('--input');
  if (inputIdx === -1 || !process.argv[inputIdx + 1]) {
    console.error('Usage: node backfill-missing.js --input <file.json>');
    console.error('');
    console.error('File format:');
    console.error(
      '  [ { "run_id": "221", "event_date": "2026-01-03", "missing_position": "209" }, ... ]',
    );
    process.exit(1);
  }

  const inputFile = path.resolve(process.argv[inputIdx + 1]);
  const missing = JSON.parse(fs.readFileSync(inputFile, 'utf8'));

  if (!Array.isArray(missing) || missing.length === 0) {
    console.error('Input file must be a non-empty JSON array.');
    process.exit(1);
  }

  console.log(`Loaded ${missing.length} missing position(s) to backfill.\n`);

  // Group by run_id
  const byRunId = new Map();
  for (const entry of missing) {
    const runId = parseInt(entry.run_id, 10);
    const pos = parseInt(entry.missing_position, 10);
    if (!Number.isFinite(runId) || !Number.isFinite(pos)) {
      console.warn(`Skipping invalid entry: ${JSON.stringify(entry)}`);
      continue;
    }
    if (!byRunId.has(runId))
      byRunId.set(runId, { eventDate: entry.event_date, positions: new Set() });
    byRunId.get(runId).positions.add(pos);
  }

  if (byRunId.size === 0) {
    console.error('No valid entries to process.');
    process.exit(1);
  }

  console.log(`Unique run_ids: ${[...byRunId.keys()].join(', ')}\n`);

  // Authenticate
  console.log('\nAuthenticating with Parkrun API...');
  const token = await parkrunAuth();
  // Use a mutable ref so 403-retry logic can swap in a fresh client
  const clientRef = { current: makeClient(token) };
  console.log('Authenticated.\n');

  let totalInserted = 0;
  let totalNotFound = 0;

  // For each run_id, fetch the run-scoped results and find the target positions
  for (const [runId, { eventDate, positions: positionSet }] of byRunId) {
    const url = `/v1/events/${PARKRUN_EVENT_ID}/runs/${runId}/results`;

    console.log(
      `Fetching run_id=${runId} (${eventDate}), looking for position(s): ${[...positionSet].sort((a, b) => a - b).join(', ')}`,
    );

    const rawRows = await fetchAllPages(clientRef, url);
    console.log(`  API returned ${rawRows.length} row(s) for run_id=${runId}`);

    const positionMap = new Map(
      rawRows.map(r => [parseInt(r.FinishPosition, 10), r]),
    );

    const rowsToInsert = [];
    for (const pos of positionSet) {
      const raw = positionMap.get(pos);
      if (!raw) {
        console.warn(`  position ${pos}: NOT FOUND in API response`);
        totalNotFound++;
        continue;
      }
      const mapped = mapResultRow(raw);
      console.log(
        `  position ${pos}: found — athlete_id=${mapped.athlete_id}, ` +
          `is_unknown=${mapped.is_unknown_athlete}, name=${mapped.first_name} ${mapped.last_name}`,
      );
      rowsToInsert.push(mapped);
    }

    if (rowsToInsert.length === 0) {
      console.log('  Nothing to insert for this run.\n');
      continue;
    }

    try {
      await insertRows(rowsToInsert);
      console.log(
        `  ✓ Inserted ${rowsToInsert.length} row(s) into ${BIGQUERY_RESULTS_TABLE}.\n`,
      );
      totalInserted += rowsToInsert.length;
    } catch (err) {
      if (err?.name === 'PartialFailureError' && Array.isArray(err.errors)) {
        const sample = err.errors
          .slice(0, 3)
          .map(e => ({ row: e.row, errors: e.errors }));
        console.error(
          `  BigQuery partial failure (first 3 of ${err.errors.length}):`,
          JSON.stringify(sample, null, 2),
        );
      }
      throw err;
    }
  }

  console.log(
    `\nDone. Inserted ${totalInserted} row(s). ${totalNotFound} position(s) not found in API.`,
  );
}

main().catch(err => {
  console.error('backfill-missing.js failed:', err?.message || err);
  process.exit(1);
});
