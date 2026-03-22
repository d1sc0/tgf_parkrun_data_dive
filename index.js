require('dotenv').config();

const axios = require('axios');
const qs = require('querystring');
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');

// ─── Parkrun API constants (from parkrun.js src/constants.ts) ─────────────────
const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_AUTH = [
  'PARKRUN_CLIENT_ID_REDACTED',
  'PARKRUN_CLIENT_SECRET_REDACTED',
];
const PARKRUN_USER_AGENT = 'parkrun/1.2.7 CFNetwork/1121.2.2 Darwin/19.3.0';
const PARKRUN_VERSION = '2.0.1';

// ─── Low-level Parkrun API client ─────────────────────────────────────────────

/**
 * Authenticate with the Parkrun API and return an access token.
 */
async function parkrunAuth(username, password) {
  const body = qs.stringify({
    username,
    password,
    scope: 'app',
    grant_type: 'password',
  });

  const maxAttempts = 4;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const res = await axios.post(`${PARKRUN_API_BASE}/user_auth.php`, body, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': PARKRUN_USER_AGENT,
          'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
        },
        auth: { username: PARKRUN_AUTH[0], password: PARKRUN_AUTH[1] },
      });

      if (!res.data || !res.data.access_token) {
        throw new Error('Authentication failed: no access_token in response');
      }
      return res.data.access_token;
    } catch (err) {
      const status = err?.response?.status;
      const retriable =
        status === 429 ||
        status === 500 ||
        status === 502 ||
        status === 503 ||
        status === 504;

      if (attempt < maxAttempts && retriable) {
        const waitMs = attempt * 5000;
        console.warn(
          `  Auth attempt ${attempt}/${maxAttempts} failed with HTTP ${status}. Retrying in ${Math.round(waitMs / 1000)}s...`,
        );
        await new Promise(resolve => setTimeout(resolve, waitMs));
        continue;
      }

      if (status === 403) {
        throw new Error(
          'Parkrun auth returned HTTP 403. Most common causes are invalid credentials/secret formatting or source IP blocking (common on shared CI runners).',
        );
      }

      throw err;
    }
  }
}

/**
 * Build an authenticated axios instance for the Parkrun API.
 */
function makeAuthedClient(accessToken) {
  return axios.create({
    baseURL: PARKRUN_API_BASE,
    headers: {
      'User-Agent': PARKRUN_USER_AGENT,
      'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
    },
    params: {
      access_token: accessToken,
      scope: 'app',
      expandedDetails: true,
    },
  });
}

/**
 * Paginate a Parkrun API endpoint that uses Content-Range / offset pagination.
 * Returns the full combined array of data items.
 */
async function multiGet(client, url, extraParams, dataKey, rangeKey) {
  // First request to discover totals and get the first page of data
  const firstRes = await client.get(url, {
    params: { ...extraParams, limit: 100, offset: 0 },
  });
  const range = firstRes.data['Content-Range']?.[rangeKey]?.[0];

  let data = firstRes.data.data?.[dataKey] || [];

  if (!range) {
    return data;
  }

  const amountDownloaded = data.length;
  const amountTotal = parseInt(range.max, 10) || 0;
  const remaining = amountTotal - amountDownloaded;

  if (remaining <= 0) return data;

  const pulls = Math.ceil(remaining / 100);
  const requests = [];
  for (let i = 0; i < pulls; i++) {
    requests.push(
      client.get(url, {
        params: {
          ...extraParams,
          offset: amountDownloaded + i * 100,
          limit: 100,
        },
      }),
    );
  }

  const responses = await Promise.all(requests);
  for (const res of responses) {
    const pageData = res.data.data?.[dataKey] || [];
    data = data.concat(pageData);
  }

  return data;
}

/**
 * Get event metadata by numeric event ID.
 * Returns { internalName, displayName }
 */
async function getEvent(client, eventId) {
  const res = await client.get(`/v1/events/${eventId}`);
  const event = res.data?.data?.Events?.[0];
  if (!event) throw new Error(`Event ${eventId} not found`);
  return {
    internalName: event.EventName,
    displayName: event.EventLongName,
  };
}

/**
 * Fetch event results with optional early-stop filters:
 * - latestOnly: fetch only newest event date
 * - targetEventNumber: fetch only a specific RunId (event instance number)
 */
async function fetchEventResults(
  client,
  eventId,
  { latestOnly, targetEventNumber },
) {
  const limit = 100;
  let offset = 0;
  let allRows = [];
  let newestDate = null;

  while (true) {
    const res = await client.get('/v1/results', {
      params: { eventNumber: eventId, limit, offset },
    });

    const rows = res.data?.data?.Results || [];
    if (rows.length === 0) break;

    if (!newestDate && rows[0]?.EventDate) {
      newestDate = toDateString(rows[0].EventDate);
    }

    allRows = allRows.concat(rows);

    if (latestOnly && newestDate) {
      const hasOlderDates = rows.some(
        r => toDateString(r.EventDate) !== newestDate,
      );
      if (hasOlderDates) {
        allRows = allRows.filter(r => toDateString(r.EventDate) === newestDate);
        break;
      }
    }

    if (targetEventNumber !== null) {
      const runIds = rows
        .map(r => parseInt(r.RunId, 10))
        .filter(n => Number.isFinite(n));

      if (runIds.length > 0) {
        const minRunId = Math.min(...runIds);
        const foundTarget = runIds.includes(targetEventNumber);

        if (minRunId < targetEventNumber || foundTarget) {
          allRows = allRows.filter(
            r => parseInt(r.RunId, 10) === targetEventNumber,
          );
          break;
        }
      }
    }

    if (rows.length < limit) break;
    offset += rows.length;
  }

  return allRows;
}

/**
 * Fetch historical volunteer rows with optional early-stop filters:
 * - latestOnly: fetch only newest event date
 * - targetEventNumber: fetch only a specific RunId (event instance number)
 */
async function fetchEventVolunteers(
  client,
  eventId,
  { latestOnly, targetEventNumber },
) {
  const limit = 100;
  let offset = 0;
  let allRows = [];
  let newestDate = null;

  while (true) {
    const res = await client.get('/v1/volunteers', {
      params: { eventNumber: eventId, limit, offset },
    });

    const rows = res.data?.data?.Volunteers || [];
    if (rows.length === 0) break;

    if (!newestDate && rows[0]?.EventDate) {
      newestDate = toDateString(rows[0].EventDate);
    }

    allRows = allRows.concat(rows);

    if (latestOnly && newestDate) {
      const hasOlderDates = rows.some(
        r => toDateString(r.EventDate) !== newestDate,
      );
      if (hasOlderDates) {
        allRows = allRows.filter(r => toDateString(r.EventDate) === newestDate);
        break;
      }
    }

    if (targetEventNumber !== null) {
      const runIds = rows
        .map(r => parseInt(r.RunId, 10))
        .filter(n => Number.isFinite(n));

      if (runIds.length > 0) {
        const minRunId = Math.min(...runIds);
        const foundTarget = runIds.includes(targetEventNumber);

        if (minRunId < targetEventNumber || foundTarget) {
          allRows = allRows.filter(
            r => parseInt(r.RunId, 10) === targetEventNumber,
          );
          break;
        }
      }
    }

    if (rows.length < limit) break;
    offset += rows.length;
  }

  return allRows;
}

/**
 * Build a task-id -> task-name lookup from roster rows.
 * The roster endpoint includes task names even when the volunteers endpoint
 * only returns role IDs.
 */
async function fetchVolunteerRoleNameMap(client, eventId) {
  const limit = 100;
  let offset = 0;
  const roleNameById = new Map();

  while (true) {
    const res = await client.get(`/v1/events/${eventId}/rosters`, {
      params: { limit, offset },
    });

    const rows = res.data?.data?.Rosters || [];
    if (rows.length === 0) break;

    for (const row of rows) {
      const id = row?.taskid != null ? parseInt(row.taskid, 10) : null;
      const name = row?.TaskName ? String(row.TaskName).trim() : '';
      if (Number.isFinite(id) && name && !roleNameById.has(id)) {
        roleNameById.set(id, name);
      }
    }

    if (rows.length < limit) break;
    offset += rows.length;
  }

  return roleNameById;
}

// ─── Environment ──────────────────────────────────────────────────────────────
const {
  GCP_PROJECT_ID,
  GOOGLE_CREDENTIALS_PATH,
  BIGQUERY_DATASET_ID = 'parkrun_data',
  BIGQUERY_RESULTS_TABLE = 'results',
  BIGQUERY_VOLUNTEERS_TABLE = 'volunteers',
  BIGQUERY_JUNIOR_RESULTS_TABLE = 'junior_results',
  BIGQUERY_JUNIOR_VOLUNTEERS_TABLE = 'junior_volunteers',
  PARKRUN_USERNAME,
  PARKRUN_PASSWORD,
  PARKRUN_EVENT_ID,
  JUNIOR_USERNAME,
  JUNIOR_PASSWORD,
  JUNIOR_EVENT_ID,
  RUN_JUNIOR,
  FETCH_LATEST_ONLY,
  TARGET_EVENT_NUMBER,
  SCRAPE_ALL_EVENTS,
  SCRAPE_MAX_EVENTS,
} = process.env;

const SHOULD_RUN_JUNIOR = RUN_JUNIOR === 'true';
const SHOULD_FETCH_LATEST_ONLY = FETCH_LATEST_ONLY === 'true';
const TARGET_EVENT_NUMBER_INT = TARGET_EVENT_NUMBER
  ? parseInt(TARGET_EVENT_NUMBER, 10)
  : null;
const SCRAPE_ALL = SCRAPE_ALL_EVENTS === 'true';
const MAX_EVENTS = SCRAPE_MAX_EVENTS ? parseInt(SCRAPE_MAX_EVENTS, 10) : null;

// ─── BigQuery Schemas ─────────────────────────────────────────────────────────
const RESULTS_SCHEMA = [
  { name: 'run_id', type: 'INTEGER', mode: 'REQUIRED' },
  { name: 'athlete_id', type: 'INTEGER', mode: 'REQUIRED' },
  { name: 'event_date', type: 'DATE', mode: 'REQUIRED' },
  { name: 'event_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'event_number', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'finish_position', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'gender_position', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'first_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'last_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'age_category', type: 'STRING', mode: 'NULLABLE' },
  { name: 'age_grading', type: 'FLOAT', mode: 'NULLABLE' },
  { name: 'finish_time', type: 'STRING', mode: 'NULLABLE' },
  { name: 'was_pb', type: 'BOOLEAN', mode: 'NULLABLE' },
  { name: 'was_genuine_pb', type: 'BOOLEAN', mode: 'NULLABLE' },
  { name: 'was_first_run_at_event', type: 'BOOLEAN', mode: 'NULLABLE' },
  { name: 'series_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'updated', type: 'TIMESTAMP', mode: 'NULLABLE' },
];

const VOLUNTEERS_SCHEMA = [
  { name: 'roster_id', type: 'INTEGER', mode: 'REQUIRED' },
  { name: 'event_number', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'event_date', type: 'DATE', mode: 'NULLABLE' },
  { name: 'athlete_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'task_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'task_ids', type: 'STRING', mode: 'NULLABLE' },
  { name: 'task_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'first_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'last_name', type: 'STRING', mode: 'NULLABLE' },
];

// ─── BigQuery client ──────────────────────────────────────────────────────────
const bq = new BigQuery({
  projectId: GCP_PROJECT_ID,
  keyFilename: path.resolve(GOOGLE_CREDENTIALS_PATH),
});

// ─── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Parse a boolean-like value from the parkrun API.
 * The API returns values like "Y"/"N", "1"/"0", true/false, or null.
 */
function parseBool(val) {
  if (val == null || val === '') return null;
  if (typeof val === 'boolean') return val;
  const s = String(val).trim().toLowerCase();
  return s === '1' || s === 'y' || s === 'true';
}

/**
 * Extract a YYYY-MM-DD string from an ISO date string or Date object.
 */
function toDateString(val) {
  if (!val) return null;
  const s = typeof val === 'string' ? val : val.toISOString();
  return s.split('T')[0];
}

// ─── Ensure dataset + tables exist ───────────────────────────────────────────
async function ensureDatasetAndTables() {
  const dataset = bq.dataset(BIGQUERY_DATASET_ID);

  const [datasetExists] = await dataset.exists();
  if (!datasetExists) {
    console.log(`Creating BigQuery dataset: ${BIGQUERY_DATASET_ID}`);
    await dataset.create();
  }

  const tableConfigs = [
    [BIGQUERY_RESULTS_TABLE, RESULTS_SCHEMA],
    [BIGQUERY_VOLUNTEERS_TABLE, VOLUNTEERS_SCHEMA],
  ];

  if (SHOULD_RUN_JUNIOR) {
    tableConfigs.push(
      [BIGQUERY_JUNIOR_RESULTS_TABLE, RESULTS_SCHEMA],
      [BIGQUERY_JUNIOR_VOLUNTEERS_TABLE, VOLUNTEERS_SCHEMA],
    );
  }

  for (const [tableId, schema] of tableConfigs) {
    const table = dataset.table(tableId);
    const [exists] = await table.exists();
    if (!exists) {
      console.log(`Creating BigQuery table: ${tableId}`);
      await table.create({ schema });
    }
  }

  // Keep volunteer tables forward-compatible when new nullable columns are added.
  await ensureColumnExists(BIGQUERY_VOLUNTEERS_TABLE, {
    name: 'task_ids',
    type: 'STRING',
    mode: 'NULLABLE',
  });

  if (SHOULD_RUN_JUNIOR) {
    await ensureColumnExists(BIGQUERY_JUNIOR_VOLUNTEERS_TABLE, {
      name: 'task_ids',
      type: 'STRING',
      mode: 'NULLABLE',
    });
  }
}

async function ensureColumnExists(tableId, columnDef) {
  const table = bq.dataset(BIGQUERY_DATASET_ID).table(tableId);
  const [exists] = await table.exists();
  if (!exists) return;

  const [metadata] = await table.getMetadata();
  const fields = metadata?.schema?.fields || [];
  const hasColumn = fields.some(f => f.name === columnDef.name);

  if (hasColumn) return;

  const updatedSchema = [...fields, columnDef];
  await table.setMetadata({
    schema: {
      fields: updatedSchema,
    },
  });

  console.log(`Added column ${columnDef.name} to ${tableId}.`);
}

// ─── Query latest stored date for an event ───────────────────────────────────
async function getLatestStoredDate(tableId, eventName) {
  const query = [
    `SELECT MAX(event_date) AS latest_date`,
    `FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_name = @eventName`,
  ].join(' ');

  const [rows] = await bq.query({ query, params: { eventName } });
  const raw = rows[0] && rows[0].latest_date;
  // BigQuery DATE values come back as { value: 'YYYY-MM-DD' }
  return (raw && (raw.value || raw)) || null;
}

async function getLatestStoredDateForEvent(tableId, eventNumber) {
  const query = [
    `SELECT MAX(event_date) AS latest_date`,
    `FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
  ].join(' ');

  const [rows] = await bq.query({ query, params: { eventNumber } });
  const raw = rows[0] && rows[0].latest_date;
  return (raw && (raw.value || raw)) || null;
}

// ─── Overwrite existing rows for incoming event dates ────────────────────────
async function deleteRowsForEventDates(tableId, eventNumber, eventDates) {
  if (!eventDates || eventDates.length === 0) return;

  const query = [
    `DELETE FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
    // Compare as string to avoid parameter type coercion edge-cases with DATE arrays.
    `AND CAST(event_date AS STRING) IN UNNEST(@eventDates)`,
  ].join(' ');

  try {
    await bq.query({
      query,
      params: {
        eventNumber,
        eventDates,
      },
    });
    return true;
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    if (msg.toLowerCase().includes('streaming buffer')) {
      console.warn(
        `  Warning: delete skipped for ${tableId} due to streaming buffer; using key-dedupe fallback.`,
      );
      return false;
    }
    throw new Error(
      `Failed deleting existing rows from ${tableId} for event ${eventNumber} and dates [${eventDates.join(', ')}]: ${msg}`,
    );
  }
}

async function getExistingKeysForEventDates(
  tableId,
  eventNumber,
  eventDates,
  keyExpr,
) {
  if (!eventDates || eventDates.length === 0) return new Set();

  const query = [
    `SELECT ${keyExpr} AS dedupe_key`,
    `FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
    `AND CAST(event_date AS STRING) IN UNNEST(@eventDates)`,
  ].join(' ');

  const [rows] = await bq.query({
    query,
    params: {
      eventNumber,
      eventDates,
    },
  });

  return new Set(rows.map(r => r.dedupe_key).filter(Boolean));
}

// ─── Map raw API row → BigQuery row ──────────────────────────────────────────
function mapResultRow(raw) {
  return {
    run_id: parseInt(raw.RunId, 10),
    athlete_id: parseInt(raw.AthleteID, 10),
    event_date: toDateString(raw.EventDate),
    event_name: raw.EventLongName || null,
    event_number:
      raw.EventNumber != null ? parseInt(raw.EventNumber, 10) : null,
    finish_position:
      raw.FinishPosition != null ? parseInt(raw.FinishPosition, 10) : null,
    gender_position:
      raw.GenderPosition != null ? parseInt(raw.GenderPosition, 10) : null,
    first_name: raw.FirstName || null,
    last_name: raw.LastName || null,
    age_category: raw.AgeCategory || null,
    age_grading: raw.AgeGrading != null ? parseFloat(raw.AgeGrading) : null,
    finish_time: raw.RunTime || null,
    was_pb: parseBool(raw.WasPbRun),
    was_genuine_pb: parseBool(raw.GenuinePB),
    was_first_run_at_event: parseBool(raw.FirstTimer),
    series_id: raw.SeriesID != null ? parseInt(raw.SeriesID, 10) : null,
    updated: raw.Updated || null,
  };
}

function parseVolunteerRoleIds(rawRoleIds) {
  if (!rawRoleIds || String(rawRoleIds).trim() === '') return [];
  return String(rawRoleIds)
    .split(',')
    .map(s => parseInt(s.trim(), 10))
    .filter(n => Number.isFinite(n));
}

function mapVolunteerRoleNames(roleIds, roleNameById) {
  if (roleIds.length === 0) return null;
  const names = roleIds.map(id => roleNameById.get(id) || `Role ${id}`);
  return names.join(', ');
}

function mapVolunteerRoleIdsCsv(roleIds) {
  if (roleIds.length === 0) return null;
  return roleIds.join(',');
}

// ─── Batch-insert rows into a BigQuery table ─────────────────────────────────
async function insertRows(tableId, rows, idFn) {
  if (rows.length === 0) {
    console.log(`  No rows to insert into ${tableId}.`);
    return;
  }

  const table = bq.dataset(BIGQUERY_DATASET_ID).table(tableId);
  const BATCH_SIZE = 500; // streaming insert limit per request

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const bqRows = batch.map(row => ({
      insertId: String(idFn(row)),
      json: row,
    }));
    try {
      await table.insert(bqRows, {
        raw: true,
        skipInvalidRows: false,
        ignoreUnknownValues: false,
      });
    } catch (err) {
      if (err.name === 'PartialFailureError' && err.errors) {
        const sample = err.errors
          .slice(0, 3)
          .map(e => ({ row: e.row, errors: e.errors }));
        console.error(
          `  BigQuery insert error (first 3 of ${err.errors.length}):`,
          JSON.stringify(sample, null, 2),
        );
      }
      throw err;
    }
  }

  console.log(`  Inserted ${rows.length} rows into ${tableId}.`);
}

// ─── Process a single parkrun event ──────────────────────────────────────────
async function processEvent({
  username,
  password,
  eventId,
  label,
  resultsTable,
  volunteersTable,
}) {
  console.log(`\n[${label}] Authenticating as ${username} ...`);
  const token = await parkrunAuth(username.trim(), password.trim());
  const client = makeAuthedClient(token);

  console.log(`[${label}] Fetching event info (ID: ${eventId}) ...`);
  const { internalName: eventShortName, displayName: eventDisplayName } =
    await getEvent(client, eventId);
  console.log(`[${label}] Event: "${eventDisplayName}" (${eventShortName})`);

  // ── Historical volunteers ───────────────────────────────────────────────
  console.log(`[${label}] Fetching volunteer history for event ${eventId} ...`);
  const rawVolunteers = await fetchEventVolunteers(client, eventId, {
    latestOnly: SHOULD_FETCH_LATEST_ONLY,
    targetEventNumber: TARGET_EVENT_NUMBER_INT,
  });
  console.log(
    `  Retrieved ${rawVolunteers.length} total volunteer rows from API.`,
  );

  const roleNameById = await fetchVolunteerRoleNameMap(client, eventId);
  console.log(
    `  Loaded ${roleNameById.size} volunteer role names from roster data.`,
  );

  let volunteersToInsertRaw = rawVolunteers;

  if (!SCRAPE_ALL) {
    const latestVolunteerDate = await getLatestStoredDateForEvent(
      volunteersTable,
      parseInt(eventId, 10),
    );
    if (latestVolunteerDate) {
      volunteersToInsertRaw = rawVolunteers.filter(
        r => toDateString(r.EventDate) >= latestVolunteerDate,
      );
      console.log(
        `  Incremental volunteer mode: ${volunteersToInsertRaw.length} rows to refresh since ${latestVolunteerDate}.`,
      );
    } else {
      console.log(
        `  No existing volunteer data found – performing full volunteer load.`,
      );
    }
  }

  if (MAX_EVENTS) {
    const sortedVolunteerDates = [
      ...new Set(
        volunteersToInsertRaw
          .map(r => toDateString(r.EventDate))
          .filter(Boolean),
      ),
    ]
      .sort()
      .reverse()
      .slice(0, MAX_EVENTS);

    const volunteerDateSet = new Set(sortedVolunteerDates);
    volunteersToInsertRaw = volunteersToInsertRaw.filter(r =>
      volunteerDateSet.has(toDateString(r.EventDate)),
    );
    console.log(
      `  SCRAPE_MAX_EVENTS=${MAX_EVENTS}: keeping volunteer dates ${sortedVolunteerDates.join(', ')}`,
    );
  }

  if (volunteersToInsertRaw.length > 0) {
    const volunteerRows = volunteersToInsertRaw.map(v => {
      const roleIds = parseVolunteerRoleIds(v.volunteerRoleIds);

      return {
        roster_id: v.VolID != null ? parseInt(v.VolID, 10) : null,
        event_number:
          v.EventNumber != null ? parseInt(v.EventNumber, 10) : null,
        event_date: toDateString(v.EventDate),
        athlete_id: v.AthleteID != null ? parseInt(v.AthleteID, 10) : null,
        task_id: roleIds.length > 0 ? roleIds[0] : null,
        task_ids: mapVolunteerRoleIdsCsv(roleIds),
        task_name: mapVolunteerRoleNames(roleIds, roleNameById),
        first_name: v.FirstName || null,
        last_name: v.LastName || null,
      };
    });

    const volunteerRowsValid = volunteerRows.filter(
      r => r.roster_id !== null && r.event_date,
    );

    const volunteerDates = [
      ...new Set(volunteerRowsValid.map(r => r.event_date).filter(Boolean)),
    ];

    const volunteersDeleted = await deleteRowsForEventDates(
      volunteersTable,
      parseInt(eventId, 10),
      volunteerDates,
    );

    const volunteerKeyFn = r =>
      `${r.event_number}-${r.event_date}-${r.athlete_id}-${r.task_id}-${r.roster_id}`;
    let volunteerRowsToInsert = volunteerRowsValid;

    if (!volunteersDeleted) {
      const existingVolunteerKeys = await getExistingKeysForEventDates(
        volunteersTable,
        parseInt(eventId, 10),
        volunteerDates,
        `CONCAT(CAST(event_number AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(task_id AS STRING), '-', CAST(roster_id AS STRING))`,
      );
      volunteerRowsToInsert = volunteerRowsValid.filter(
        r => !existingVolunteerKeys.has(volunteerKeyFn(r)),
      );
      console.log(
        `  Dedupe fallback: ${volunteerRowsToInsert.length} new volunteer rows to insert.`,
      );
    }

    await insertRows(volunteersTable, volunteerRowsToInsert, volunteerKeyFn);
  } else {
    console.log(`  No volunteer rows matched the configured filters.`);
  }

  // ── Run results ─────────────────────────────────────────────────────────
  console.log(`[${label}] Fetching run results for event ${eventId} ...`);
  const rawResults = await fetchEventResults(client, eventId, {
    latestOnly: SHOULD_FETCH_LATEST_ONLY,
    targetEventNumber: TARGET_EVENT_NUMBER_INT,
  });
  console.log(`  Retrieved ${rawResults.length} total result rows from API.`);

  let toInsert = rawResults;

  if (!SCRAPE_ALL) {
    const latestDate = await getLatestStoredDate(
      resultsTable,
      eventDisplayName,
    );
    if (latestDate) {
      // Include the latest stored date so corrections for that event day are refreshed.
      toInsert = rawResults.filter(
        r => toDateString(r.EventDate) >= latestDate,
      );
      console.log(
        `  Incremental mode: ${toInsert.length} rows to refresh since ${latestDate}.`,
      );
    } else {
      console.log(`  No existing data found – performing full load.`);
    }
  }

  if (MAX_EVENTS) {
    const sortedDates = [
      ...new Set(toInsert.map(r => toDateString(r.EventDate)).filter(Boolean)),
    ]
      .sort()
      .reverse()
      .slice(0, MAX_EVENTS);

    const dateSet = new Set(sortedDates);
    toInsert = toInsert.filter(r => dateSet.has(toDateString(r.EventDate)));
    console.log(
      `  SCRAPE_MAX_EVENTS=${MAX_EVENTS}: keeping dates ${sortedDates.join(', ')}`,
    );
  }

  const mapped = toInsert.map(mapResultRow);

  const resultDates = [
    ...new Set(mapped.map(r => r.event_date).filter(Boolean)),
  ];

  const resultsDeleted = await deleteRowsForEventDates(
    resultsTable,
    parseInt(eventId, 10),
    resultDates,
  );

  const resultKeyFn = r =>
    `${r.event_number}-${r.event_date}-${r.athlete_id}-${r.run_id}`;
  let mappedToInsert = mapped;

  if (!resultsDeleted) {
    const existingResultKeys = await getExistingKeysForEventDates(
      resultsTable,
      parseInt(eventId, 10),
      resultDates,
      `CONCAT(CAST(event_number AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(run_id AS STRING))`,
    );
    mappedToInsert = mapped.filter(
      r => !existingResultKeys.has(resultKeyFn(r)),
    );
    console.log(
      `  Dedupe fallback: ${mappedToInsert.length} new result rows to insert.`,
    );
  }

  await insertRows(resultsTable, mappedToInsert, resultKeyFn);

  console.log(`[${label}] Done.`);
}

// ─── Main ────────────────────────────────────────────────────────────────────
async function main() {
  // Validate required env vars up front.
  const required = [
    'GCP_PROJECT_ID',
    'GOOGLE_CREDENTIALS_PATH',
    'PARKRUN_USERNAME',
    'PARKRUN_PASSWORD',
    'PARKRUN_EVENT_ID',
  ];

  if (SHOULD_RUN_JUNIOR) {
    required.push('JUNIOR_USERNAME', 'JUNIOR_PASSWORD', 'JUNIOR_EVENT_ID');
  }

  const missing = required.filter(k => !process.env[k]);
  if (missing.length > 0) {
    console.error(
      'Missing required environment variables:',
      missing.join(', '),
    );
    process.exit(1);
  }

  console.log(
    `Mode: ${SCRAPE_ALL ? 'full scrape' : 'incremental'}${MAX_EVENTS ? `, max ${MAX_EVENTS} event dates` : ''}`,
  );

  if (SHOULD_FETCH_LATEST_ONLY) {
    console.log(
      'API optimization: FETCH_LATEST_ONLY=true (newest event date only).',
    );
  }
  if (TARGET_EVENT_NUMBER_INT !== null) {
    console.log(
      `API optimization: TARGET_EVENT_NUMBER=${TARGET_EVENT_NUMBER_INT} (specific RunId only).`,
    );
  }

  await ensureDatasetAndTables();

  await processEvent({
    username: PARKRUN_USERNAME,
    password: PARKRUN_PASSWORD,
    eventId: PARKRUN_EVENT_ID,
    label: 'Main parkrun',
    resultsTable: BIGQUERY_RESULTS_TABLE,
    volunteersTable: BIGQUERY_VOLUNTEERS_TABLE,
  });

  if (SHOULD_RUN_JUNIOR) {
    await processEvent({
      username: JUNIOR_USERNAME,
      password: JUNIOR_PASSWORD,
      eventId: JUNIOR_EVENT_ID,
      label: 'Junior parkrun',
      resultsTable: BIGQUERY_JUNIOR_RESULTS_TABLE,
      volunteersTable: BIGQUERY_JUNIOR_VOLUNTEERS_TABLE,
    });
  } else {
    console.log('\nSkipping junior event (RUN_JUNIOR=false).');
  }

  console.log('\nAll done.');
}

main().catch(err => {
  if (err.response) {
    console.error(
      `Fatal error: HTTP ${err.response.status} from ${err.config?.url}`,
    );
    console.error('Response body:', JSON.stringify(err.response.data));
  } else {
    console.error('Fatal error:', err && err.message ? err.message : err);
    if (err && err.stack) {
      console.error(err.stack);
    }
  }
  process.exit(1);
});
