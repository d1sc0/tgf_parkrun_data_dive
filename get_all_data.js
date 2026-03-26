require('dotenv').config();

const axios = require('axios');
const qs = require('querystring');
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

// Parkrun API constants (from parkrun.js constants)
const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_USER_AGENT = 'parkrun/1.2.7 CFNetwork/1121.2.2 Darwin/19.3.0';
const PARKRUN_VERSION = '2.0.1';

const {
  GCP_PROJECT_ID,
  GOOGLE_CREDENTIALS_PATH,
  PARKRUN_CLIENT_ID,
  PARKRUN_CLIENT_SECRET,
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
  RUN_JUNIOR = 'false',
  GET_ALL_PAGE_CONCURRENCY = '1',
  GET_ALL_START_OFFSET = '0',
  GET_ALL_RETRY_403_MS = '100000',
  GET_ALL_PROGRESS_EVERY_PAGES = '10',
} = process.env;

const SHOULD_RUN_JUNIOR = RUN_JUNIOR === 'true';
const PAGE_SIZE = 100;
const PAGE_CONCURRENCY = Math.max(
  1,
  parseInt(GET_ALL_PAGE_CONCURRENCY, 10) || 1,
);
const START_OFFSET = Math.max(0, parseInt(GET_ALL_START_OFFSET, 10) || 0);
const RETRY_403_MS = Math.max(
  1000,
  parseInt(GET_ALL_RETRY_403_MS, 10) || 100000,
);
const PROGRESS_EVERY_PAGES = Math.max(
  1,
  parseInt(GET_ALL_PROGRESS_EVERY_PAGES, 10) || 10,
);

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
  { name: 'is_unknown_athlete', type: 'BOOLEAN', mode: 'NULLABLE' },
  { name: 'club_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'home_run_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'run_total', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'vol_count', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'parkrun_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'volunteer_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'junior_run_total', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'junior_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'series_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'updated', type: 'TIMESTAMP', mode: 'NULLABLE' },
];

const VOLUNTEERS_SCHEMA = [
  { name: 'roster_id', type: 'INTEGER', mode: 'REQUIRED' },
  { name: 'event_number', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'run_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'event_date', type: 'DATE', mode: 'NULLABLE' },
  { name: 'athlete_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'task_id', type: 'INTEGER', mode: 'NULLABLE' },
  { name: 'task_ids', type: 'STRING', mode: 'NULLABLE' },
  { name: 'task_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'first_name', type: 'STRING', mode: 'NULLABLE' },
  { name: 'last_name', type: 'STRING', mode: 'NULLABLE' },
];

const bq = new BigQuery({
  projectId: GCP_PROJECT_ID,
  keyFilename: path.resolve(GOOGLE_CREDENTIALS_PATH),
});

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

function parseVolunteerRoleIds(rawRoleIds) {
  if (!rawRoleIds || String(rawRoleIds).trim() === '') return [];
  return String(rawRoleIds)
    .split(',')
    .map(s => parseInt(s.trim(), 10))
    .filter(Number.isFinite);
}

function mapVolunteerRoleIdsCsv(roleIds) {
  if (!roleIds || roleIds.length === 0) return null;
  return roleIds.join(',');
}

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
        auth: { username: PARKRUN_CLIENT_ID, password: PARKRUN_CLIENT_SECRET },
      });

      if (!res.data || !res.data.access_token) {
        throw new Error('Authentication failed: no access_token in response');
      }
      return res.data.access_token;
    } catch (err) {
      const status = err?.response?.status;
      const retriable =
        status === 403 ||
        status === 429 ||
        status === 500 ||
        status === 502 ||
        status === 503 ||
        status === 504;

      if (!retriable || attempt === maxAttempts) throw err;

      const waitMs = status === 403 ? RETRY_403_MS : attempt * 5000;
      console.warn(
        `Auth: HTTP ${status} attempt ${attempt}/${maxAttempts}; retrying in ${Math.round(waitMs / 1000)}s...`,
      );
      await sleep(waitMs);
    }
  }

  throw new Error('Authentication failed after retries');
}

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

async function getWithRetry(client, url, params, label) {
  const maxAttempts = 6;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await client.get(url, { params });
    } catch (err) {
      const status = err?.response?.status;
      const retriable =
        status === 403 ||
        status === 429 ||
        status === 500 ||
        status === 502 ||
        status === 503 ||
        status === 504;

      if (!retriable || attempt === maxAttempts) throw err;

      const waitMs =
        status === 403 ? RETRY_403_MS : Math.max(3000, attempt * 5000);
      console.warn(
        `${label}: HTTP ${status} attempt ${attempt}/${maxAttempts}; retrying in ${Math.round(waitMs / 1000)}s...`,
      );
      await sleep(waitMs);
    }
  }

  throw new Error(`Request failed: ${label}`);
}

async function fetchPaged(
  client,
  url,
  { dataKey, rangeKey, baseParams = {}, label, onPage = null },
) {
  const normalizedStart = Math.floor(START_OFFSET / PAGE_SIZE) * PAGE_SIZE;
  if (normalizedStart !== START_OFFSET) {
    console.log(
      `START_OFFSET ${START_OFFSET} normalized to ${normalizedStart}.`,
    );
  }

  const firstRes = await getWithRetry(
    client,
    url,
    { ...baseParams, limit: PAGE_SIZE, offset: normalizedStart },
    `${label} offset=${normalizedStart}`,
  );

  const firstRows = firstRes.data?.data?.[dataKey] || [];
  const range = firstRes.data?.['Content-Range']?.[rangeKey]?.[0];
  const total = parseInt(range?.max, 10);

  if (!Number.isFinite(total)) {
    throw new Error(`${label}: missing/invalid Content-Range ${rangeKey}.max`);
  }

  if (normalizedStart >= total) {
    console.log(
      `${label}: start offset ${normalizedStart} >= total ${total}, no rows to fetch.`,
    );
    return { totalRows: 0, totalPages: 0, fetchedPages: 0 };
  }

  const totalPages = Math.ceil((total - normalizedStart) / PAGE_SIZE);
  let totalRows = 0;
  let fetchedPages = 0;

  const processPage = async rows => {
    fetchedPages += 1;
    totalRows += rows.length;
    if (onPage) {
      await onPage(rows, { fetchedPages, totalPages });
    }

    if (fetchedPages % 10 === 0 || fetchedPages === totalPages) {
      console.log(
        `${label}: fetched ${fetchedPages}/${totalPages} page(s), ${totalRows} row(s).`,
      );
    }
  };

  await processPage(firstRows);

  const offsets = [];
  for (
    let offset = normalizedStart + PAGE_SIZE;
    offset < total;
    offset += PAGE_SIZE
  ) {
    offsets.push(offset);
  }

  // Sequential page fetch guarantees each page is persisted before moving on,
  // so a retry wait never holds unpersisted pages in memory.
  for (const offset of offsets) {
    const res = await getWithRetry(
      client,
      url,
      { ...baseParams, limit: PAGE_SIZE, offset },
      `${label} offset=${offset}`,
    );

    const rows = res.data?.data?.[dataKey] || [];
    await processPage(rows);
  }

  return {
    totalRows,
    totalPages,
    fetchedPages,
  };
}

async function ensureDatasetAndTables() {
  const dataset = bq.dataset(BIGQUERY_DATASET_ID);
  const [datasetExists] = await dataset.exists();
  if (!datasetExists) {
    console.log(`Creating dataset ${BIGQUERY_DATASET_ID}...`);
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
      console.log(`Creating table ${tableId}...`);
      await table.create({ schema });
    }
  }

  const resultForwardCompatColumns = [
    { name: 'is_unknown_athlete', type: 'BOOLEAN', mode: 'NULLABLE' },
    { name: 'club_name', type: 'STRING', mode: 'NULLABLE' },
    { name: 'home_run_name', type: 'STRING', mode: 'NULLABLE' },
    { name: 'run_total', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'vol_count', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'parkrun_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'volunteer_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'junior_run_total', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'junior_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
  ];

  for (const col of resultForwardCompatColumns) {
    await ensureColumnExists(BIGQUERY_RESULTS_TABLE, col);
    if (SHOULD_RUN_JUNIOR) {
      await ensureColumnExists(BIGQUERY_JUNIOR_RESULTS_TABLE, col);
    }
  }

  for (const col of [
    { name: 'run_id', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'task_ids', type: 'STRING', mode: 'NULLABLE' },
  ]) {
    await ensureColumnExists(BIGQUERY_VOLUNTEERS_TABLE, col);
    if (SHOULD_RUN_JUNIOR) {
      await ensureColumnExists(BIGQUERY_JUNIOR_VOLUNTEERS_TABLE, col);
    }
  }
}

async function ensureColumnExists(tableId, columnDef) {
  const table = bq.dataset(BIGQUERY_DATASET_ID).table(tableId);
  const [exists] = await table.exists();
  if (!exists) return;

  const [metadata] = await table.getMetadata();
  const fields = metadata?.schema?.fields || [];
  if (fields.some(f => f.name === columnDef.name)) return;

  await table.setMetadata({ schema: { fields: [...fields, columnDef] } });
  console.log(`Added column ${columnDef.name} to ${tableId}.`);
}

async function insertRows(tableId, rows) {
  if (rows.length === 0) {
    console.log(`No rows to insert into ${tableId}.`);
    return;
  }

  const table = bq.dataset(BIGQUERY_DATASET_ID).table(tableId);
  const chunkSize = 500;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const bqRows = chunk.map((row, idx) => ({
      insertId:
        `${tableId}-${i + idx}-` +
        `${row.event_number ?? 'na'}-${row.event_date ?? 'na'}-` +
        `${row.run_id ?? row.roster_id ?? 'na'}-${row.athlete_id ?? 'na'}`,
      json: row,
    }));

    try {
      await table.insert(bqRows, {
        raw: true,
        skipInvalidRows: false,
        ignoreUnknownValues: false,
      });
    } catch (err) {
      if (err?.name === 'PartialFailureError' && Array.isArray(err.errors)) {
        const sample = err.errors.slice(0, 3).map(e => ({
          row: e.row,
          errors: e.errors,
        }));
        console.error(
          `BigQuery partial failure for ${tableId} (first 3 of ${err.errors.length}):`,
          JSON.stringify(sample, null, 2),
        );
      }
      throw err;
    }

    if ((i / chunkSize + 1) % 20 === 0 || i + chunkSize >= rows.length) {
      console.log(
        `Inserted ${Math.min(i + chunkSize, rows.length)}/${rows.length} into ${tableId}.`,
      );
    }
  }
}

async function deleteRowsForEvent(tableId, eventNumber) {
  const query = [
    `DELETE FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
  ].join(' ');

  try {
    await bq.query({ query, params: { eventNumber } });
    return true;
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.toLowerCase().includes('streaming buffer')) {
      console.warn(
        `Delete skipped for ${tableId} due to streaming buffer; using dedupe fallback.`,
      );
      return false;
    }
    throw err;
  }
}

async function getExistingKeysForEvent(tableId, eventNumber, keyExpr) {
  const query = [
    `SELECT ${keyExpr} AS dedupe_key`,
    `FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
  ].join(' ');

  const [rows] = await bq.query({ query, params: { eventNumber } });
  return new Set(rows.map(r => r.dedupe_key).filter(Boolean));
}

function mapResultRow(raw) {
  const athleteId = parseNullableInt(raw.AthleteID);
  const runTotal = parseNullableInt(raw.RunTotal);
  const volCount = parseNullableInt(raw.volcount);
  const parkrunClubMembership = parseNullableInt(raw.parkrunClubMembership);
  const volunteerClubMembership = parseNullableInt(raw.volunteerClubMembership);
  const juniorRunTotal = parseNullableInt(raw.JuniorRunTotal);
  const juniorClubMembership = parseNullableInt(raw.JuniorClubMembership);

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
    is_unknown_athlete: athleteId === 2214,
    club_name: raw.ClubName || null,
    home_run_name: raw.HomeRunName || null,
    run_total: Number.isFinite(runTotal) ? runTotal : null,
    vol_count: Number.isFinite(volCount) ? volCount : null,
    parkrun_club_membership: Number.isFinite(parkrunClubMembership)
      ? parkrunClubMembership
      : null,
    volunteer_club_membership: Number.isFinite(volunteerClubMembership)
      ? volunteerClubMembership
      : null,
    junior_run_total: Number.isFinite(juniorRunTotal) ? juniorRunTotal : null,
    junior_club_membership: Number.isFinite(juniorClubMembership)
      ? juniorClubMembership
      : null,
    series_id: parseNullableInt(raw.SeriesID),
    updated: raw.Updated || null,
  };
}

function mapVolunteerRow(raw) {
  const roleIds = parseVolunteerRoleIds(raw.volunteerRoleIds);
  const taskName =
    raw.TaskName || raw.VolunteerRoleName || raw.VolunteerRole || null;

  return {
    roster_id: parseNullableInt(raw.VolID),
    event_number: parseNullableInt(raw.EventNumber),
    run_id: parseNullableInt(raw.RunId),
    event_date: toDateString(raw.EventDate),
    athlete_id: parseNullableInt(raw.AthleteID),
    task_id: roleIds.length > 0 ? roleIds[0] : null,
    task_ids: mapVolunteerRoleIdsCsv(roleIds),
    task_name: taskName,
    first_name: raw.FirstName || null,
    last_name: raw.LastName || null,
  };
}

async function processEvent({
  label,
  username,
  password,
  eventId,
  resultsTable,
  volunteersTable,
}) {
  console.log(`\n[${label}] Authenticating as ${username}...`);
  const token = await parkrunAuth(username.trim(), password.trim());
  const client = makeAuthedClient(token);

  const eventNumberInt = parseInt(eventId, 10);
  const resultsDeleted = await deleteRowsForEvent(resultsTable, eventNumberInt);
  const resultKeyFn = r =>
    `${r.event_number}-${r.event_date}-${r.athlete_id}-${r.run_id}-${r.finish_position ?? 'null'}`;
  let existingResultKeys = null;

  if (!resultsDeleted) {
    existingResultKeys = await getExistingKeysForEvent(
      resultsTable,
      eventNumberInt,
      `CONCAT(CAST(event_number AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(run_id AS STRING), '-', IFNULL(CAST(finish_position AS STRING), 'null'))`,
    );
  }

  let insertedResults = 0;

  console.log(
    `[${label}] Fetching results and writing pages directly to ${resultsTable}...`,
  );
  const resultsFetchSummary = await fetchPaged(
    client,
    `/v1/events/${eventId}/results`,
    {
      dataKey: 'Results',
      rangeKey: 'ResultsRange',
      label: `[${label}] results`,
      onPage: async (rows, pageMeta) => {
        const mappedPageRows = rows
          .map(mapResultRow)
          .filter(
            r => r.run_id != null && r.athlete_id != null && r.event_date,
          );

        let pageToInsert = mappedPageRows;
        if (existingResultKeys) {
          pageToInsert = mappedPageRows.filter(r => {
            const key = resultKeyFn(r);
            if (existingResultKeys.has(key)) return false;
            existingResultKeys.add(key);
            return true;
          });
        }

        if (pageToInsert.length > 0) {
          await insertRows(resultsTable, pageToInsert);
          insertedResults += pageToInsert.length;
        }

        if (
          pageMeta.fetchedPages % PROGRESS_EVERY_PAGES === 0 ||
          pageMeta.fetchedPages === pageMeta.totalPages
        ) {
          console.log(
            `[${label}] results insert progress: ${insertedResults} inserted after ${pageMeta.fetchedPages}/${pageMeta.totalPages} page(s).`,
          );
        }
      },
    },
  );

  console.log(
    `[${label}] Results complete. Retrieved ${resultsFetchSummary.totalRows} row(s), inserted ${insertedResults}.`,
  );

  const volunteersDeleted = await deleteRowsForEvent(
    volunteersTable,
    eventNumberInt,
  );
  const volunteerKeyFn = r =>
    `${r.event_number}-${r.run_id}-${r.event_date}-${r.athlete_id}-${r.task_id}-${r.roster_id}`;
  let existingVolunteerKeys = null;

  if (!volunteersDeleted) {
    existingVolunteerKeys = await getExistingKeysForEvent(
      volunteersTable,
      eventNumberInt,
      `CONCAT(CAST(event_number AS STRING), '-', CAST(run_id AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(task_id AS STRING), '-', CAST(roster_id AS STRING))`,
    );
  }

  let insertedVolunteers = 0;

  console.log(
    `[${label}] Fetching volunteers and writing pages directly to ${volunteersTable}...`,
  );
  const volunteerFetchSummary = await fetchPaged(client, '/v1/volunteers', {
    dataKey: 'Volunteers',
    rangeKey: 'VolunteersRange',
    baseParams: { eventNumber: eventId },
    label: `[${label}] volunteers`,
    onPage: async (rows, pageMeta) => {
      const mappedPageRows = rows
        .map(mapVolunteerRow)
        .filter(r => r.roster_id != null && r.event_date);

      let pageToInsert = mappedPageRows;
      if (existingVolunteerKeys) {
        pageToInsert = mappedPageRows.filter(r => {
          const key = volunteerKeyFn(r);
          if (existingVolunteerKeys.has(key)) return false;
          existingVolunteerKeys.add(key);
          return true;
        });
      }

      if (pageToInsert.length > 0) {
        await insertRows(volunteersTable, pageToInsert);
        insertedVolunteers += pageToInsert.length;
      }

      if (
        pageMeta.fetchedPages % PROGRESS_EVERY_PAGES === 0 ||
        pageMeta.fetchedPages === pageMeta.totalPages
      ) {
        console.log(
          `[${label}] volunteers insert progress: ${insertedVolunteers} inserted after ${pageMeta.fetchedPages}/${pageMeta.totalPages} page(s).`,
        );
      }
    },
  });

  console.log(
    `[${label}] Volunteers complete. Retrieved ${volunteerFetchSummary.totalRows} row(s), inserted ${insertedVolunteers}.`,
  );

  console.log(
    `[${label}] Done. Inserted ${insertedResults} results and ${insertedVolunteers} volunteers.`,
  );
}

async function main() {
  console.log('Starting get_all_data.js...');
  console.log(`RUN_JUNIOR=${RUN_JUNIOR}`);
  console.log(`GET_ALL_PAGE_CONCURRENCY=${PAGE_CONCURRENCY}`);
  console.log(`GET_ALL_START_OFFSET=${START_OFFSET}`);
  console.log(`GET_ALL_RETRY_403_MS=${RETRY_403_MS}`);
  console.log(`GET_ALL_PROGRESS_EVERY_PAGES=${PROGRESS_EVERY_PAGES}`);

  await ensureDatasetAndTables();

  await processEvent({
    label: 'PARKRUN',
    username: PARKRUN_USERNAME,
    password: PARKRUN_PASSWORD,
    eventId: PARKRUN_EVENT_ID,
    resultsTable: BIGQUERY_RESULTS_TABLE,
    volunteersTable: BIGQUERY_VOLUNTEERS_TABLE,
  });

  if (SHOULD_RUN_JUNIOR) {
    await processEvent({
      label: 'JUNIOR',
      username: JUNIOR_USERNAME,
      password: JUNIOR_PASSWORD,
      eventId: JUNIOR_EVENT_ID,
      resultsTable: BIGQUERY_JUNIOR_RESULTS_TABLE,
      volunteersTable: BIGQUERY_JUNIOR_VOLUNTEERS_TABLE,
    });
  }

  console.log('All requested data loads complete.');
}

main().catch(err => {
  const status = err?.response?.status;
  const payload = err?.response?.data || err?.message || err;
  console.error(
    `get_all_data.js failed${status ? ` (HTTP ${status})` : ''}:`,
    payload,
  );
  process.exit(1);
});
