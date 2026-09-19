require('dotenv').config();

const axios = require('axios');
const qs = require('querystring');
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');

// ─── Parkrun API constants ────────────────────────────────────────────────────
const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_CLIENT_ID = process.env.PARKRUN_CLIENT_ID;
const PARKRUN_CLIENT_SECRET = process.env.PARKRUN_CLIENT_SECRET;
const PARKRUN_USER_AGENT =
  process.env.PARKRUN_USER_AGENT ||
  'parkrun/2.2.0 CFNetwork/1498.700.2 Darwin/23.6.0';
const TOKEN_CACHE_FILE = path.resolve(__dirname, '.parkrun-token-cache.json');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Token Cache Helpers ──────────────────────────────────────────────────────
function loadTokenCache() {
  try {
    if (fs.existsSync(TOKEN_CACHE_FILE)) {
      const data = JSON.parse(fs.readFileSync(TOKEN_CACHE_FILE, 'utf8'));
      if (typeof data === 'object' && data !== null) return data;
    }
  } catch (_) {}
  return {};
}

function saveTokenCache(cache) {
  try {
    fs.writeFileSync(TOKEN_CACHE_FILE, JSON.stringify(cache, null, 2), {
      mode: 0o600,
    });
  } catch (err) {
    console.warn(`  Warning: could not write token cache: ${err.message}`);
  }
}

function getCachedToken(username) {
  const cache = loadTokenCache();
  const entry = cache[username];
  if (!entry || !entry.access_token || !entry.expires_at) return null;
  // Consider token valid if it has at least 2 minutes (120s) remaining
  if (entry.expires_at > Date.now() + 120000) {
    const minsLeft = Math.max(
      1,
      Math.round((entry.expires_at - Date.now()) / 60000),
    );
    console.log(
      `  Using valid cached Parkrun access token (expires in ~${minsLeft}m).`,
    );
    return entry.access_token;
  }
  return null;
}

function setCachedToken(username, tokenData) {
  const cache = loadTokenCache();
  const expiresInSec = tokenData.expires_in
    ? parseInt(tokenData.expires_in, 10)
    : 3600;
  cache[username] = {
    access_token: tokenData.access_token,
    refresh_token: tokenData.refresh_token || null,
    expires_at: Date.now() + expiresInSec * 1000,
  };
  saveTokenCache(cache);
}

function clearCachedToken(username) {
  const cache = loadTokenCache();
  if (cache[username]) {
    delete cache[username];
    saveTokenCache(cache);
  }
}

// ─── Low-level Parkrun API client ─────────────────────────────────────────────

/**
 * Authenticate with the Parkrun API and return an access token.
 * Uses local file cache (.parkrun-token-cache.json) to avoid repeated logins.
 */
async function parkrunAuth(username, password, { forceRefresh = false } = {}) {
  const cleanUser = username.trim();
  const cleanPass = password.trim();

  if (!forceRefresh) {
    const cached = getCachedToken(cleanUser);
    if (cached) return cached;
  }

  const body = qs.stringify({
    username: cleanUser,
    password: cleanPass,
    scope: 'app',
    grant_type: 'password',
  });

  const maxAttempts = 3;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const res = await axios.post(`${PARKRUN_API_BASE}/user_auth.php`, body, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': PARKRUN_USER_AGENT,
          Accept: 'application/json',
          'Accept-Language': 'en-GB,en;q=0.9',
        },
        auth: { username: PARKRUN_CLIENT_ID, password: PARKRUN_CLIENT_SECRET },
        timeout: 30000,
      });

      if (!res.data || !res.data.access_token) {
        throw new Error('Authentication failed: no access_token in response');
      }

      setCachedToken(cleanUser, res.data);
      return res.data.access_token;
    } catch (err) {
      const status = err?.response?.status;
      const isLast = attempt === maxAttempts;

      if (status === 403) {
        if (!isLast) {
          console.warn(
            `  Parkrun auth returned HTTP 403 (WAF cooldown). Waiting 100s before retrying (attempt ${attempt}/${maxAttempts})...`,
          );
          await sleep(100000);
          continue;
        }
        throw new Error(
          'Parkrun auth returned HTTP 403 after retry. Most common causes are invalid credentials or Cloudflare IP rate-limit.',
        );
      }

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
        await sleep(waitMs);
        continue;
      }

      throw err;
    }
  }
}

/**
 * Build an authenticated axios instance for the Parkrun API.
 */
function makeAuthedClient(accessToken, username) {
  const client = axios.create({
    baseURL: PARKRUN_API_BASE,
    headers: {
      'User-Agent': PARKRUN_USER_AGENT,
      Accept: 'application/json',
      'Accept-Language': 'en-GB,en;q=0.9',
    },
    params: {
      access_token: accessToken,
      scope: 'app',
      expandedDetails: true,
    },
    timeout: 30000,
  });
  client._parkrunUsername = username;
  return client;
}

/**
 * Robust GET wrapper with 403 cooldown (100s) and transient retries.
 */
async function apiGet(client, url, config = {}, maxAttempts = 3) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await client.get(url, config);
    } catch (err) {
      const status = err?.response?.status;
      const isLast = attempt === maxAttempts;

      if (status === 401) {
        if (client._parkrunUsername) {
          clearCachedToken(client._parkrunUsername);
        }
        throw err;
      }

      if (status === 403) {
        if (client._parkrunUsername) {
          clearCachedToken(client._parkrunUsername);
        }
        if (!isLast) {
          console.warn(
            `  HTTP 403 on ${url} (WAF cooldown). Waiting 100s before retry (attempt ${attempt}/${maxAttempts})...`,
          );
          await sleep(100000);
          continue;
        }
      }

      const retriable =
        status === 429 ||
        status === 500 ||
        status === 502 ||
        status === 503 ||
        status === 504;

      if (retriable && !isLast) {
        const waitMs = attempt * 3000;
        console.warn(
          `  HTTP ${status} on ${url}. Retrying in ${Math.round(waitMs / 1000)}s (attempt ${attempt}/${maxAttempts})...`,
        );
        await sleep(waitMs);
        continue;
      }

      throw err;
    }
  }
}

/**
 * Paginate a Parkrun API endpoint that uses Content-Range / offset pagination.
 * Fetches pages sequentially with polite inter-page pacing to prevent WAF bursts.
 */
async function multiGet(client, url, extraParams, dataKey, rangeKey) {
  // First request to discover totals and get the first page of data
  const firstRes = await apiGet(client, url, {
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
  for (let i = 0; i < pulls; i += 1) {
    await sleep(250); // polite inter-page delay to avoid WAF rate-burst alarms

    const res = await apiGet(client, url, {
      params: {
        ...extraParams,
        offset: amountDownloaded + i * 100,
        limit: 100,
      },
    });

    const pageData = res.data?.data?.[dataKey] || [];
    data = data.concat(pageData);
  }

  return data;
}

async function fetchRunIds(client, eventId, { startRunId = null } = {}) {
  const limit = 100;
  let offset = 0;
  const runIds = [];

  while (true) {
    const res = await apiGet(client, `/v1/events/${eventId}/runs`, {
      params: { limit, offset },
    });

    const runs = res.data?.data?.Runs || [];
    if (runs.length === 0) break;

    for (const run of runs) {
      const runId = parseInt(run.RunId, 10);
      if (Number.isFinite(runId)) {
        runIds.push(runId);
      }
    }

    if (runs.length < limit) break;
    offset += runs.length;
    await sleep(250);
  }

  if (startRunId !== null) {
    return runIds.filter(runId => runId >= startRunId);
  }

  return runIds;
}

async function fetchLatestRunId(client, eventId) {
  const firstPage = await apiGet(client, `/v1/events/${eventId}/runs`, {
    params: { limit: 1, offset: 0 },
  });

  const totalRunsRaw =
    firstPage.data?.['Content-Range']?.RunsRange?.[0]?.max ||
    firstPage.data?.['Content-Range']?.Runsrange?.[0]?.max;
  const totalRuns = parseInt(totalRunsRaw, 10);

  if (!Number.isFinite(totalRuns) || totalRuns <= 0) {
    const only = firstPage.data?.data?.Runs?.[0];
    const fallbackRunId = only ? parseInt(only.RunId, 10) : null;
    return Number.isFinite(fallbackRunId) ? fallbackRunId : null;
  }

  const lastOffset = Math.max(totalRuns - 1, 0);
  await sleep(250);
  const lastPage = await apiGet(client, `/v1/events/${eventId}/runs`, {
    params: { limit: 1, offset: lastOffset },
  });

  const latest = lastPage.data?.data?.Runs?.[0];
  const latestRunId = latest ? parseInt(latest.RunId, 10) : null;
  return Number.isFinite(latestRunId) ? latestRunId : null;
}

async function fetchRowsByRunIds(
  client,
  eventId,
  runIds,
  { dataType, dataKey, rangeKey, delayMs },
) {
  const allRows = [];

  for (let i = 0; i < runIds.length; i += 1) {
    const runId = runIds[i];
    const endpoint = `/v1/events/${eventId}/runs/${runId}/${dataType}`;

    console.log(
      `  ${dataType}: processing run ${i + 1}/${runIds.length} (run_id=${runId})...`,
    );

    const maxAttempts = 5;
    let rowsForRun = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        rowsForRun = await multiGet(client, endpoint, {}, dataKey, rangeKey);
        break;
      } catch (err) {
        const status = err?.response?.status;
        const retriable =
          status === 403 ||
          status === 429 ||
          status === 500 ||
          status === 502 ||
          status === 503 ||
          status === 504;

        if (!retriable || attempt === maxAttempts) {
          throw err;
        }

        const waitMs =
          status === 403 ? 100000 : Math.max(delayMs, 1000) * attempt;
        console.warn(
          `  ${dataType} run ${runId}: HTTP ${status} on attempt ${attempt}/${maxAttempts}; retrying in ${Math.round(waitMs / 1000)}s...`,
        );
        await sleep(waitMs);
      }
    }

    if (rowsForRun && rowsForRun.length > 0) {
      allRows.push(...rowsForRun);
    }

    if ((i + 1) % 20 === 0 || i + 1 === runIds.length) {
      console.log(
        `  ${dataType}: processed ${i + 1}/${runIds.length} runs (${allRows.length} rows so far).`,
      );
    }

    if (delayMs > 0 && i + 1 < runIds.length) {
      console.log(
        `  ${dataType}: applying inter-run pause of ${delayMs}ms (RUN_FETCH_DELAY_MS=${RUN_FETCH_DELAY_MS ?? 'unset'} => ${RUN_FETCH_DELAY_MS_INT}ms).`,
      );
      await sleep(delayMs);
    }
  }

  return allRows;
}

/**
 * Get event metadata by numeric event ID.
 * Returns { internalName, displayName }
 */
async function getEvent(client, eventId) {
  const res = await apiGet(client, `/v1/events/${eventId}`);
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
  {
    latestOnly,
    targetEventNumber,
    useRunScopedHistory,
    runFetchDelayMs,
    startEventNumber,
  },
) {
  if (targetEventNumber !== null) {
    const runRows = await multiGet(
      client,
      `/v1/events/${eventId}/runs/${targetEventNumber}/results`,
      {},
      'Results',
      'ResultsRange',
    );
    return runRows;
  }

  if (latestOnly) {
    const latestRunId = await fetchLatestRunId(client, eventId);
    if (latestRunId === null) return [];

    const runRows = await multiGet(
      client,
      `/v1/events/${eventId}/runs/${latestRunId}/results`,
      {},
      'Results',
      'ResultsRange',
    );
    return runRows;
  }

  if (useRunScopedHistory) {
    const runIds = await fetchRunIds(client, eventId, {
      startRunId: startEventNumber,
    });
    console.log(
      `  Using run-scoped results fetch across ${runIds.length} runs for event ${eventId}.`,
    );
    return fetchRowsByRunIds(client, eventId, runIds, {
      dataType: 'results',
      dataKey: 'Results',
      rangeKey: 'ResultsRange',
      delayMs: runFetchDelayMs,
    });
  }

  const limit = 100;
  let offset = 0;
  let allRows = [];
  let newestDate = null;

  while (true) {
    const res = await apiGet(client, '/v1/results', {
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

    if (rows.length < limit) break;
    offset += rows.length;
    await sleep(250);
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
  {
    latestOnly,
    targetEventNumber,
    useRunScopedHistory,
    runFetchDelayMs,
    startEventNumber,
  },
) {
  if (targetEventNumber !== null) {
    const runRows = await multiGet(
      client,
      `/v1/events/${eventId}/runs/${targetEventNumber}/volunteers`,
      {},
      'Volunteers',
      'VolunteersRange',
    );
    return runRows;
  }

  if (latestOnly) {
    const latestRunId = await fetchLatestRunId(client, eventId);
    if (latestRunId === null) return [];

    const runRows = await multiGet(
      client,
      `/v1/events/${eventId}/runs/${latestRunId}/volunteers`,
      {},
      'Volunteers',
      'VolunteersRange',
    );
    return runRows;
  }

  if (useRunScopedHistory) {
    const runIds = await fetchRunIds(client, eventId, {
      startRunId: startEventNumber,
    });
    console.log(
      `  Using run-scoped volunteers fetch across ${runIds.length} runs for event ${eventId}.`,
    );
    return fetchRowsByRunIds(client, eventId, runIds, {
      dataType: 'volunteers',
      dataKey: 'Volunteers',
      rangeKey: 'VolunteersRange',
      delayMs: runFetchDelayMs,
    });
  }

  const limit = 100;
  let offset = 0;
  let allRows = [];
  let newestDate = null;

  while (true) {
    const res = await apiGet(client, '/v1/volunteers', {
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

    if (rows.length < limit) break;
    offset += rows.length;
    await sleep(250);
  }

  return allRows;
}

/**
 * Build an athleteId -> task-name lookup by fetching date-based rosters.
 * Uses /v1/events/{eventId}/rosters/{yyyymmdd} which returns TaskName per athlete.
 */
async function fetchTaskNameByAthleteIdForDates(client, eventId, dates) {
  const taskNameByKey = new Map();
  const uniqueDates = [...new Set(dates)].filter(Boolean);
  const lookupStartedAt = Date.now();

  for (let i = 0; i < uniqueDates.length; i += 1) {
    const date = uniqueDates[i];
    if (i > 0) await sleep(250);
    const dateStr = String(date).replace(/-/g, '');
    const maxAttempts = 3;
    let rosterRows = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        rosterRows = await multiGet(
          client,
          `/v1/events/${eventId}/rosters/${dateStr}`,
          {},
          'Rosters',
          'RostersRange',
        );
        break;
      } catch (err) {
        const status = err?.response?.status;
        const retriable =
          status === 403 ||
          status === 429 ||
          status === 500 ||
          status === 502 ||
          status === 503 ||
          status === 504;

        if (!retriable || attempt === maxAttempts) {
          throw err;
        }

        const waitMs =
          status === 403 ? 100000 : Math.max(RUN_FETCH_DELAY_MS_INT, 1000) * attempt;
        console.warn(
          `  rosters ${dateStr}: HTTP ${status} attempt ${attempt}/${maxAttempts}; retrying in ${Math.round(waitMs / 1000)}s.`,
        );
        await sleep(waitMs);
      }
    }

    const rows = rosterRows || [];
    for (const row of rows) {
      const rawAthleteId = row?.athleteid ?? row?.AthleteID ?? row?.athleteId;
      const id = rawAthleteId != null ? parseInt(rawAthleteId, 10) : null;
      const name = firstNonEmptyString([
        row?.TaskName,
        row?.taskName,
        row?.taskname,
        row?.VolunteerRoleName,
        row?.volunteerRoleName,
      ]);
      if (Number.isFinite(id) && name) {
        const key = `${dateStr}:${id}`;
        if (!taskNameByKey.has(key)) taskNameByKey.set(key, new Set());
        taskNameByKey.get(key).add(name);
      }
    }
  }

  const lookupMs = Date.now() - lookupStartedAt;
  console.log(
    `  Volunteer roster lookup completed in ${lookupMs}ms across ${uniqueDates.length} date(s).`,
  );

  return taskNameByKey;
}

// ─── Environment & CLI Arguments ──────────────────────────────────────────────
function parseCliArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const raw = argv[i];
    if (!raw) continue;

    if (raw.includes('=')) {
      const eqIdx = raw.indexOf('=');
      const key = raw
        .slice(0, eqIdx)
        .replace(/^--?/, '')
        .toUpperCase()
        .replace(/-/g, '_');
      const val = raw.slice(eqIdx + 1);
      args[key] = val;
      continue;
    }

    if (raw.startsWith('--') || raw.startsWith('-')) {
      const key = raw.replace(/^--?/, '').toUpperCase().replace(/-/g, '_');
      const next = argv[i + 1];
      if (next && !next.startsWith('-') && !next.includes('=')) {
        args[key] = next;
        i += 1;
      } else {
        args[key] = 'true';
      }
      continue;
    }

    if (/^\d+$/.test(raw)) {
      args.TARGET_EVENT_NUMBER = raw;
    }
  }
  return args;
}

const cliArgs = parseCliArgs(process.argv);

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
  START_EVENT_NUMBER,
  SCRAPE_ALL_EVENTS,
  SCRAPE_MAX_EVENTS,
  RUN_FETCH_DELAY_MS,
} = process.env;

const rawTargetEventNumber =
  cliArgs.TARGET_EVENT_NUMBER ||
  cliArgs.TARGET ||
  cliArgs.EVENT ||
  cliArgs.RUN ||
  TARGET_EVENT_NUMBER;

const TARGET_EVENT_NUMBER_INT = rawTargetEventNumber
  ? parseInt(rawTargetEventNumber, 10)
  : null;

const rawStartEventNumber =
  cliArgs.START_EVENT_NUMBER ||
  cliArgs.START ||
  START_EVENT_NUMBER;

const START_EVENT_NUMBER_INT = rawStartEventNumber
  ? parseInt(rawStartEventNumber, 10)
  : null;

const rawFetchLatestOnly =
  cliArgs.FETCH_LATEST_ONLY !== undefined
    ? cliArgs.FETCH_LATEST_ONLY
    : cliArgs.LATEST !== undefined
      ? cliArgs.LATEST
      : FETCH_LATEST_ONLY;

// If a specific target event number is requested, disable latest-only mode automatically.
const SHOULD_FETCH_LATEST_ONLY =
  TARGET_EVENT_NUMBER_INT !== null
    ? false
    : rawFetchLatestOnly === 'true';

const rawRunJunior =
  cliArgs.RUN_JUNIOR !== undefined
    ? cliArgs.RUN_JUNIOR
    : cliArgs.JUNIOR !== undefined
      ? cliArgs.JUNIOR
      : RUN_JUNIOR;

const SHOULD_RUN_JUNIOR = rawRunJunior === 'true';

const rawScrapeAll =
  cliArgs.SCRAPE_ALL_EVENTS !== undefined
    ? cliArgs.SCRAPE_ALL_EVENTS
    : cliArgs.SCRAPE_ALL !== undefined
      ? cliArgs.SCRAPE_ALL
      : cliArgs.ALL !== undefined
        ? cliArgs.ALL
        : SCRAPE_ALL_EVENTS;

const SCRAPE_ALL = rawScrapeAll === 'true';

const rawMaxEvents =
  cliArgs.SCRAPE_MAX_EVENTS ||
  cliArgs.MAX_EVENTS ||
  SCRAPE_MAX_EVENTS;

const MAX_EVENTS = rawMaxEvents ? parseInt(rawMaxEvents, 10) : null;

const rawRunFetchDelay =
  cliArgs.RUN_FETCH_DELAY_MS ||
  cliArgs.FETCH_DELAY ||
  RUN_FETCH_DELAY_MS;

const RUN_FETCH_DELAY_MS_INT = rawRunFetchDelay
  ? parseInt(rawRunFetchDelay, 10)
  : 250;

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

  await ensureColumnExists(BIGQUERY_RESULTS_TABLE, {
    name: 'is_unknown_athlete',
    type: 'BOOLEAN',
    mode: 'NULLABLE',
  });

  if (SHOULD_RUN_JUNIOR) {
    await ensureColumnExists(BIGQUERY_JUNIOR_RESULTS_TABLE, {
      name: 'is_unknown_athlete',
      type: 'BOOLEAN',
      mode: 'NULLABLE',
    });
  }

  // Add athlete-profile columns to results tables.
  for (const col of [
    { name: 'club_name', type: 'STRING', mode: 'NULLABLE' },
    { name: 'home_run_name', type: 'STRING', mode: 'NULLABLE' },
    { name: 'run_total', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'vol_count', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'parkrun_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'volunteer_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'junior_run_total', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'junior_club_membership', type: 'INTEGER', mode: 'NULLABLE' },
  ]) {
    await ensureColumnExists(BIGQUERY_RESULTS_TABLE, col);
    if (SHOULD_RUN_JUNIOR)
      await ensureColumnExists(BIGQUERY_JUNIOR_RESULTS_TABLE, col);
  }

  // Keep volunteer tables forward-compatible when new nullable columns are added.
  for (const col of [
    { name: 'run_id', type: 'INTEGER', mode: 'NULLABLE' },
    { name: 'task_ids', type: 'STRING', mode: 'NULLABLE' },
  ]) {
    await ensureColumnExists(BIGQUERY_VOLUNTEERS_TABLE, col);
    if (SHOULD_RUN_JUNIOR)
      await ensureColumnExists(BIGQUERY_JUNIOR_VOLUNTEERS_TABLE, col);
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
async function getLatestStoredDate(tableId, eventNumber) {
  const query = [
    `SELECT MAX(event_date) AS latest_date`,
    `FROM \`${GCP_PROJECT_ID}.${BIGQUERY_DATASET_ID}.${tableId}\``,
    `WHERE event_number = @eventNumber`,
  ].join(' ');

  const [rows] = await bq.query({ query, params: { eventNumber } });
  const raw = rows[0] && rows[0].latest_date;
  // BigQuery DATE values come back as { value: 'YYYY-MM-DD' }
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

// Map raw API row → BigQuery row ──────────────────────────────────────────
function mapResultRow(raw) {
  const rawAthleteId =
    raw.AthleteID != null && raw.AthleteID !== ''
      ? parseInt(raw.AthleteID, 10)
      : null;
  // Treat null/missing athleteId as unknown athlete (2214)
  const isUnknown =
    rawAthleteId === 2214 ||
    rawAthleteId == null ||
    !Number.isFinite(rawAthleteId);
  const athleteId = Number.isFinite(rawAthleteId) ? rawAthleteId : 2214;
  const runTotal =
    raw.RunTotal != null && raw.RunTotal !== ''
      ? parseInt(raw.RunTotal, 10)
      : null;
  const volCount =
    raw.volcount != null && raw.volcount !== ''
      ? parseInt(raw.volcount, 10)
      : null;
  const parkrunClubMembership =
    raw.parkrunClubMembership != null && raw.parkrunClubMembership !== ''
      ? parseInt(raw.parkrunClubMembership, 10)
      : null;
  const volunteerClubMembership =
    raw.volunteerClubMembership != null && raw.volunteerClubMembership !== ''
      ? parseInt(raw.volunteerClubMembership, 10)
      : null;
  const juniorRunTotal =
    raw.JuniorRunTotal != null && raw.JuniorRunTotal !== ''
      ? parseInt(raw.JuniorRunTotal, 10)
      : null;
  const juniorClubMembership =
    raw.JuniorClubMembership != null && raw.JuniorClubMembership !== ''
      ? parseInt(raw.JuniorClubMembership, 10)
      : null;

  return {
    run_id: parseInt(raw.RunId, 10),
    athlete_id: athleteId,
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
    is_unknown_athlete: isUnknown,
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

function firstNonEmptyString(values) {
  for (const value of values) {
    if (value == null) continue;
    const normalized = String(value).trim();
    if (normalized) return normalized;
  }
  return '';
}

function mapVolunteerRoleNames(roleIds, taskNameByKey, raw) {
  const allNames = new Set();

  // 1. Check for a name returned directly on the raw volunteer row
  const directName = firstNonEmptyString([
    raw?.TaskName,
    raw?.taskName,
    raw?.task_name,
    raw?.VolunteerRoleName,
    raw?.volunteerRoleName,
    raw?.VolunteerRole,
    raw?.volunteerRole,
  ]);
  if (directName) allNames.add(directName);

  // 2. Look up by date + athleteId from date-based roster
  const dateStr = raw?.EventDate
    ? String(raw.EventDate).replace(/-/g, '')
    : null;
  const athleteId = raw?.AthleteID != null ? parseInt(raw.AthleteID, 10) : null;
  if (dateStr && Number.isFinite(athleteId)) {
    const rosterNames = taskNameByKey.get(`${dateStr}:${athleteId}`);
    if (rosterNames) {
      rosterNames.forEach(n => allNames.add(n));
    }
  }

  // 3. Fall back to role ID only if no names found yet
  if (allNames.size === 0) {
    roleIds.forEach(id => {
      allNames.add(`Role ${id}`);
    });
  }

  if (allNames.size === 0) return 'Unknown Role';
  return Array.from(allNames).join(', ');
}

function mapVolunteerRoleIdsCsv(roleIds) {
  if (roleIds.length === 0) return null;
  return roleIds.join(',');
}

function summarizeRawRowsByEventDate(rawRows) {
  const counts = new Map();

  for (const row of rawRows) {
    const eventDate = toDateString(row.EventDate);
    if (!eventDate) continue;
    counts.set(eventDate, (counts.get(eventDate) || 0) + 1);
  }

  return [...counts.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([eventDate, count]) => `${eventDate}: ${count}`)
    .join(', ');
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

// ─── Per-run-id write: fetch and insert results+volunteers one run at a time ──
async function processRunScopedHistory({
  client,
  eventId,
  label,
  resultsTable,
  volunteersTable,
}) {
  const runIds = await fetchRunIds(client, eventId, {
    startRunId: START_EVENT_NUMBER_INT,
  });
  console.log(
    `  Full-history mode: ${runIds.length} runs to process for event ${eventId}.`,
  );
  if (START_EVENT_NUMBER_INT !== null) {
    console.log(
      `  Full-history start filter enabled: START_EVENT_NUMBER=${START_EVENT_NUMBER_INT}.`,
    );
  }

  const resultKeyFn = r =>
    `${r.event_number}-${r.event_date}-${r.athlete_id}-${r.run_id}-${r.finish_position ?? 'null'}`;
  const volunteerKeyFn = r =>
    `${r.event_number}-${r.run_id}-${r.event_date}-${r.athlete_id}-${r.task_id}-${r.roster_id}`;

  let totalResultsInserted = 0;
  let totalVolunteersInserted = 0;

  for (let i = 0; i < runIds.length; i += 1) {
    const runId = runIds[i];
    console.log(`\n  [Run ${i + 1}/${runIds.length}] run_id=${runId}`);

    // ── Results ────────────────────────────────────────────────────────────
    let resultRows = null;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        resultRows = await multiGet(
          client,
          `/v1/events/${eventId}/runs/${runId}/results`,
          {},
          'Results',
          'ResultsRange',
        );
        break;
      } catch (err) {
        const status = err?.response?.status;
        const retriable =
          status === 403 ||
          status === 429 ||
          status === 500 ||
          status === 502 ||
          status === 503 ||
          status === 504;
        if (!retriable || attempt === 5) throw err;
        const waitMs = Math.max(RUN_FETCH_DELAY_MS_INT, 1000) * attempt;
        console.warn(
          `    results run ${runId}: HTTP ${status} on attempt ${attempt}/5; retrying in ${Math.round(waitMs / 1000)}s...`,
        );
        console.warn(
          `    results run ${runId}: applying retry pause of ${waitMs}ms (RUN_FETCH_DELAY_MS=${RUN_FETCH_DELAY_MS ?? 'unset'} => ${RUN_FETCH_DELAY_MS_INT}ms).`,
        );
        await sleep(waitMs);
      }
    }

    if (resultRows && resultRows.length > 0) {
      const mapped = resultRows.map(mapResultRow);
      const resultDates = [
        ...new Set(mapped.map(r => r.event_date).filter(Boolean)),
      ];
      const resultsDeleted = await deleteRowsForEventDates(
        resultsTable,
        parseInt(eventId, 10),
        resultDates,
      );
      let toInsert = mapped;
      if (!resultsDeleted) {
        const existingKeys = await getExistingKeysForEventDates(
          resultsTable,
          parseInt(eventId, 10),
          resultDates,
          `CONCAT(CAST(event_number AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(run_id AS STRING), '-', IFNULL(CAST(finish_position AS STRING), 'null'))`,
        );
        toInsert = mapped.filter(r => !existingKeys.has(resultKeyFn(r)));
        console.log(`    Dedupe fallback: ${toInsert.length} new result rows.`);
      }
      await insertRows(resultsTable, toInsert, resultKeyFn);
      totalResultsInserted += toInsert.length;
    } else {
      console.log(`    No result rows for run_id=${runId}.`);
    }

    // ── Volunteers ─────────────────────────────────────────────────────────
    let volunteerRows = null;
    for (let attempt = 1; attempt <= 5; attempt += 1) {
      try {
        volunteerRows = await multiGet(
          client,
          `/v1/events/${eventId}/runs/${runId}/volunteers`,
          {},
          'Volunteers',
          'VolunteersRange',
        );
        break;
      } catch (err) {
        const status = err?.response?.status;
        const retriable =
          status === 403 ||
          status === 429 ||
          status === 500 ||
          status === 502 ||
          status === 503 ||
          status === 504;
        if (!retriable || attempt === 5) throw err;
        const waitMs = Math.max(RUN_FETCH_DELAY_MS_INT, 1000) * attempt;
        console.warn(
          `    volunteers run ${runId}: HTTP ${status} on attempt ${attempt}/5; retrying in ${Math.round(waitMs / 1000)}s...`,
        );
        console.warn(
          `    volunteers run ${runId}: applying retry pause of ${waitMs}ms (RUN_FETCH_DELAY_MS=${RUN_FETCH_DELAY_MS ?? 'unset'} => ${RUN_FETCH_DELAY_MS_INT}ms).`,
        );
        await sleep(waitMs);
      }
    }

    if (volunteerRows && volunteerRows.length > 0) {
      const datesForRosterLookup = [
        ...new Set(volunteerRows.map(v => v.EventDate).filter(Boolean)),
      ];
      const taskNameByKey = await fetchTaskNameByAthleteIdForDates(
        client,
        eventId,
        datesForRosterLookup,
      );
      const mapped = volunteerRows.map(v => {
        const roleIds = parseVolunteerRoleIds(v.volunteerRoleIds);
        return {
          roster_id: v.VolID != null ? parseInt(v.VolID, 10) : null,
          event_number:
            v.EventNumber != null ? parseInt(v.EventNumber, 10) : null,
          run_id: v.RunId != null ? parseInt(v.RunId, 10) : null,
          event_date: toDateString(v.EventDate),
          athlete_id: v.AthleteID != null ? parseInt(v.AthleteID, 10) : 2214,
          task_id: roleIds.length > 0 ? roleIds[0] : null,
          task_ids: mapVolunteerRoleIdsCsv(roleIds),
          task_name: mapVolunteerRoleNames(roleIds, taskNameByKey, v),
          first_name: v.FirstName || null,
          last_name: v.LastName || null,
        };
      });
      const valid = mapped.filter(r => r.roster_id !== null && r.event_date);
      const volunteerDates = [
        ...new Set(valid.map(r => r.event_date).filter(Boolean)),
      ];
      const volunteersDeleted = await deleteRowsForEventDates(
        volunteersTable,
        parseInt(eventId, 10),
        volunteerDates,
      );
      let toInsert = valid;
      if (!volunteersDeleted) {
        const existingKeys = await getExistingKeysForEventDates(
          volunteersTable,
          parseInt(eventId, 10),
          volunteerDates,
          `CONCAT(CAST(event_number AS STRING), '-', CAST(run_id AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(task_id AS STRING), '-', CAST(roster_id AS STRING))`,
        );
        toInsert = valid.filter(r => !existingKeys.has(volunteerKeyFn(r)));
        console.log(
          `    Dedupe fallback: ${toInsert.length} new volunteer rows.`,
        );
      }
      await insertRows(volunteersTable, toInsert, volunteerKeyFn);
      totalVolunteersInserted += toInsert.length;
    } else {
      console.log(`    No volunteer rows for run_id=${runId}.`);
    }

    if (RUN_FETCH_DELAY_MS_INT > 0 && i + 1 < runIds.length) {
      console.log(
        `    full-history: applying inter-run pause of ${RUN_FETCH_DELAY_MS_INT}ms (RUN_FETCH_DELAY_MS=${RUN_FETCH_DELAY_MS ?? 'unset'} => ${RUN_FETCH_DELAY_MS_INT}ms).`,
      );
      await sleep(RUN_FETCH_DELAY_MS_INT);
    }
  }

  console.log(
    `\n  Full-history complete: ${totalResultsInserted} results + ${totalVolunteersInserted} volunteers inserted across ${runIds.length} runs.`,
  );
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
  const client = makeAuthedClient(token, username.trim());

  console.log(`[${label}] Fetching event info (ID: ${eventId}) ...`);
  const { internalName: eventShortName, displayName: eventDisplayName } =
    await getEvent(client, eventId);
  console.log(`[${label}] Event: "${eventDisplayName}" (${eventShortName})`);
  await sleep(350);

  const useRunScopedHistory =
    SCRAPE_ALL && !SHOULD_FETCH_LATEST_ONLY && TARGET_EVENT_NUMBER_INT === null;

  if (useRunScopedHistory) {
    await processRunScopedHistory({
      client,
      eventId,
      label,
      resultsTable,
      volunteersTable,
    });
    console.log(`[${label}] Done.`);
    return;
  }

  // ── Run results ─────────────────────────────────────────────────────────
  console.log(`[${label}] Fetching run results for event ${eventId} ...`);
  const rawResults = await fetchEventResults(client, eventId, {
    latestOnly: SHOULD_FETCH_LATEST_ONLY,
    targetEventNumber: TARGET_EVENT_NUMBER_INT,
    useRunScopedHistory,
    runFetchDelayMs: RUN_FETCH_DELAY_MS_INT,
    startEventNumber: START_EVENT_NUMBER_INT,
  });
  console.log(`  Retrieved ${rawResults.length} total result rows from API.`);

  let toInsert = rawResults;

  if (!SCRAPE_ALL && TARGET_EVENT_NUMBER_INT === null) {
    const latestDate = await getLatestStoredDate(
      resultsTable,
      parseInt(eventId, 10),
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

  const resultsRefreshByDate = summarizeRawRowsByEventDate(toInsert);
  if (resultsRefreshByDate) {
    console.log(
      `  Results rows to refresh by event date: ${resultsRefreshByDate}`,
    );
  }

  if (MAX_EVENTS && TARGET_EVENT_NUMBER_INT === null) {
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

  const resultMapStartedAt = Date.now();
  const mapped = toInsert.map(mapResultRow);
  const resultMapMs = Date.now() - resultMapStartedAt;
  console.log(`  Result row mapping phase took ${resultMapMs}ms.`);

  const resultDates = [
    ...new Set(mapped.map(r => r.event_date).filter(Boolean)),
  ];

  const resultDeleteStartedAt = Date.now();
  const resultsDeleted = await deleteRowsForEventDates(
    resultsTable,
    parseInt(eventId, 10),
    resultDates,
  );
  const resultDeleteMs = Date.now() - resultDeleteStartedAt;
  console.log(`  Result delete/refresh phase took ${resultDeleteMs}ms.`);

  const resultKeyFn = r =>
    `${r.event_number}-${r.event_date}-${r.athlete_id}-${r.run_id}-${r.finish_position ?? 'null'}`;
  let mappedToInsert = mapped;

  if (!resultsDeleted) {
    const resultDedupeStartedAt = Date.now();
    const existingResultKeys = await getExistingKeysForEventDates(
      resultsTable,
      parseInt(eventId, 10),
      resultDates,
      `CONCAT(CAST(event_number AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(run_id AS STRING), '-', IFNULL(CAST(finish_position AS STRING), 'null'))`,
    );
    mappedToInsert = mapped.filter(
      r => !existingResultKeys.has(resultKeyFn(r)),
    );
    console.log(
      `  Dedupe fallback: ${mappedToInsert.length} new result rows to insert.`,
    );
    const resultDedupeMs = Date.now() - resultDedupeStartedAt;
    console.log(`  Result dedupe query/filter took ${resultDedupeMs}ms.`);
  }

  const resultInsertStartedAt = Date.now();
  await insertRows(resultsTable, mappedToInsert, resultKeyFn);
  const resultInsertMs = Date.now() - resultInsertStartedAt;
  console.log(
    `  Result insert phase took ${resultInsertMs}ms for ${mappedToInsert.length} row(s).`,
  );

  // ── Historical volunteers (processed after results) ─────────────────────
  await sleep(350);
  console.log(`[${label}] Fetching volunteer history for event ${eventId} ...`);
  const rawVolunteers = await fetchEventVolunteers(client, eventId, {
    latestOnly: SHOULD_FETCH_LATEST_ONLY,
    targetEventNumber: TARGET_EVENT_NUMBER_INT,
    useRunScopedHistory,
    runFetchDelayMs: RUN_FETCH_DELAY_MS_INT,
    startEventNumber: START_EVENT_NUMBER_INT,
  });
  console.log(
    `  Retrieved ${rawVolunteers.length} total volunteer rows from API.`,
  );

  let volunteersToInsertRaw = rawVolunteers;

  if (!SCRAPE_ALL && TARGET_EVENT_NUMBER_INT === null) {
    const latestVolunteerDate = await getLatestStoredDate(
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

  if (MAX_EVENTS && TARGET_EVENT_NUMBER_INT === null) {
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
    const datesForRosterLookup = [
      ...new Set(volunteersToInsertRaw.map(v => v.EventDate).filter(Boolean)),
    ];

    const rosterLookupStartedAt = Date.now();
    await sleep(350);
    const taskNameByKey = await fetchTaskNameByAthleteIdForDates(
      client,
      eventId,
      datesForRosterLookup,
    );
    const rosterLookupMs = Date.now() - rosterLookupStartedAt;
    console.log(
      `  Loaded ${taskNameByKey.size} volunteer roster assignments from ${datesForRosterLookup.length} date(s) in ${rosterLookupMs}ms.`,
    );

    const volunteerRows = volunteersToInsertRaw.map(v => {
      const roleIds = parseVolunteerRoleIds(v.volunteerRoleIds);

      return {
        roster_id: v.VolID != null ? parseInt(v.VolID, 10) : null,
        event_number:
          v.EventNumber != null ? parseInt(v.EventNumber, 10) : null,
        run_id: v.RunId != null ? parseInt(v.RunId, 10) : null,
        event_date: toDateString(v.EventDate),
        athlete_id: v.AthleteID != null ? parseInt(v.AthleteID, 10) : 2214,
        task_id: roleIds.length > 0 ? roleIds[0] : null,
        task_ids: mapVolunteerRoleIdsCsv(roleIds),
        task_name: mapVolunteerRoleNames(roleIds, taskNameByKey, v),
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

    const deleteStartedAt = Date.now();
    const volunteersDeleted = await deleteRowsForEventDates(
      volunteersTable,
      parseInt(eventId, 10),
      volunteerDates,
    );
    const deleteMs = Date.now() - deleteStartedAt;
    console.log(`  Volunteer delete/refresh phase took ${deleteMs}ms.`);

    const volunteerKeyFn = r =>
      `${r.event_number}-${r.run_id}-${r.event_date}-${r.athlete_id}-${r.task_id}-${r.roster_id}`;
    let volunteerRowsToInsert = volunteerRowsValid;

    if (!volunteersDeleted) {
      const dedupeStartedAt = Date.now();
      const existingVolunteerKeys = await getExistingKeysForEventDates(
        volunteersTable,
        parseInt(eventId, 10),
        volunteerDates,
        `CONCAT(CAST(event_number AS STRING), '-', CAST(run_id AS STRING), '-', CAST(event_date AS STRING), '-', CAST(athlete_id AS STRING), '-', CAST(task_id AS STRING), '-', CAST(roster_id AS STRING))`,
      );
      volunteerRowsToInsert = volunteerRowsValid.filter(
        r => !existingVolunteerKeys.has(volunteerKeyFn(r)),
      );
      const dedupeMs = Date.now() - dedupeStartedAt;
      console.log(
        `  Dedupe fallback: ${volunteerRowsToInsert.length} new volunteer rows to insert.`,
      );
      console.log(`  Volunteer dedupe query/filter took ${dedupeMs}ms.`);
    }

    const insertStartedAt = Date.now();
    await insertRows(volunteersTable, volunteerRowsToInsert, volunteerKeyFn);
    const insertMs = Date.now() - insertStartedAt;
    console.log(
      `  Volunteer insert phase took ${insertMs}ms for ${volunteerRowsToInsert.length} row(s).`,
    );
  } else {
    console.log(`  No volunteer rows matched the configured filters.`);
  }

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
    `Mode: ${TARGET_EVENT_NUMBER_INT !== null ? `target run ${TARGET_EVENT_NUMBER_INT}` : SCRAPE_ALL ? 'full scrape' : 'incremental'}${MAX_EVENTS && TARGET_EVENT_NUMBER_INT === null ? `, max ${MAX_EVENTS} event dates` : ''}`,
  );
  console.log(
    `Run fetch pause: RUN_FETCH_DELAY_MS=${RUN_FETCH_DELAY_MS ?? 'unset'} (effective ${RUN_FETCH_DELAY_MS_INT}ms).`,
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
  if (START_EVENT_NUMBER_INT !== null) {
    console.log(
      `API optimization: START_EVENT_NUMBER=${START_EVENT_NUMBER_INT} (full run-scoped fetch starts at this RunId).`,
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
