require('dotenv').config();

const fs = require('fs');
const path = require('path');
const axios = require('axios');
const qs = require('querystring');
const { BigQuery } = require('@google-cloud/bigquery');

const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_USER_AGENT = 'parkrun/1.2.7 CFNetwork/1121.2.2 Darwin/19.3.0';
const PARKRUN_VERSION = '2.0.1';
const RETRY_DELAY_MS = 100_000;

const {
  GCP_PROJECT_ID,
  GOOGLE_CREDENTIALS_PATH,
  BIGQUERY_DATASET_ID = 'parkrun_data',
  BIGQUERY_RESULTS_TABLE = 'results',
  BIGQUERY_VOLUNTEERS_TABLE = 'volunteers',
  BIGQUERY_JUNIOR_RESULTS_TABLE = 'junior_results',
  BIGQUERY_JUNIOR_VOLUNTEERS_TABLE = 'junior_volunteers',
  PARKRUN_CLIENT_ID,
  PARKRUN_CLIENT_SECRET,
  PARKRUN_USERNAME,
  PARKRUN_PASSWORD,
  PARKRUN_EVENT_ID,
  JUNIOR_USERNAME,
  JUNIOR_PASSWORD,
  JUNIOR_EVENT_ID,
} = process.env;

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    if (argv[i].startsWith('--')) {
      const key = argv[i].slice(2);
      const value =
        argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : 'true';
      out[key] = value;
      if (value !== 'true') i += 1;
    }
  }
  return out;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function is403(err) {
  return err?.response?.status === 403;
}

function getTotalFromContentRange(payload, defaultValue) {
  const contentRange = payload?.['Content-Range'];
  if (!contentRange || typeof contentRange !== 'object') return defaultValue;

  for (const key of Object.keys(contentRange)) {
    const first = Array.isArray(contentRange[key])
      ? contentRange[key][0]
      : null;
    const max = parseInt(first?.max, 10);
    if (Number.isFinite(max)) return max;
  }
  return defaultValue;
}

function getFirstArray(dataObj) {
  if (!dataObj || typeof dataObj !== 'object') return [];
  for (const key of Object.keys(dataObj)) {
    if (Array.isArray(dataObj[key])) return dataObj[key];
  }
  return [];
}

async function getAccessTokenWithRetry(username, password) {
  const body = qs.stringify({
    username: username.trim(),
    password: password.trim(),
    scope: 'app',
    grant_type: 'password',
  });

  try {
    const res = await axios.post(`${PARKRUN_API_BASE}/user_auth.php`, body, {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'User-Agent': PARKRUN_USER_AGENT,
        'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
      },
      auth: {
        username: PARKRUN_CLIENT_ID,
        password: PARKRUN_CLIENT_SECRET,
      },
    });
    if (!res.data?.access_token) {
      throw new Error('Auth failed: missing access_token');
    }
    return res.data.access_token;
  } catch (err) {
    if (!is403(err)) throw err;

    console.warn('403 during auth. Waiting 100s before retrying auth...');
    await sleep(RETRY_DELAY_MS);

    const retryRes = await axios.post(
      `${PARKRUN_API_BASE}/user_auth.php`,
      body,
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': PARKRUN_USER_AGENT,
          'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
        },
        auth: {
          username: PARKRUN_CLIENT_ID,
          password: PARKRUN_CLIENT_SECRET,
        },
      },
    );

    if (!retryRes.data?.access_token) {
      throw new Error('Auth retry failed: missing access_token');
    }
    return retryRes.data.access_token;
  }
}

async function fetchRunsPageWithRetry(clientRef, eventId, params, authConfig) {
  try {
    return await clientRef.current.get(`/v1/events/${eventId}/runs`, {
      params,
    });
  } catch (err) {
    if (!is403(err)) throw err;

    console.warn(
      '403 while fetching runs. Waiting 100s, re-authenticating, and retrying...',
    );
    await sleep(RETRY_DELAY_MS);
    const newToken = await getAccessTokenWithRetry(
      authConfig.username,
      authConfig.password,
    );
    clientRef.current = createApiClient(newToken);
    return clientRef.current.get(`/v1/events/${eventId}/runs`, { params });
  }
}

function createApiClient(token) {
  return axios.create({
    baseURL: PARKRUN_API_BASE,
    headers: {
      'User-Agent': PARKRUN_USER_AGENT,
      'X-Powered-By': `parkrun.js/${PARKRUN_VERSION} (https://parkrun.js.org/)`,
    },
    params: {
      access_token: token,
      scope: 'app',
      expandedDetails: true,
    },
  });
}

async function fetchAllRuns(clientRef, eventId, authConfig) {
  const firstRes = await fetchRunsPageWithRetry(
    clientRef,
    eventId,
    {
      eventNumber: eventId,
      limit: 100,
      offset: 0,
    },
    authConfig,
  );

  let rows = getFirstArray(firstRes.data?.data);
  const total = getTotalFromContentRange(firstRes.data, rows.length);

  for (let offset = rows.length; offset < total; offset += 100) {
    const res = await fetchRunsPageWithRetry(
      clientRef,
      eventId,
      {
        eventNumber: eventId,
        limit: 100,
        offset,
      },
      authConfig,
    );
    rows = rows.concat(getFirstArray(res.data?.data));
  }

  return rows;
}

async function queryBqCounts(
  bigquery,
  projectId,
  datasetId,
  resultsTable,
  volunteersTable,
  eventId,
) {
  const query = `
WITH result_counts AS (
  SELECT
    run_id,
    COUNT(*) AS row_count
  FROM \`${projectId}.${datasetId}.${resultsTable}\`
  WHERE CAST(event_number AS STRING) = @eventNumber
  GROUP BY run_id
),
volunteer_counts AS (
  SELECT
    run_id,
    COUNT(*) AS volunteer_row_count
  FROM \`${projectId}.${datasetId}.${volunteersTable}\`
  WHERE CAST(event_number AS STRING) = @eventNumber
  GROUP BY run_id
)
SELECT
  r.run_id,
  r.row_count,
  COALESCE(v.volunteer_row_count, 0) AS volunteer_row_count
FROM result_counts r
LEFT JOIN volunteer_counts v
  ON v.run_id = r.run_id
ORDER BY r.run_id DESC
`;

  const [rows] = await bigquery.query({
    query,
    params: { eventNumber: String(eventId) },
  });

  return rows.map(r => ({
    run_id: String(r.run_id),
    row_count: String(r.row_count),
    volunteer_row_count: String(r.volunteer_row_count),
  }));
}

function compareCounts(bqRows, pageRows) {
  const pageMap = new Map(
    pageRows.map(r => [
      String(r.RunId),
      {
        run_id: String(r.RunId),
        row_count: String(r.NumberRunners),
        volunteer_row_count: String(r.NumberOfVolunteers),
      },
    ]),
  );

  const bqMap = new Map(bqRows.map(r => [String(r.run_id), r]));

  const diffs = [];
  const missingOnPage = [];
  const missingInBq = [];

  for (const bqRow of bqRows) {
    const pageRow = pageMap.get(String(bqRow.run_id));
    if (!pageRow) {
      missingOnPage.push(String(bqRow.run_id));
      continue;
    }

    const rowCountMismatch =
      String(bqRow.row_count) !== String(pageRow.row_count);
    const volunteerMismatch =
      String(bqRow.volunteer_row_count) !== String(pageRow.volunteer_row_count);

    if (rowCountMismatch || volunteerMismatch) {
      diffs.push({
        run_id: String(bqRow.run_id),
        bq_row_count: String(bqRow.row_count),
        page_row_count: String(pageRow.row_count),
        bq_volunteer_row_count: String(bqRow.volunteer_row_count),
        page_volunteer_row_count: String(pageRow.volunteer_row_count),
      });
    }
  }

  for (const pageRow of pageRows) {
    const runId = String(pageRow.RunId);
    if (!bqMap.has(runId)) {
      missingInBq.push(runId);
    }
  }

  return { diffs, missingOnPage, missingInBq };
}

function sortByRunId(rows) {
  return [...rows].sort((a, b) => Number(b.run_id) - Number(a.run_id));
}

function toMarkdownTable(headers, rows) {
  if (!rows || rows.length === 0) return '(none)';
  const headerRow = `| ${headers.join(' | ')} |`;
  const separatorRow = `| ${headers.map(() => '---').join(' | ')} |`;
  const bodyRows = rows.map(row => `| ${row.join(' | ')} |`);
  return [headerRow, separatorRow, ...bodyRows].join('\n');
}

function buildMissingRows(summary) {
  return [
    ...summary.missing_on_page.map(runId => ['missing_on_page', runId]),
    ...summary.missing_in_bq.map(runId => ['missing_in_bq', runId]),
  ].sort((a, b) => Number(b[1]) - Number(a[1]));
}

function buildFinishersDiffRows(summary) {
  return sortByRunId(
    summary.diffs.filter(
      d => String(d.bq_row_count) !== String(d.page_row_count),
    ),
  ).map(d => [d.run_id, d.bq_row_count, d.page_row_count]);
}

function buildVolunteersDiffRows(summary) {
  return sortByRunId(
    summary.diffs.filter(
      d =>
        String(d.bq_volunteer_row_count) !== String(d.page_volunteer_row_count),
    ),
  ).map(d => [d.run_id, d.bq_volunteer_row_count, d.page_volunteer_row_count]);
}

function buildSectionReport(summary) {
  const missingRows = buildMissingRows(summary);
  const finishersRows = buildFinishersDiffRows(summary);
  const volunteersRows = buildVolunteersDiffRows(summary);

  return [
    `## ${summary.label.toUpperCase()} (event_id=${summary.event_id})`,
    '',
    'Summary of missing events',
    `- missing_on_page_count: ${summary.missing_on_page_count}`,
    `- missing_in_bq_count: ${summary.missing_in_bq_count}`,
    '',
    toMarkdownTable(['type', 'run_id'], missingRows),
    '',
    'Finishers count differences',
    toMarkdownTable(
      ['run_id', 'bq_finishers', 'page_finishers'],
      finishersRows,
    ),
    '',
    'Volunteers count differences',
    toMarkdownTable(
      ['run_id', 'bq_volunteers', 'page_volunteers'],
      volunteersRows,
    ),
    '',
  ].join('\n');
}

function buildTextReport(summary) {
  const sections = [
    '# Parkrun vs BigQuery Count Comparison Report',
    '',
    `Generated: ${new Date().toISOString()}`,
    '',
  ];

  if (summary.comparisons.parkrun) {
    sections.push(buildSectionReport(summary.comparisons.parkrun));
  }

  if (summary.comparisons.junior) {
    sections.push(buildSectionReport(summary.comparisons.junior));
  } else {
    sections.push('## JUNIOR');
    sections.push('');
    sections.push('(junior comparison not available in this run)');
    sections.push('');
  }

  return sections.join('\n');
}

async function runComparison(bigquery, config) {
  console.log(`\n=== ${config.label} ===`);
  console.log(`Querying BigQuery counts for event ${config.eventId}...`);

  const bqRows = await queryBqCounts(
    bigquery,
    GCP_PROJECT_ID,
    config.datasetId,
    config.resultsTable,
    config.volunteersTable,
    config.eventId,
  );

  console.log(`Fetched ${bqRows.length} run(s) from BigQuery.`);
  console.log('Authenticating with Parkrun API...');

  const token = await getAccessTokenWithRetry(
    config.auth.username,
    config.auth.password,
  );
  const clientRef = { current: createApiClient(token) };

  console.log('Fetching event history runs from Parkrun API...');
  const pageRows = await fetchAllRuns(clientRef, config.eventId, config.auth);
  console.log(`Fetched ${pageRows.length} run(s) from Parkrun API.`);

  const result = compareCounts(bqRows, pageRows);
  const summary = {
    label: config.label,
    event_id: String(config.eventId),
    bq_rows: bqRows.length,
    page_rows: pageRows.length,
    missing_on_page_count: result.missingOnPage.length,
    missing_in_bq_count: result.missingInBq.length,
    diff_count: result.diffs.length,
    missing_on_page: result.missingOnPage,
    missing_in_bq: result.missingInBq,
    diffs: result.diffs,
  };

  console.log(`missing_on_page: ${summary.missing_on_page_count}`);
  console.log(`missing_in_bq: ${summary.missing_in_bq_count}`);
  console.log(`diff_count: ${summary.diff_count}`);

  if (summary.diff_count > 0) {
    console.log(JSON.stringify(summary.diffs, null, 2));
  }

  return summary;
}

async function main() {
  const args = parseArgs(process.argv);

  const eventId = args['event-id'] || PARKRUN_EVENT_ID;
  const juniorEventId = args['junior-event-id'] || JUNIOR_EVENT_ID;
  const datasetId = args['dataset'] || BIGQUERY_DATASET_ID;
  const resultsTable = args['results-table'] || BIGQUERY_RESULTS_TABLE;
  const volunteersTable = args['volunteers-table'] || BIGQUERY_VOLUNTEERS_TABLE;
  const juniorResultsTable =
    args['junior-results-table'] || BIGQUERY_JUNIOR_RESULTS_TABLE;
  const juniorVolunteersTable =
    args['junior-volunteers-table'] || BIGQUERY_JUNIOR_VOLUNTEERS_TABLE;
  const outputPath = args.out || null;
  const textOutputPath = args['text-out'] || 'compare-bq-report.txt';
  const includeJunior = String(args['include-junior'] || 'true') !== 'false';

  if (!eventId)
    throw new Error(
      'Missing event id. Set PARKRUN_EVENT_ID or pass --event-id.',
    );
  if (!GCP_PROJECT_ID) throw new Error('Missing GCP_PROJECT_ID.');
  if (!GOOGLE_CREDENTIALS_PATH)
    throw new Error('Missing GOOGLE_CREDENTIALS_PATH.');
  if (!PARKRUN_CLIENT_ID || !PARKRUN_CLIENT_SECRET) {
    throw new Error('Missing PARKRUN_CLIENT_ID/PARKRUN_CLIENT_SECRET.');
  }
  if (!PARKRUN_USERNAME || !PARKRUN_PASSWORD) {
    throw new Error('Missing PARKRUN_USERNAME/PARKRUN_PASSWORD.');
  }

  const bigquery = new BigQuery({
    projectId: GCP_PROJECT_ID,
    keyFilename: path.resolve(GOOGLE_CREDENTIALS_PATH),
  });

  const summaries = {};

  summaries.parkrun = await runComparison(bigquery, {
    label: 'parkrun',
    eventId,
    datasetId,
    resultsTable,
    volunteersTable,
    auth: {
      username: PARKRUN_USERNAME,
      password: PARKRUN_PASSWORD,
    },
  });

  if (includeJunior) {
    if (!juniorEventId) {
      console.warn('Skipping junior comparison: missing JUNIOR_EVENT_ID.');
    } else if (!JUNIOR_USERNAME || !JUNIOR_PASSWORD) {
      console.warn(
        'Skipping junior comparison: missing JUNIOR_USERNAME/JUNIOR_PASSWORD.',
      );
    } else {
      summaries.junior = await runComparison(bigquery, {
        label: 'junior',
        eventId: juniorEventId,
        datasetId,
        resultsTable: juniorResultsTable,
        volunteersTable: juniorVolunteersTable,
        auth: {
          username: JUNIOR_USERNAME,
          password: JUNIOR_PASSWORD,
        },
      });
    }
  }

  const summary = {
    comparisons: summaries,
  };

  const textReport = buildTextReport(summary);
  const resolvedTextPath = path.resolve(textOutputPath);
  fs.writeFileSync(resolvedTextPath, textReport, 'utf8');
  console.log(`Wrote text report to ${resolvedTextPath}`);

  if (outputPath) {
    const resolved = path.resolve(outputPath);
    fs.writeFileSync(resolved, JSON.stringify(summary, null, 2));
    console.log(`Wrote comparison output to ${resolved}`);
  }
}

main().catch(err => {
  const status = err?.response?.status;
  const body = err?.response?.data;
  if (status) {
    console.error(
      `Failed with HTTP ${status}:`,
      typeof body === 'string' ? body : JSON.stringify(body),
    );
  } else {
    console.error('Failed:', err?.message || err);
  }
  process.exit(1);
});
