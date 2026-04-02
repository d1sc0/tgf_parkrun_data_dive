require('dotenv').config();

const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

const projectId = process.env.GCP_PROJECT_ID;
const datasetId = process.env.BIGQUERY_DATASET_ID || 'parkrun_data';
const resultsTable = process.env.BIGQUERY_RESULTS_TABLE || 'results';
const weatherTable = process.env.BIGQUERY_WEATHER_TABLE || 'event_weather';
const keyFilename =
  process.env.GOOGLE_CREDENTIALS_PATH ||
  process.env.GOOGLE_APPLICATION_CREDENTIALS;

const weatherLatitude = Number(process.env.WEATHER_LATITUDE || 50.7123);
const weatherLongitude = Number(process.env.WEATHER_LONGITUDE || -2.4651);
const weatherHourUtc = Number(process.env.WEATHER_HOUR_UTC || 9);

const bq = new BigQuery({
  projectId,
  ...(keyFilename ? { keyFilename: path.resolve(keyFilename) } : {}),
});

function normalizeDate(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().split('T')[0];
  if (typeof value === 'object' && value.value) return value.value;
  return String(value);
}

function getHourlyIndex(times) {
  if (!Array.isArray(times)) return -1;
  const target = `T${String(weatherHourUtc).padStart(2, '0')}:00`;
  return times.findIndex(t => String(t).includes(target));
}

async function ensureWeatherTable() {
  const dataset = bq.dataset(datasetId);
  const table = dataset.table(weatherTable);

  const [exists] = await table.exists();
  if (exists) return;

  await dataset.createTable(weatherTable, {
    schema: [
      { name: 'run_id', type: 'INTEGER', mode: 'REQUIRED' },
      { name: 'event_date', type: 'DATE', mode: 'REQUIRED' },
      { name: 'temp_c', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'weather_code', type: 'INTEGER', mode: 'NULLABLE' },
      { name: 'wind_mph', type: 'FLOAT64', mode: 'NULLABLE' },
      { name: 'fetched_at', type: 'TIMESTAMP', mode: 'REQUIRED' },
      { name: 'source', type: 'STRING', mode: 'REQUIRED' },
    ],
  });

  console.log(`Created table: ${datasetId}.${weatherTable}`);
}

async function getTargetRuns(mode) {
  const latestClause = mode === 'latest' ? 'LIMIT 1' : '';
  const query = `
    SELECT
      run_id,
      MAX(event_date) AS event_date
    FROM \`${projectId}.${datasetId}.${resultsTable}\`
    WHERE run_id IS NOT NULL
      AND event_date IS NOT NULL
    GROUP BY run_id
    ORDER BY event_date DESC, run_id DESC
    ${latestClause}
  `;

  const [rows] = await bq.query({ query, useLegacySql: false });
  return rows.map(r => ({
    run_id: Number(r.run_id?.value || r.run_id),
    event_date: normalizeDate(r.event_date),
  }));
}

async function fetchWeatherForDate(eventDate) {
  const url =
    `https://archive-api.open-meteo.com/v1/archive?latitude=${weatherLatitude}` +
    `&longitude=${weatherLongitude}` +
    `&start_date=${eventDate}` +
    `&end_date=${eventDate}` +
    `&hourly=temperature_2m,weather_code,wind_speed_10m` +
    `&wind_speed_unit=mph&timezone=GMT`;

  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Open-Meteo responded ${res.status} for date ${eventDate}`);
  }

  const data = await res.json();
  const hourly = data?.hourly;
  if (!hourly || !Array.isArray(hourly.time)) {
    return null;
  }

  let idx = getHourlyIndex(hourly.time);
  if (idx < 0) idx = 0;

  return {
    temp_c:
      Array.isArray(hourly.temperature_2m) && hourly.temperature_2m[idx] != null
        ? Number(hourly.temperature_2m[idx])
        : null,
    weather_code:
      Array.isArray(hourly.weather_code) && hourly.weather_code[idx] != null
        ? Number(hourly.weather_code[idx])
        : null,
    wind_mph:
      Array.isArray(hourly.wind_speed_10m) && hourly.wind_speed_10m[idx] != null
        ? Number(hourly.wind_speed_10m[idx])
        : null,
  };
}

async function upsertWeather(runId, eventDate, weather) {
  const query = `
    MERGE \`${projectId}.${datasetId}.${weatherTable}\` t
    USING (
      SELECT
        @runId AS run_id,
        DATE(@eventDate) AS event_date,
        @temp_c AS temp_c,
        @weather_code AS weather_code,
        @wind_mph AS wind_mph,
        CURRENT_TIMESTAMP() AS fetched_at,
        'open-meteo-archive' AS source
    ) s
    ON t.run_id = s.run_id
    WHEN MATCHED THEN
      UPDATE SET
        event_date = s.event_date,
        temp_c = s.temp_c,
        weather_code = s.weather_code,
        wind_mph = s.wind_mph,
        fetched_at = s.fetched_at,
        source = s.source
    WHEN NOT MATCHED THEN
      INSERT (run_id, event_date, temp_c, weather_code, wind_mph, fetched_at, source)
      VALUES (s.run_id, s.event_date, s.temp_c, s.weather_code, s.wind_mph, s.fetched_at, s.source)
  `;

  await bq.query({
    query,
    params: {
      runId,
      eventDate,
      temp_c: weather?.temp_c ?? null,
      weather_code: weather?.weather_code ?? null,
      wind_mph: weather?.wind_mph ?? null,
    },
    useLegacySql: false,
  });
}

async function main() {
  const mode = process.argv.includes('--latest') ? 'latest' : 'backfill';

  if (!projectId) throw new Error('Missing GCP_PROJECT_ID in environment.');

  await ensureWeatherTable();

  const runs = await getTargetRuns(mode);
  if (runs.length === 0) {
    console.log('No runs found in results table; nothing to sync.');
    return;
  }

  console.log(`Sync mode: ${mode}. Processing ${runs.length} run(s).`);

  let success = 0;
  let failed = 0;

  for (let i = 0; i < runs.length; i += 1) {
    const row = runs[i];
    try {
      const weather = await fetchWeatherForDate(row.event_date);
      await upsertWeather(row.run_id, row.event_date, weather);
      success += 1;
    } catch (error) {
      failed += 1;
      console.error(
        `Failed weather sync for run_id=${row.run_id}, date=${row.event_date}:`,
        error.message || error,
      );
    }

    if ((i + 1) % 25 === 0 || i === runs.length - 1) {
      console.log(
        `Progress ${i + 1}/${runs.length} (success=${success}, failed=${failed})`,
      );
    }
  }

  if (failed > 0) {
    throw new Error(
      `Weather sync completed with failures: success=${success}, failed=${failed}`,
    );
  }

  console.log(`Weather sync completed successfully. Rows upserted: ${success}`);
}

main().catch(err => {
  console.error('Weather sync failed:', err.message || err);
  process.exit(1);
});
