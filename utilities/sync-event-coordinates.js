/**
 * Sync Event Coordinates Utility
 * Fetches parkrun event coordinates from the official events.json and loads them into BigQuery.
 * This eliminates the need to fetch events.json on every dashboard page load.
 */

require('dotenv').config();
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const fs = require('fs');
const os = require('os');

const projectId = process.env.GCP_PROJECT_ID;
const datasetId = process.env.BIGQUERY_DATASET_ID || 'parkrun_data';
const tableId = 'event_coordinates';
const keyFilename =
  process.env.GOOGLE_CREDENTIALS_PATH ||
  process.env.GOOGLE_APPLICATION_CREDENTIALS;

const bq = new BigQuery({
  projectId,
  ...(keyFilename ? { keyFilename: path.resolve(keyFilename) } : {}),
});

async function fetchEventCoordinates() {
  try {
    console.log('Fetching event coordinates from parkrun events.json...');
    const response = await fetch('https://images.parkrun.com/events.json');
    const data = await response.json();

    const coordinates = data.events.features
      .filter(feature => feature.geometry && feature.properties)
      .map(feature => ({
        event_name: feature.properties.eventname || '',
        event_long_name: feature.properties.EventLongName || '',
        latitude: feature.geometry.coordinates[1],
        longitude: feature.geometry.coordinates[0],
        country: feature.properties.CountryCode || '',
        last_updated: new Date().toISOString(),
      }))
      .filter(item => item.event_name.trim() !== '');

    console.log(
      `Fetched ${coordinates.length} event coordinates. Loading into BigQuery...`,
    );
    return coordinates;
  } catch (error) {
    console.error('Failed to fetch event coordinates:', error);
    throw error;
  }
}

async function createTableIfNotExists() {
  const dataset = bq.dataset(datasetId);
  const table = dataset.table(tableId);

  const [exists] = await table.exists();
  if (exists) {
    console.log(`Table ${datasetId}.${tableId} already exists.`);
    return;
  }

  console.log(`Creating table ${datasetId}.${tableId}...`);
  const schema = [
    { name: 'event_name', type: 'STRING', mode: 'REQUIRED' },
    { name: 'event_long_name', type: 'STRING', mode: 'NULLABLE' },
    { name: 'latitude', type: 'FLOAT64', mode: 'REQUIRED' },
    { name: 'longitude', type: 'FLOAT64', mode: 'REQUIRED' },
    { name: 'country', type: 'STRING', mode: 'NULLABLE' },
    { name: 'last_updated', type: 'TIMESTAMP', mode: 'REQUIRED' },
  ];

  await dataset.createTable(tableId, { schema });
  console.log(`Table created successfully.`);
}

async function loadCoordinatesToBigQuery(coordinates) {
  const dataset = bq.dataset(datasetId);
  const table = dataset.table(tableId);

  const tempFilePath = path.join(
    os.tmpdir(),
    `event-coordinates-${Date.now()}.jsonl`,
  );

  console.log(`Preparing ${coordinates.length} rows for load job...`);
  fs.writeFileSync(
    tempFilePath,
    coordinates.map(row => JSON.stringify(row)).join('\n'),
    'utf8',
  );

  console.log(
    `Loading rows into ${datasetId}.${tableId} using WRITE_TRUNCATE...`,
  );
  try {
    await table.load(tempFilePath, {
      sourceFormat: 'NEWLINE_DELIMITED_JSON',
      writeDisposition: 'WRITE_TRUNCATE',
      schema: {
        fields: [
          { name: 'event_name', type: 'STRING', mode: 'REQUIRED' },
          { name: 'event_long_name', type: 'STRING', mode: 'NULLABLE' },
          { name: 'latitude', type: 'FLOAT64', mode: 'REQUIRED' },
          { name: 'longitude', type: 'FLOAT64', mode: 'REQUIRED' },
          { name: 'country', type: 'STRING', mode: 'NULLABLE' },
          { name: 'last_updated', type: 'TIMESTAMP', mode: 'REQUIRED' },
        ],
      },
    });
  } catch (error) {
    console.error('Load job failed:', error);
    throw error;
  } finally {
    try {
      fs.unlinkSync(tempFilePath);
    } catch (cleanupError) {
      console.warn(`Could not remove temp file ${tempFilePath}:`, cleanupError);
    }
  }

  console.log(`Successfully loaded ${coordinates.length} event coordinates.`);
}

async function main() {
  try {
    await createTableIfNotExists();
    const coordinates = await fetchEventCoordinates();
    await loadCoordinatesToBigQuery(coordinates);
    console.log('✅ Event coordinates sync completed successfully.');
  } catch (error) {
    console.error('❌ Event coordinates sync failed:', error);
    process.exit(1);
  }
}

main();
