require('dotenv').config();

const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

const projectId = process.env.GCP_PROJECT_ID;
const keyFilename = path.resolve(process.env.GOOGLE_CREDENTIALS_PATH);
const datasetId = process.env.BIGQUERY_DATASET_ID || 'parkrun_data';

const resultsSchema = [
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

const volunteersSchema = [
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

const tableConfigs = [
  [process.env.BIGQUERY_RESULTS_TABLE || 'results', resultsSchema],
  [process.env.BIGQUERY_VOLUNTEERS_TABLE || 'volunteers', volunteersSchema],
  [
    process.env.BIGQUERY_JUNIOR_RESULTS_TABLE || 'junior_results',
    resultsSchema,
  ],
  [
    process.env.BIGQUERY_JUNIOR_VOLUNTEERS_TABLE || 'junior_volunteers',
    volunteersSchema,
  ],
];

async function main() {
  const bigquery = new BigQuery({ projectId, keyFilename });
  const dataset = bigquery.dataset(datasetId);

  const [datasetExists] = await dataset.exists();
  if (!datasetExists) {
    await dataset.create();
    console.log(`Created dataset: ${datasetId}`);
  } else {
    console.log(`Dataset exists: ${datasetId}`);
  }

  for (const [tableId, schema] of tableConfigs) {
    const table = dataset.table(tableId);
    const [exists] = await table.exists();
    if (!exists) {
      await table.create({ schema });
      console.log(`Created table: ${tableId}`);
    } else {
      console.log(`Table exists: ${tableId}`);
    }
  }
}

main().catch(err => {
  console.error('Setup failed:', err.message || err);
  process.exit(1);
});
