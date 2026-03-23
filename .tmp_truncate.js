require('dotenv').config();

const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

const projectId = process.env.GCP_PROJECT_ID;
const keyFilename = path.resolve(process.env.GOOGLE_CREDENTIALS_PATH);
const datasetId = process.env.BIGQUERY_DATASET_ID || 'parkrun_data';

const tableNames = [
  process.env.BIGQUERY_RESULTS_TABLE || 'results',
  process.env.BIGQUERY_VOLUNTEERS_TABLE || 'volunteers',
  process.env.BIGQUERY_JUNIOR_RESULTS_TABLE || 'junior_results',
  process.env.BIGQUERY_JUNIOR_VOLUNTEERS_TABLE || 'junior_volunteers',
];

async function main() {
  const bigquery = new BigQuery({ projectId, keyFilename });

  for (const tableName of tableNames) {
    try {
      await bigquery.query({
        query: `DELETE FROM \`${projectId}.${datasetId}.${tableName}\` WHERE 1=1`,
      });
      console.log(`✓ Truncated ${tableName}`);
    } catch (err) {
      const msg = String(err?.message || err);
      if (msg.includes('streaming buffer')) {
        console.log(`⏳ ${tableName} has streaming buffer lock, will retry...`);
      } else {
        console.error(`✗ ${tableName}: ${msg}`);
      }
    }
  }
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
