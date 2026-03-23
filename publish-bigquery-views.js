require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

const projectId = process.env.GCP_PROJECT_ID;
const keyFilename = path.resolve(
  process.env.GOOGLE_CREDENTIALS_PATH || 'service-account-key.json',
);
const viewsDatasetId =
  process.env.BIGQUERY_VIEWS_DATASET_ID ||
  process.env.BIGQUERY_DATASET_ID ||
  'parkrun_data';
const sqlDir = path.resolve(__dirname, 'sql', 'bigquery');

function toViewId(filename) {
  const base = path.basename(filename, '.sql').toLowerCase();
  const sanitized = base.replace(/[^a-z0-9_]/g, '_');
  return /^[a-z_]/.test(sanitized) ? sanitized : `v_${sanitized}`;
}

async function ensureDataset(bigquery, datasetId) {
  const dataset = bigquery.dataset(datasetId);
  const [exists] = await dataset.exists();
  if (!exists) {
    await dataset.create();
    console.log(`Created dataset: ${datasetId}`);
  }
  return dataset;
}

async function publishView(dataset, sqlFilePath) {
  const sql = fs.readFileSync(sqlFilePath, 'utf8').trim();
  if (!sql) {
    throw new Error(`SQL file is empty: ${sqlFilePath}`);
  }

  const viewId = toViewId(path.basename(sqlFilePath));
  const table = dataset.table(viewId);
  const [exists] = await table.exists();

  if (!exists) {
    await table.create({
      view: {
        query: sql,
        useLegacySql: false,
      },
    });
    console.log(`Created view: ${dataset.id}.${viewId}`);
    return;
  }

  await table.setMetadata({
    view: {
      query: sql,
      useLegacySql: false,
    },
  });
  console.log(`Updated view: ${dataset.id}.${viewId}`);
}

async function main() {
  if (!projectId) {
    throw new Error('Missing GCP_PROJECT_ID in environment.');
  }

  if (!process.env.GOOGLE_CREDENTIALS_PATH) {
    console.warn(
      'GOOGLE_CREDENTIALS_PATH not set; using service-account-key.json in workspace root.',
    );
  }

  if (!fs.existsSync(sqlDir)) {
    throw new Error(`SQL directory not found: ${sqlDir}`);
  }

  const sqlFiles = fs
    .readdirSync(sqlDir)
    .filter(name => name.toLowerCase().endsWith('.sql'))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

  if (sqlFiles.length === 0) {
    throw new Error(`No .sql files found in: ${sqlDir}`);
  }

  const bigquery = new BigQuery({ projectId, keyFilename });
  const dataset = await ensureDataset(bigquery, viewsDatasetId);

  console.log(
    `Publishing ${sqlFiles.length} SQL views from ${sqlDir} into ${projectId}.${viewsDatasetId} ...`,
  );

  for (const filename of sqlFiles) {
    const fullPath = path.join(sqlDir, filename);
    await publishView(dataset, fullPath);
  }

  console.log('Done.');
}

main().catch(err => {
  console.error('View publish failed:', err && (err.message || err));
  process.exit(1);
});
