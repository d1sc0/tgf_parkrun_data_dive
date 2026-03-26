require('dotenv').config();
const path = require('path');
const { BigQuery } = require('@google-cloud/bigquery');

async function main() {
  const bq = new BigQuery({
    projectId: process.env.GCP_PROJECT_ID,
    keyFilename: path.resolve(process.env.GOOGLE_CREDENTIALS_PATH),
  });

  const q = `
    SELECT
      athlete_id,
      highest_parkrun_club_membership_number,
      highest_volunteer_club_membership_number,
      highest_run_total,
      highest_volunteer_count,
      genuine_pb_count
    FROM \`${process.env.GCP_PROJECT_ID}.parkrun_data._06_results_athlete_summary\`
    ORDER BY appearances_in_results DESC
    LIMIT 3
  `;

  const qj = `
    SELECT
      athlete_id,
      highest_parkrun_club_membership_number,
      highest_volunteer_club_membership_number,
      highest_run_total,
      highest_volunteer_count,
      genuine_pb_count
    FROM \`${process.env.GCP_PROJECT_ID}.parkrun_data._07_junior_results_athlete_summary\`
    ORDER BY appearances_in_junior_results DESC
    LIMIT 3
  `;

  const [rows1] = await bq.query({ query: q });
  const [rows2] = await bq.query({ query: qj });

  console.log('06 sample:', JSON.stringify(rows1, null, 2));
  console.log('07 sample:', JSON.stringify(rows2, null, 2));
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
