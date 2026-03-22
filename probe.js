require('dotenv').config();
const axios = require('axios');
const qs = require('querystring');

const PARKRUN_API_BASE = 'https://api.parkrun.com';
const PARKRUN_AUTH = [
  'PARKRUN_CLIENT_ID_REDACTED',
  'PARKRUN_CLIENT_SECRET_REDACTED',
];
const UA = 'parkrun/1.2.7 CFNetwork/1121.2.2 Darwin/19.3.0';

async function main() {
  const body = qs.stringify({
    username: process.env.PARKRUN_USERNAME.trim(),
    password: process.env.PARKRUN_PASSWORD.trim(),
    scope: 'app',
    grant_type: 'password',
  });
  const authRes = await axios.post(PARKRUN_API_BASE + '/user_auth.php', body, {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'User-Agent': UA,
    },
    auth: { username: PARKRUN_AUTH[0], password: PARKRUN_AUTH[1] },
  });
  const token = authRes.data.access_token;
  const client = axios.create({
    baseURL: PARKRUN_API_BASE,
    params: { access_token: token, scope: 'app', expandedDetails: true },
  });

  // Skip roster probe to avoid 403 rate limit issues
  // const rosterRes = await client.get('/v1/events/2927/rosters');
  // Rosters not probed
  console.log('Roster probe skipped.');

  // Probe results with various filters
  const candidates = [
    { eventNumber: 2927, limit: 2, offset: 0 },
    { partnerId: 2927, eventDate: '2026-03-21', limit: 2, offset: 0 },
    { partnerId: 2927, eventDate: '20260321', limit: 2, offset: 0 },
    { athleteId: 633637, limit: 2, offset: 0 },
  ];
  for (const params of candidates) {
    try {
      const resultsRes = await client.get('/v1/results', { params });
      const results =
        (resultsRes.data &&
          resultsRes.data.data &&
          resultsRes.data.data.Results) ||
        [];
      const range = resultsRes.data && resultsRes.data['Content-Range'];
      console.log('Params:', JSON.stringify(params));
      console.log('Results range:', JSON.stringify(range, null, 2));
      if (results.length > 0)
        console.log('Result sample:', JSON.stringify(results[0], null, 2));
    } catch (e) {
      console.error(
        'Results error (params=' + JSON.stringify(params) + '):',
        e.response ? JSON.stringify(e.response.data, null, 2) : e.message,
      );
    }
  }

  // ── Historical volunteer / roster endpoint probes ────────────────────────
  // A known past event date to test against
  const PAST_DATE = '2026-03-15';
  const PAST_DATE_COMPACT = '20260315';
  const EVENT_ID = '2927';

  const rosterProbes = [
    // Date filter variations on the rosters endpoint
    { url: `/v1/events/${EVENT_ID}/rosters`, params: { eventDate: PAST_DATE } },
    {
      url: `/v1/events/${EVENT_ID}/rosters`,
      params: { eventDate: PAST_DATE_COMPACT },
    },
    {
      url: `/v1/events/${EVENT_ID}/rosters`,
      params: { eventDate: PAST_DATE, limit: 100, offset: 0 },
    },
    // Paginated rosters with no date filter (all history)
    { url: `/v1/events/${EVENT_ID}/rosters`, params: { limit: 10, offset: 0 } },
    // Volunteers endpoint (may not exist)
    {
      url: `/v1/volunteers`,
      params: { eventNumber: EVENT_ID, limit: 5, offset: 0 },
    },
    {
      url: `/v1/volunteers`,
      params: { eventNumber: EVENT_ID, eventDate: PAST_DATE, limit: 5 },
    },
    // Event history endpoint
    { url: `/v1/eventhistory`, params: { eventNumber: EVENT_ID, limit: 5 } },
    // Results endpoint — check if volunteer rows come back (role/task fields)
    {
      url: `/v1/results`,
      params: { eventNumber: EVENT_ID, eventDate: PAST_DATE, limit: 5 },
    },
  ];

  for (const probe of rosterProbes) {
    console.log(`\n── Probing ${probe.url} with`, JSON.stringify(probe.params));
    try {
      const res = await client.get(probe.url, { params: probe.params });
      const keys = res.data?.data ? Object.keys(res.data.data) : [];
      console.log('  Status: 200, data keys:', keys);
      // Print first item from each array key
      for (const key of keys) {
        const val = res.data.data[key];
        if (Array.isArray(val)) {
          console.log(`  ${key} count:`, val.length);
          if (val.length > 0)
            console.log(`  ${key}[0]:`, JSON.stringify(val[0], null, 2));
        }
      }
      const range =
        res.headers?.['content-range'] || res.data?.['Content-Range'];
      if (range) console.log('  Content-Range:', range);
    } catch (e) {
      const status = e.response?.status;
      const body = e.response?.data
        ? JSON.stringify(e.response.data).slice(0, 200)
        : e.message;
      console.log(`  Status: ${status ?? 'ERR'}, body: ${body}`);
    }
  }
}

main().catch(e => {
  console.error(
    'Error:',
    e.response ? JSON.stringify(e.response.data) : e.message,
  );
  process.exit(1);
});
