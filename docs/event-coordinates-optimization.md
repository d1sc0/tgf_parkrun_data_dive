# Performance Optimization: Event Coordinates Caching

## Overview

The `HomeRunMap` component displays athlete visitor origins on an interactive map. Previously, it required fetching the external `events.json` file (~1-2MB) from the Parkrun API on every page load to match home run names to geographic coordinates.

This optimization caches event coordinates in BigQuery, eliminating the external API dependency and speeding up page load times.

## How It Works

1. **Sync Script** (`utilities/sync-event-coordinates.js`): Fetches `events.json` once and loads coordinates into a BigQuery table
2. **View Query** (`sql/bigquery/22_dashboard_visitor_stats.sql`): Includes commented-out optimized SQL that joins to the cached coordinates
3. **Component Logic** (`dashboard/src/components/HomeRunMap.astro`): Intelligently uses cached coordinates when available, falls back to fetching `events.json` if needed

## Setup Instructions

### Step 1: Ensure GCP Credentials

Make sure your environment has proper Google Cloud credentials:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
# OR use Application Default Credentials (gcloud auth application-default login)
```

### Step 2: Sync Event Coordinates

Run the sync script to load coordinates into BigQuery:

```bash
npm run sync:coordinates
```

This will:

- Create the `event_coordinates` table in your BigQuery dataset
- Fetch all event coordinates from the Parkrun API
- Insert them into BigQuery (can be re-run anytime to refresh)

Expected output:

```
Fetching event coordinates from parkrun events.json...
Fetched 600+ event coordinates. Loading into BigQuery...
Successfully loaded XXX event coordinates.
✅ Event coordinates sync completed successfully.
```

### Step 3: Enable the Optimized View

Once coordinates are cached, uncomment the optimized SQL in `sql/bigquery/22_dashboard_visitor_stats.sql`:

1. Open `sql/bigquery/22_dashboard_visitor_stats.sql`
2. Comment out the current version (lines with `CAST(NULL AS...`)
3. Uncomment the optimized version (lines starting with `--	v.home_run_name`)
4. Save and republish views:

```bash
npm run publish:views
```

### Step 4: Verify

- The map component will now use cached BigQuery coordinates
- Check the browser console—you should NOT see a log message about fetching from Parkrun API
- Page load times should improve slightly

## Benefits

✅ **Eliminates external API call** (~1-2MB download)  
✅ **Faster page loads** for HomeRunMap component  
✅ **Reduces request latency** and bandwidth  
✅ **More resilient** if Parkrun API is unavailable  
✅ **One-time sync** per environment

## Optimization Details

### Event Coordinates Table Schema

```
event_coordinates:
  - event_name (STRING): e.g., "Weymouth"
  - event_long_name (STRING): e.g., "Weymouth parkrun"
  - latitude (FLOAT64): Geographic latitude
  - longitude (FLOAT64): Geographic longitude
  - country (STRING): e.g., "GB"
  - last_updated (TIMESTAMP): When coordinates were synced
```

### View Behavior

**Before optimization:** View returns NULL for latitude/longitude → HomeRunMap falls back to external API  
**After optimization:** View returns real coordinates from JOIN → HomeRunMap uses cached data

### Fallback Logic

If coordinates table is missing or view isn't updated, HomeRunMap automatically:

1. Detects NULL coordinates from the view
2. Falls back to fetching `events.json` from Parkrun
3. Matches coordinates client-side (original behavior)
4. No errors—just slower

## Maintenance

Re-run `npm run sync:coordinates` periodically (e.g., monthly) if new parkrun events are added. This won't affect the dashboard—it just refreshes the coordinate cache.

## Troubleshooting

**Error: "Could not load the default credentials"**

- Ensure `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set or you're authenticated via `gcloud auth application-default login`

**Error: "Table event_coordinates was not found"**

- Run `npm run sync:coordinates` first to create and populate the table

**Map still fetching from Parkrun API**

- Check if the optimized SQL in `22_dashboard_visitor_stats.sql` is uncommented
- Run `npm run publish:views` to apply changes
- Check browser console to see which path is being taken

## Related Files

- `utilities/sync-event-coordinates.js` — Sync script
- `sql/bigquery/22_dashboard_visitor_stats.sql` — View with optimized SQL (commented)
- `dashboard/src/components/HomeRunMap.astro` — Component with fallback logic
- `package.json` — `npm run sync:coordinates` script
