-- View: dashboard_visitor_stats
-- Purpose: Home-run visitor summary with cached coordinates for map visualization.
-- Joins visitor statistics to event_coordinates so HomeRunMap can avoid external events.json calls.

WITH visitor_stats AS (
  SELECT
	home_run_name,
	COUNT(*) AS visit_count,
	COUNT(DISTINCT athlete_id) AS athlete_count,
	MIN(event_date) AS first_seen_date,
	MAX(event_date) AS last_seen_date,
	LOWER(TRIM(home_run_name)) AS normalized_home_run_name,
	LOWER(TRIM(REGEXP_REPLACE(home_run_name, r'\\s+parkrun$', ''))) AS normalized_home_run_base
  FROM `parkrun_data.results`
  WHERE home_run_name IS NOT NULL
	AND home_run_name != ''
	AND is_unknown_athlete = FALSE
  GROUP BY home_run_name
),
visitor_keys AS (
  SELECT
	home_run_name,
	normalized_home_run_name AS match_key,
	1 AS key_priority
  FROM visitor_stats

  UNION ALL

  SELECT
	home_run_name,
	normalized_home_run_base AS match_key,
	2 AS key_priority
  FROM visitor_stats
),
coordinate_keys AS (
  SELECT
	LOWER(TRIM(event_name)) AS match_key,
	latitude,
	longitude,
	1 AS coord_priority
  FROM `parkrun_data.event_coordinates`
  WHERE event_name IS NOT NULL

  UNION ALL

  SELECT
	LOWER(TRIM(event_long_name)) AS match_key,
	latitude,
	longitude,
	2 AS coord_priority
  FROM `parkrun_data.event_coordinates`
  WHERE event_long_name IS NOT NULL

  UNION ALL

  SELECT
	LOWER(TRIM(REGEXP_REPLACE(event_name, r'\\s+parkrun$', ''))) AS match_key,
	latitude,
	longitude,
	3 AS coord_priority
  FROM `parkrun_data.event_coordinates`
  WHERE event_name IS NOT NULL

  UNION ALL

  SELECT
	LOWER(TRIM(REGEXP_REPLACE(event_long_name, r'\\s+parkrun$', ''))) AS match_key,
	latitude,
	longitude,
	4 AS coord_priority
  FROM `parkrun_data.event_coordinates`
  WHERE event_long_name IS NOT NULL
),
best_coordinate_match AS (
  SELECT
	vk.home_run_name,
	ck.latitude,
	ck.longitude,
	ROW_NUMBER() OVER (
	  PARTITION BY vk.home_run_name
	  ORDER BY vk.key_priority, ck.coord_priority
	) AS rn
  FROM visitor_keys vk
  JOIN coordinate_keys ck
	ON vk.match_key = ck.match_key
  WHERE vk.match_key IS NOT NULL
	AND vk.match_key != ''
)

SELECT
	v.home_run_name,
	v.normalized_home_run_name,
	v.visit_count,
	v.athlete_count,
	v.first_seen_date,
	v.last_seen_date,
	bm.latitude,
	bm.longitude
FROM visitor_stats v
LEFT JOIN best_coordinate_match bm
	ON v.home_run_name = bm.home_run_name
	AND bm.rn = 1
ORDER BY v.athlete_count DESC, v.visit_count DESC, v.home_run_name ASC;
