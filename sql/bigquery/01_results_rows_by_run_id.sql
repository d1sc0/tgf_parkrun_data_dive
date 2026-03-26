WITH result_counts AS (
  SELECT
    run_id,
    COUNT(*) AS row_count
  FROM parkrun_data.results
  GROUP BY run_id
),
volunteer_counts AS (
  SELECT
    run_id,
    COUNT(*) AS volunteer_row_count
  FROM parkrun_data.volunteers
  GROUP BY run_id
)
SELECT
  r.run_id,
  r.row_count,
  COALESCE(v.volunteer_row_count, 0) AS volunteer_row_count
FROM result_counts r
LEFT JOIN volunteer_counts v
  ON v.run_id = r.run_id
ORDER BY r.run_id DESC;
