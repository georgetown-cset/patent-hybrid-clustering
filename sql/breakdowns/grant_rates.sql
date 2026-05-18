WITH
initial_patents AS (
  SELECT
    cluster_id,
    patent_id,
    family_id,
    first_priority_date
  FROM
    staging_patent_clusters.patents_last_10_years
  -- exclude patents from the last 3 years
  -- as they may not have had time to be granted
  WHERE first_priority_date < (CURRENT_DATE() - 1096)
),

get_grants AS (
  SELECT
    cluster_id,
    initial_patents.family_id,
    LOGICAL_OR(granted) AS granted --noqa: L029
  FROM
    initial_patents
  LEFT JOIN
    unified_patents.metadata USING (patent_id)
  GROUP BY
    cluster_id,
    family_id
),

grant_rates AS (
  SELECT
    cluster_id,
    SUM(IF(granted IS TRUE, 1, 0)) / COUNT(DISTINCT family_id) AS grant_percentage
  FROM
    get_grants
  GROUP BY cluster_id
)

SELECT
  cluster_id,
  grant_percentage,
  PERCENT_RANK() OVER (ORDER BY grant_percentage) AS grant_percentile
FROM grant_rates
ORDER BY grant_percentile DESC
