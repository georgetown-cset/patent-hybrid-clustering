WITH
org_counts AS (
  SELECT
    assignee AS org_name,
    COUNT(DISTINCT(cluster_id)) AS num_clusters
  FROM
    staging_patent_clusters.assignees
  WHERE
    assignee_rank <= 10
  GROUP BY
    assignee )

SELECT org_name
FROM
  org_counts
WHERE
  REGEXP_CONTAINS(org_name, r'[A-Za-z]')
ORDER BY
  num_clusters DESC
LIMIT
  3000
