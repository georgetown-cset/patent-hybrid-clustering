/* aggregate patent category names and percentages into a string for presentation */
WITH
  ca_tab AS (
  SELECT
    *
  FROM
    staging_patent_clusters.patent_category_staging
  ORDER BY
    cluster_id,
    category_rank
)

SELECT DISTINCT
  cluster_id,
  MAX(class_cat) AS main_category,
  STRING_AGG(CONCAT(category, ' (',
  ROUND(percentage * 100,1), '%)'), ', ') AS top_categories,
FROM
  ca_tab
GROUP BY
  cluster_id
ORDER BY
  cluster_id