SELECT
  LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(main_category IS NOT NULL)
  AND LOGICAL_AND(top_categories IS NOT NULL)
FROM
  staging_patent_clusters.patent_category