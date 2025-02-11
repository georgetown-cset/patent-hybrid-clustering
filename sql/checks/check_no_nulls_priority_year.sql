SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(NPF_cluster IS NOT NULL)
  AND LOGICAL_AND(year IS NOT NULL)
FROM
  staging_patent_clusters.priority_year
