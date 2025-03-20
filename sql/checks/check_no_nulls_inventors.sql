-- Most of the inventor fields can be null if there are not inventors
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_inventors IS NOT NULL)
FROM
  staging_patent_clusters.inventors
