SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(inventor IS NOT NULL)
  AND LOGICAL_AND(inventor_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_inventor IS NOT NULL)
  AND LOGICAL_AND(NPF_top10_inventors IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_inventors IS NOT NULL)
FROM
  staging_patent_clusters.inventor
