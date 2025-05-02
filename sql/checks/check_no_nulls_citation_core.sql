-- Check that citation_code that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(family_id IS NOT NULL)
  AND LOGICAL_AND(core_stat IS NOT NULL)
  AND LOGICAL_AND(core_rank IS NOT NULL)
  AND LOGICAL_AND(core_title IS NOT NULL)
FROM
  staging_patent_clusters.citation_core
