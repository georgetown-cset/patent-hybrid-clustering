-- check that each family id is only assigned once
SELECT COUNT(DISTINCT family_id) = COUNT(family_id)
FROM
  staging_patent_clusters.cluster_assignment
