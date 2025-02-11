SELECT DISTINCT
  *
FROM
  staging_patent_clusters.cluster_assignment
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  staging_patent_clusters.new_cluster_assignment