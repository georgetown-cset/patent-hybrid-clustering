-- Combine the previous and new cluster assignment to create the final clusters
SELECT DISTINCT
  family_id,
  cluster_id
FROM
  staging_patent_clusters.cluster_assignment_staging
UNION DISTINCT
SELECT DISTINCT
  family_id,
  cluster_id
FROM
  staging_patent_clusters.new_cluster_assignment
