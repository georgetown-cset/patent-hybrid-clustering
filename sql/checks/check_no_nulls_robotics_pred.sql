SELECT
  LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(pred_robotics IS NOT NULL)
FROM
  staging_patent_clusters.robotics_pred