-- Check that cybersecurity_pred fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(pred_cybersecurity IS NOT NULL)
FROM
  staging_patent_clusters.cybersecurity_pred
