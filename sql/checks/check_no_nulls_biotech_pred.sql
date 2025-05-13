-- Check that biotech_pred fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(pred_biotech IS NOT NULL)
FROM
  staging_patent_clusters.biotech_pred
