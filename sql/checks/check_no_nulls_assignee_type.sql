-- Check that assignee_type fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_assignee_types IS NOT NULL)
  AND LOGICAL_AND(NPF IS NOT NULL)
FROM
  staging_patent_clusters.assignee_type
