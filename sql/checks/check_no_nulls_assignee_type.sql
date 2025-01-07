SELECT
  LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(Company IS NOT NULL)
  AND LOGICAL_AND(Education IS NOT NULL)
  AND LOGICAL_AND(Nonprofit IS NOT NULL)
  AND LOGICAL_AND(Government IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_assignee_types IS NOT NULL)
  AND LOGICAL_AND(NPF IS NOT NULL)
FROM
  staging_patent_clusters.assignee_type