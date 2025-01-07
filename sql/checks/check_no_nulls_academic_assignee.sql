SELECT
  LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(academic_assignee IS NOT NULL)
  AND LOGICAL_AND(academic_assignee_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_academic_assignee IS NOT NULL)
  AND LOGICAL_AND(NPF_top10_academic_assignees IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_academic_assignees IS NOT NULL)
FROM
  staging_patent_clusters.academic_assignee