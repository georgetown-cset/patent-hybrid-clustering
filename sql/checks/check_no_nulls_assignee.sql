SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(assignee IS NOT NULL)
  AND LOGICAL_AND(assignee_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_assignee IS NOT NULL)
  AND LOGICAL_AND(NPF_top10_assignees IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_assignees IS NOT NULL)
FROM
  staging_patent_clusters.assignee
