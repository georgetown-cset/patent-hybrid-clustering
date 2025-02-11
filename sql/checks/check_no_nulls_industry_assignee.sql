SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(industry_assignee IS NOT NULL)
  AND LOGICAL_AND(industry_assignee_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_industry_assignee IS NOT NULL)
  AND LOGICAL_AND(NPF_top10_industry_assignees IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_industry_assignees IS NOT NULL)
FROM
  staging_patent_clusters.industry_assignee
