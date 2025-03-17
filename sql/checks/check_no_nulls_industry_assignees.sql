-- Most of the assignee fields can be null if there are not assignees
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_industry_assignees IS NOT NULL)
FROM
  staging_patent_clusters.industry_assignees
