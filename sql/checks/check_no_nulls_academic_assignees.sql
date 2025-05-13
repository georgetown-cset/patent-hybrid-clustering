-- Check that academic assignee fields that shouldn't be null aren't
-- Most of the assignee fields can be null if there are not assignees
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_academic_assignees IS NOT NULL)
FROM
  staging_patent_clusters.academic_assignees
