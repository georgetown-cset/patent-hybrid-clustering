-- Check that patent_class fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(class_cat IS NOT NULL)
  AND LOGICAL_AND(categories_agg IS NOT NULL)
FROM
  staging_patent_clusters.patent_class
