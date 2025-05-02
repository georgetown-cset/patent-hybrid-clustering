-- Check that phrases fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(cset_extracted_phrase IS NOT NULL)
  AND LOGICAL_AND(score IS NOT NULL)
FROM
  staging_patent_clusters.phrases
