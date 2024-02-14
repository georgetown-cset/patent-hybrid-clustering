WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `staging_patent_clusters.metadata_d_p_removed` 
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS families_missing
  FROM `unified_patents.classifications` 
  WHERE ARRAY_LENGTH(cpcs) = 0 
    AND patent_id IN (SELECT patent_id FROM `staging_patent_clusters.metadata_d_p_removed`)
)
SELECT 
  ROUND((families_missing/total_families)*100, 2)
FROM counts
CROSS JOIN totals 