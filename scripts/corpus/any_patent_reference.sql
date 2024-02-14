WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `unified_patents.references`
  WHERE patent_id IN (SELECT patent_id FROM `staging_patent_clusters.metadata_d_p_removed`)
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_with_references
  FROM `unified_patents.references`
  WHERE patent_reference IS NOT NULL
    AND patent_id IN (SELECT patent_id FROM `staging_patent_clusters.metadata_d_p_removed`)
)
SELECT 
  ROUND((total_with_references/total_families)*100,2)
FROM counts
CROSS JOIN totals 