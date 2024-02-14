WITH refactor AS (
  SELECT 
    ref.patent_id, 
    md.family_id AS family_id, 
    patent_reference, 
    md2.family_id AS reference_family
  FROM`unified_patents.references` ref
  LEFT JOIN `staging_patent_clusters.metadata_d_p_removed` md USING(patent_id)
  LEFT JOIN `staging_patent_clusters.metadata_d_p_removed` md2 ON(ref.patent_reference=md2.patent_id)
  WHERE 
    ref.patent_id IN (SELECT patent_id FROM `staging_patent_clusters.metadata_d_p_removed`)
    AND patent_reference IS NOT NULL 
), totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM refactor
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_outside_reference_families
  FROM refactor
  WHERE reference_family != family_id
)
SELECT 
  ROUND((total_outside_reference_families/total_families)*100,2)
FROM counts
CROSS JOIN totals 