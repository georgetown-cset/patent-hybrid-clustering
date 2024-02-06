WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `unified_patents.references`
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_with_references
  FROM `unified_patents.references`
  WHERE patent_reference IS NOT NULL
)
SELECT 
  ROUND((total_with_references/total_families)*100,2)
FROM counts
CROSS JOIN totals 