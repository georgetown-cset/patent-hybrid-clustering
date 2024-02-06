WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `unified_patents.metadata`
), counts AS (
SELECT 
  COUNT(DISTINCT family_id) AS total_design_patent
FROM `unified_patents.metadata`
WHERE 
  REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'S') 
)
SELECT 
  ROUND((total_design_patent/total_families)*100,2)
FROM counts
CROSS JOIN totals 