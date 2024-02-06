WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `unified_patents.metadata` 
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS families_missing
  FROM `unified_patents.classifications` 
  WHERE ARRAY_LENGTH(cpcs) = 0 
)
SELECT 
  ROUND((families_missing/total_families)*100, 2)
FROM counts
CROSS JOIN totals 