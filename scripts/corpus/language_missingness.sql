WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `unified_patents.metadata` 
), aggregation AS (
  SELECT 
    family_id,
    ARRAY_AGG(language IGNORE NULLS) AS languages,
  FROM `unified_patents.metadata`
  GROUP BY family_id
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS families_missing
  FROM aggregation
  WHERE ARRAY_LENGTH(languages) IS NULL
)
SELECT 
  ROUND((families_missing/total_families)*100, 2)
FROM counts
CROSS JOIN totals 