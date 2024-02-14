WITH totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM `staging_patent_clusters.metadata_d_p_removed` 
), aggregation AS (
  SELECT 
    family_id,
    ARRAY_AGG(title_original IGNORE NULLS) AS titles,
  FROM `staging_patent_clusters.metadata_d_p_removed`
  GROUP BY family_id
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS families_missing
  FROM aggregation
  WHERE ARRAY_LENGTH(titles) IS NULL
)
SELECT 
  ROUND((families_missing/total_families)*100, 2)
FROM counts
CROSS JOIN totals 