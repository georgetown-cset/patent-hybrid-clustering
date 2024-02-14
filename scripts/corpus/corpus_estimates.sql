WITH aggregation AS (
  SELECT 
    family_id,
    ARRAY_LENGTH(ARRAY_AGG(patent_id)) AS family_patents, 
    ARRAY_AGG(title IGNORE NULLS) AS titles,
    ARRAY_AGG(title_original IGNORE NULLS) AS titles_og,
    ARRAY_AGG(abstract IGNORE NULLS) AS abstracts,
    ARRAY_AGG(abstract_original IGNORE NULLS) AS abstracts_og,
  FROM `staging_patent_clusters.metadata_d_p_removed`
  GROUP BY family_id
), selection AS (
  SELECT 
    family_id, 
    family_patents
  FROM aggregation 
  WHERE 
    ARRAY_LENGTH(titles) IS NOT NULL
    AND ARRAY_LENGTH(titles_og) IS NOT NULL
    AND ARRAY_LENGTH(abstracts) IS NOT NULL 
    AND ARRAY_LENGTH(abstracts_og) IS NOT NULL
)
SELECT 
  COUNT(DISTINCT family_id) AS total_families, 
  SUM(family_patents) AS total_patents
FROM selection