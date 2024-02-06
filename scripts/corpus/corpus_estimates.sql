/*
Create table for Design & Plant patents to remove. 
*/
CREATE OR REPLACE TABLE cs2078_sandbox.patent_test AS (
  SELECT 
    DISTINCT family_id AS family_id
  FROM `unified_patents.metadata`
  WHERE 
    REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'S')
    OR REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'P')
); 
WITH aggregation AS (
  SELECT 
    family_id,
    ARRAY_LENGTH(ARRAY_AGG(patent_id)) AS family_patents, 
    ARRAY_AGG(title IGNORE NULLS) AS titles,
    ARRAY_AGG(title_original IGNORE NULLS) AS titles_og,
    ARRAY_AGG(abstract IGNORE NULLS) AS abstracts,
    ARRAY_AGG(abstract_original IGNORE NULLS) AS abstracts_og,
  FROM `unified_patents.metadata`
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
WHERE family_id NOT IN (SELECT family_id FROM cs2078_sandbox.patent_test WHERE family_id IS NOT NULL)