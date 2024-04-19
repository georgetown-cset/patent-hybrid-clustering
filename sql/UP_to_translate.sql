/*
Goal: 
- How many total patent families have only text from languages outside the model
- How much total text that is in characters, so we can calculate the cost.
For UP: 
 - ['bs', 'is', 'no', 'ot', 'sh']
*/
CREATE OR REPLACE TABLE staging_patent_clusters.UP_to_translate AS (
  WITH UP_patents AS (
    SELECT  
      family_id
    FROM `staging_patent_clusters.metadata_d_p_removed`
    WHERE 
      language IN ('bs', 'is', 'no', 'ot', 'sh')
  ), model_check_stage AS (
    # Ensured the same number of family IDs in this subquery as above. 
    SELECT
      family_id, 
      ARRAY_AGG(language IGNORE NULLS) AS langs
    FROM UP_patents
    JOIN `staging_patent_clusters.metadata_d_p_removed` USING(family_id)
    GROUP BY family_id
  ), model_check AS (
    SELECT 
      DISTINCT family_id, 
    FROM model_check_stage 
    # Check for model coverage in a patent family's aggregated languages.
    WHERE staging_patent_clusters.checkLanguages(langs) IS FALSE
  )
  SELECT 
    DISTINCT family_id,
    patent_id, 
    title_original, 
    abstract_original
  FROM model_check
  JOIN `staging_patent_clusters.metadata_d_p_removed` USING(family_ID)
)
