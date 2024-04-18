/*
Goal: 
- How many total patent families have only text from languages outside the model
- How much total text that is in characters, so we can calculate the cost.
For UP: 
 - ['bs', 'is', 'no', 'ot', 'sh']
*/
CREATE OR REPLACE TABLE staging_patent_clusters.UP_to_translate AS (
  WITH UP_patents AS (
    # Get langs into array for later model check. 
    SELECT  
      family_id, 
      ARRAY_AGG(md.language IGNORE NULLS) AS langs
    FROM `staging_patent_clusters.metadata_d_p_removed` md
    WHERE 
      language IN ('bs', 'is', 'no', 'ot', 'sh')
    GROUP BY family_id
  ), language_check_stage AS (
    # Ensured the same number of family IDs in this subquery as above. 
    SELECT
      family_id, 
      langs, 
      patent_id, 
      title_original, 
      abstract_original
    FROM UP_patents
    JOIN `staging_patent_clusters.metadata_d_p_removed` USING(family_id)
    # Model language check.
    WHERE staging_patent_clusters.checkLanguages(langs) IS FALSE
  )
  SELECT 
    DISTINCT family_id, 
    patent_id, 
    title_original, 
    abstract_original
  FROM language_check_stage 
)