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
  ), english_check_stage AS (
    # For non-LID, we need to make sure there isn't an English-version available.
    SELECT
      family_id, 
      ARRAY_AGG(language) AS langs, 
      STRING_AGG(title_original) AS titles_original, 
      STRING_AGG(abstract_original) AS abstracts_original
    FROM UP_patents
    JOIN `staging_patent_clusters.metadata_d_p_removed` USING(family_id)
    WHERE 
      # We deal with null language in the LID queries. 
      language IS NOT NULL 
    GROUP BY family_id
  )
  SELECT 
    family_id, 
    titles_original, 
    abstracts_original 
  FROM english_check_stage 
  # Check for English in a patent family's aggregated languages.
  WHERE 'en' NOT IN UNNEST(langs)
)