/*
Goal: 
- How many total patent families have only text from languages outside the model
- How much total text that is in characters, so we can calculate the cost.
For LID: 
 - ['aa', 'ab', 'af', 'az', 'bs', 'co', 'cy', 
    'eo', 'eu', 'fy', 'gn', 'ia', 'ie', 'la', 
    'lb', 'ln', 'nn', 'no', 'oc', 'qu', 'sw',
    'tk', 'tl', 'tt', 'vo', 'war']
*/
CREATE OR REPLACE TABLE staging_patent_clusters.LID_to_translate AS (
  WITH LID_patents AS (
    # We need family IDs from the metadata table.
    # Get langs into array for later model check. 
    SELECT  
      family_id, 
      ARRAY_AGG(md.language IGNORE NULLS) AS langs 
    FROM `staging_patent_clusters.patents_lid` lid
    JOIN `staging_patent_clusters.metadata_d_p_removed` md USING(patent_id)
    WHERE 
      lid.language IN ('aa', 'ab', 'af', 'az', 'bs', 'co', 'cy', 
                  'eo', 'eu', 'fy', 'gn', 'ia', 'ie', 'la', 
                  'lb', 'ln', 'nn', 'no', 'oc', 'qu', 'sw',
                  'tk', 'tl', 'tt', 'vo', 'war')
    GROUP BY family_id
  ), language_check_stage AS (
    # Ensured the same number of family IDs in this subquery as above. 
    SELECT 
      family_id, 
      langs, 
      patent_id, 
      title_original, 
      abstract_original
    FROM LID_patents
    JOIN `staging_patent_clusters.metadata_d_p_removed` md USING(family_id)
    WHERE 
      # Model language check. 
      staging_patent_clusters.checkLanguages(langs) IS FALSE
      # Just make sure we're not double-counting families/characters. 
      AND family_id NOT IN (SELECT family_id FROM `staging_patent_clusters.UP_to_translate`)
  )
  SELECT 
    DISTINCT family_id, 
    patent_id, 
    title_original, 
    abstract_original
  FROM language_check_stage 
)