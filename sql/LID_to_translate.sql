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
    # We need family ID's from the metadata table.
    # No English check is needed because these patents don't have any language code. 
    SELECT  
      family_id,
      STRING_AGG(title_original) AS titles_original, 
      STRING_AGG(abstract_original) AS abstracts_original
    FROM `staging_patent_clusters.patents_lid` lid
    JOIN `staging_patent_clusters.metadata_d_p_removed` USING(patent_id)
    WHERE 
      lid.language IN ('aa', 'ab', 'af', 'az', 'bs', 'co', 'cy', 
                  'eo', 'eu', 'fy', 'gn', 'ia', 'ie', 'la', 
                  'lb', 'ln', 'nn', 'no', 'oc', 'qu', 'sw',
                  'tk', 'tl', 'tt', 'vo', 'war')
    GROUP BY family_id
  )
  SELECT 
    family_id, 
    titles_original, 
    abstracts_original 
  FROM LID_patents 
  WHERE 
    # Just make sure we're not double-counting families/characters. 
    family_id NOT IN (SELECT family_id FROM `staging_patent_clusters.UP_to_translate`)
)