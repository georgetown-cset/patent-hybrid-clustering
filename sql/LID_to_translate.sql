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
    SELECT family_id
    FROM `staging_patent_clusters.patents_lid`
    INNER JOIN `staging_patent_clusters.metadata_d_p_removed` USING (patent_id)
    WHERE
      staging_patent_clusters.patents_lid.language IN ('aa', 'ab', 'af', 'az', 'bs', 'co', 'cy',
        'eo', 'eu', 'fy', 'gn', 'ia', 'ie', 'la',
        'lb', 'ln', 'nn', 'no', 'oc', 'qu', 'sw',
        'tk', 'tl', 'tt', 'vo', 'war')
  ),

  model_check_stage AS (
    # Ensured the same number of family IDs in this subquery as above.
    SELECT
      family_id,
      ARRAY_AGG(staging_patent_clusters.metadata_d_p_removed.language IGNORE NULLS) AS langs
    FROM LID_patents
    INNER JOIN `staging_patent_clusters.metadata_d_p_removed` USING (family_id)
    GROUP BY family_id
  ),

  model_check AS (
    SELECT DISTINCT family_id
    FROM model_check_stage
    WHERE
      staging_patent_clusters.CHECKLANGUAGES(langs) IS FALSE
      # Just make sure we're not double-counting families/characters.
      AND family_id NOT IN (SELECT family_id FROM `staging_patent_clusters.UP_to_translate`)
  )

  SELECT DISTINCT
    family_id,
    patent_id,
    title_original,
    abstract_original
  FROM model_check
  INNER JOIN `staging_patent_clusters.LID_to_translate` USING (family_id)
)
