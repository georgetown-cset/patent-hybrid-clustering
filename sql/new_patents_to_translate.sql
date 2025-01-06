WITH
LID_families AS (
  SELECT
    CAST(family_id AS STRING) AS family_id,
    ARRAY_AGG(DISTINCT language IGNORE NULLS) AS langs
  FROM staging_patent_clusters.patent_lid
  GROUP BY family_id
),

model_check AS (
  SELECT family_id
  FROM LID_families
  WHERE staging_patent_clusters.CHECKLANGUAGES(langs) IS FALSE
)

SELECT
  patent_id,
  family_id,
  title_original,
  abstract_original
FROM staging_patent_clusters.new_metadata_to_lid
WHERE family_id IN (SELECT family_id FROM model_check)
