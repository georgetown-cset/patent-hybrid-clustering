-- Get new non-English patents to run LID on

SELECT
  patent_id,
  COALESCE(family_id, 'X-' || patent_id) AS family_id,
  title_original,
  abstract_original
FROM staging_patent_clusters.new_metadata
WHERE
  title IS NULL
  AND abstract IS NULL
  AND title_original IS NOT NULL
  AND abstract_original IS NOT NULL
