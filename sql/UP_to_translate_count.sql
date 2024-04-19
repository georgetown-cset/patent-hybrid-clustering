SELECT
  COUNT(DISTINCT family_id) AS num_families,
  SUM(LENGTH(title_original)) AS title_chars,
  SUM(LENGTH(abstract_original)) AS abstracts_chars
FROM `staging_patent_clusters.UP_to_translate`
