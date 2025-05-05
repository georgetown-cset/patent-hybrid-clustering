-- Figure out how much text we have to translate to calculate cost

SELECT
  COUNT(DISTINCT family_id) AS num_families,
  SUM(LENGTH(titles_original)) AS title_chars,
  SUM(LENGTH(abstracts_original)) AS abstracts_chars
FROM `staging_patent_clusters.LID_to_translate`
