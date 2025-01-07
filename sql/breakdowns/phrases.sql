/* get distinct phrases */
WITH
dist_ph AS (
  SELECT DISTINCT
    TRIM(cset_extracted_phrase) AS cset_extracted_phrase,
    cluster_id,
    score
  FROM
    /* drop copyright garbage */
    staging_patent_clusters.phrases
  WHERE
    cset_extracted_phrase NOT LIKE "%lsevier%"
    AND NOT REGEXP_CONTAINS(
      LOWER(cset_extracted_phrase),
      r'(?i)((documentclass 12pt minimal)|(United States)|(China)|(case study)'
      || r'|(group control group)|(high control group)|(low control group)|(real time)|'
      || r'(experimental result)|(setlength oddsidemargin 69pt)|(copyright))'
    )
)

/* merge aggregate clusters, phrases */
SELECT
  cluster_id,
  cset_extracted_phrase,
  SUM(score) AS score
FROM
  dist_ph
GROUP BY
  cluster_id,
  cset_extracted_phrase
ORDER BY
  cluster_id,
  cset_extracted_phrase
