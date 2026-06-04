--count the number of links between clusters
WITH
links_table AS (
  SELECT
    family_id,
    year,
    citing_cluster,
    cited_cluster,
    COUNT(cited) AS nlinks
  FROM
    staging_patent_clusters.citing_to_cited
  GROUP BY
    family_id,
    year,
    citing_cluster,
    cited_cluster
  ORDER BY
    family_id)

SELECT
  *
FROM
  links_table
WHERE
  nlinks >= 4 --remove rows that have less than 4 links (per Kevin's code)
  AND citing_cluster != cited_cluster
ORDER BY
  family_id
