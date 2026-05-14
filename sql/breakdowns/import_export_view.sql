WITH
import_links AS (
  SELECT
    year,
    citing_cluster,
    cited_cluster,
    COUNT(family_id) AS ct
  FROM
    staging_patent_clusters.nlinks
  GROUP BY
    year,
    citing_cluster,
    cited_cluster
  ORDER BY
    citing_cluster)

-- get the number of imports and exports by unique cluster pair
-- ct_ab is the number of imports from cluster_a to cluster_b, and ct_ba is the number of exports
-- from cluster_a to cluster_b
SELECT
  import_links.year,
  import_links.citing_cluster AS cluster_a,
  import_links.cited_cluster AS cluster_b,
  import_links.ct AS ct_ab,
  two.ct AS ct_ba,
  import_links.ct + two.ct AS ct_total
FROM
  import_links
LEFT JOIN
  import_links AS two
  ON
    (import_links.cited_cluster = two.citing_cluster
      AND import_links.citing_cluster = two.cited_cluster
      AND import_links.year = two.year)
