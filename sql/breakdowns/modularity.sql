/* calculate modularity index for each cluster. Modularity generally varies between 0 and 1
(0 is random, 1 super-cohesive cluster, less than 0 worse than random)*/
WITH
cluster_links AS (
  SELECT
    hybrid_sts_scaled_weights_latest.id,
    ref_id,
    c1.cluster_id,
    c2.cluster_id AS ref_cluster_id,
    IF(c1.cluster_id = c2.cluster_id, 1, 0) AS internal_link,
    COUNT(*) OVER() AS N_links_network
  FROM staging_patent_clusters.hybrid_sts_scaled_weights_latest
  INNER JOIN staging_patent_clusters.cluster_assignment AS c1 ON id = c1.family_id
  INNER JOIN staging_patent_clusters.cluster_assignment AS c2 ON ref_id = c2.family_id
),

/* calculate modularity coefficient. See equation (4) in https://en.wikipedia.org/wiki/Modularity_(networks) */
N_links_tab AS (
  SELECT
    cluster_id,
    SUM(internal_link) / COUNT(*) AS internal_links_share,
    COUNT(*) / N_links_network AS cluster_links_share_in_network
  FROM
    cluster_links
  GROUP BY
    cluster_id,
    N_links_network
)

SELECT DISTINCT
  cluster_id,
  (internal_links_share - POW(cluster_links_share_in_network, 2)) AS cluster_modularity
FROM
  N_links_tab
