/* calculate modularity index for each cluster. Modularity generally varies between 0 and 1
(0 is random, 1 super-cohesive cluster, less than 0 worse than random)*/
CREATE OR REPLACE TABLE patent_clustering_metrics.cluster_modularity_by_weights AS (
  WITH
  /* assign cluster IDs to citations */
  cluster_ref_tab AS (
    SELECT
      cluster_id,
      cluster_ref_id,
      IF(cluster_id = cluster_ref_id, 1, 0) AS internal_link,
      COUNT(*) OVER() AS N_links_network
    FROM ( /* assign cluster IDs to referencing papers */
      SELECT
        family_id AS id,
        family_reference AS ref_id
      FROM
        -- we're using our weights here instead of citations
        staging_patent_clusters.combined_weights)
    INNER JOIN (
      SELECT
        cluster_id,
        family_id AS id
      FROM
        patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped )
      USING
        (id)
    INNER JOIN ( /* assign cluster IDs to referenced papers */
      SELECT
        cluster_id AS cluster_ref_id,
        family_id AS ref_id
      FROM
        patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped )
      USING
        (ref_id) ),

  /* calculate modularity coefficient. See equation (4) in https://en.wikipedia.org/wiki/Modularity_(networks) */
  N_links_tab AS (
    SELECT
      cluster_id,
      SUM(internal_link) / COUNT(*) AS internal_links_share,
      COUNT(*) / N_links_network AS cluster_links_share_in_network
    FROM
      cluster_ref_tab
    GROUP BY
      cluster_id,
      N_links_network ),

  /* finding distribution of cluster modularity */
  modularity AS (
    SELECT DISTINCT
      cluster_id,
      (internal_links_share - POW(cluster_links_share_in_network, 2)) AS cluster_modularity
    FROM
      N_links_tab
    ORDER BY cluster_modularity
  ),

  /* finding histograms of modularities */
  histogram AS (
    SELECT
      FLOOR(cluster_modularity * 100) / 100 AS bin_floor,
      COUNT(DISTINCT cluster_id) AS n_clusters
    FROM modularity
    GROUP BY bin_floor
    ORDER BY bin_floor
  )

  SELECT
    bin_floor,
    n_clusters
  FROM histogram
  ORDER BY bin_floor
)
