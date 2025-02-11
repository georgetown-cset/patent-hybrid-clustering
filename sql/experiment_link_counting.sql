/* Script to count the number of times edges in the patent hybrid network connect to patents in the
same cluster for 5 experiments.

Network: staging_patent_clusters.hybrid_sts_scaled_weights
Experiments: staging_clustering_experiments.patent_cluster_intial_experiement_sts_voting_10012024_{X_clusters}
             where X is the experiment number (0,1,2,3,4)

Table saved in staging_patent_clusters
*/

CREATE OR REPLACE TABLE staging_patent_clusters.hybrid_sts_scaled_weights_link_counts AS (

  WITH
  -- Finding which patents are connected to each other via links
  links AS (
    SELECT DISTINCT
      CAST(id AS STRING) AS node_id,
      CAST(ref_id AS STRING) AS ref_id,
      weight
    FROM staging_patent_clusters.hybrid_sts_scaled_weights
  ),

  -- Finding the cluster assignments for each patent from each run
  c0 AS (
    SELECT
      CAST(family_id AS STRING) AS node_id,
      CAST(cluster_id AS STRING) AS cluster_id
    FROM
      staging_clustering_experiments.patent_cluster_experiment_sts_voting_20241206_0_clusters_mapped_from_cset
  ),

  c1 AS (
    SELECT
      CAST(family_id AS STRING) AS node_id,
      CAST(cluster_id AS STRING) AS cluster_id
    FROM
      staging_clustering_experiments.patent_cluster_experiment_sts_voting_20241206_1_clusters_mapped_from_cset
  ),

  c2 AS (
    SELECT
      CAST(family_id AS STRING) AS node_id,
      CAST(cluster_id AS STRING) AS cluster_id
    FROM
      staging_clustering_experiments.patent_cluster_experiment_sts_voting_20241206_2_clusters_mapped_from_cset
  ),

  c3 AS (
    SELECT
      CAST(family_id AS STRING) AS node_id,
      CAST(cluster_id AS STRING) AS cluster_id
    FROM
      staging_clustering_experiments.patent_cluster_experiment_sts_voting_20241206_3_clusters_mapped_from_cset
  ),

  c4 AS (
    SELECT
      CAST(family_id AS STRING) AS node_id,
      CAST(cluster_id AS STRING) AS cluster_id
    FROM
      staging_clustering_experiments.patent_cluster_experiment_sts_voting_20241206_4_clusters_mapped_from_cset
  ),

  -- Seeing if each edge in the link graph connects to patents in the same cluster or not for each experiment
  edge_matching AS (
    SELECT
      links.node_id AS node_id,
      links.ref_id AS ref_id,
      links.weight AS weight,
      CASE WHEN c0a.cluster_id = c0b.cluster_id THEN 1 ELSE 0 END AS c0_match,
      CASE WHEN c1a.cluster_id = c1b.cluster_id THEN 1 ELSE 0 END AS c1_match,
      CASE WHEN c2a.cluster_id = c2b.cluster_id THEN 1 ELSE 0 END AS c2_match,
      CASE WHEN c3a.cluster_id = c3b.cluster_id THEN 1 ELSE 0 END AS c3_match,
      CASE WHEN c4a.cluster_id = c4b.cluster_id THEN 1 ELSE 0 END AS c4_match

    FROM links
    LEFT JOIN c0 AS c0a ON links.node_id = c0a.node_id
    LEFT JOIN c0 AS c0b ON links.ref_id = c0b.node_id
    LEFT JOIN c1 AS c1a ON links.node_id = c1a.node_id
    LEFT JOIN c1 AS c1b ON links.ref_id = c1b.node_id
    LEFT JOIN c2 AS c2a ON links.node_id = c2a.node_id
    LEFT JOIN c2 AS c2b ON links.ref_id = c2b.node_id
    LEFT JOIN c3 AS c3a ON links.node_id = c3a.node_id
    LEFT JOIN c3 AS c3b ON links.ref_id = c3b.node_id
    LEFT JOIN c4 AS c4a ON links.node_id = c4a.node_id
    LEFT JOIN c4 AS c4b ON links.ref_id = c4b.node_id
  )

  -- Generating the edge list with number of times it connects to patents in the same cluster.
  -- We will only keep edges that connect 2/5 times or more
  SELECT
    node_id,
    ref_id,
    weight,
    c0_match + c1_match + c2_match + c3_match + c4_match AS link_count
  FROM edge_matching
)
