CREATE OR REPLACE TABLE staging_patent_clusters.cluster_ranked_connections AS (
WITH
  cluster_assignment AS (
    SELECT
      DISTINCT
      family_id,
      cluster_id,
    FROM staging_patent_clusters.cluster_assignment
  ),
  all_weights AS (
    SELECT
    DISTINCT
      id AS family_id,
      a1.cluster_id AS family_cluster,
      ref_id,
      a2.cluster_id AS ref_cluster,
      weight
    FROM staging_patent_clusters.hybrid_mean_scaled_weights
    LEFT JOIN cluster_assignment AS a1 ON id = a1.family_id
    LEFT JOIN cluster_assignment AS a2 on ref_id = a2.family_id
  ),
  cluster_cits AS (
    SELECT
      family_cluster,
      ref_cluster,
      SUM(weight) AS weight
    FROM all_weights
    WHERE family_cluster != ref_cluster
    GROUP BY family_cluster,ref_cluster
  ),
  cluster_cit_refs AS (
    SELECT
      DISTINCT *
    FROM cluster_cits
    UNION DISTINCT
    (SELECT
      ref_cluster AS family_cluster,
      family_cluster AS ref_cluster,
      weight
    FROM cluster_cits)
  ),
  cluster_weights AS (
    SELECT
      family_cluster,
      ref_cluster,
      SUM(weight) AS weight
    FROM cluster_cit_refs
    GROUP BY family_cluster, ref_cluster
  ),
  cluster_ranks AS (
    SELECT
      family_cluster,
      ref_cluster,
      weight,
      ROW_NUMBER() OVER (PARTITION BY family_cluster ORDER BY weight DESC) AS link_rank
    FROM cluster_weights
  )

  SELECT * FROM cluster_ranks
)
