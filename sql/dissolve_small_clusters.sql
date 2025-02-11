CREATE OR REPLACE TABLE staging_patent_clusters.cluster_assignment_small_reassigned AS (
  WITH
  all_clusters AS (
    SELECT
      *
    FROM
      staging_clustering_experiments.patent_cluster_pruning_experiement_sts_scaling_20241211_best_clusters_mapped_from_cset
  ),

  cluster_sizes AS (
    SELECT
      cluster_id,
      COUNT(*) AS size
    FROM all_clusters
    GROUP BY cluster_id
  ),

  large_cluster_assignments AS (
    SELECT
      family_id,
      cluster_id
    FROM all_clusters
    WHERE cluster_id IN (SELECT cluster_id FROM cluster_sizes WHERE size > 49)
  ),

  patent_cluster_weights AS (
    SELECT DISTINCT
      id AS family_id,
      fam.cluster_id AS family_cluster,
      ref_id,
      ref.cluster_id AS ref_cluster,
      weight
    FROM staging_patent_clusters.hybrid_sts_scaled_weights
    LEFT JOIN large_cluster_assignments AS fam ON id = fam.family_id
    LEFT JOIN large_cluster_assignments AS ref ON ref_id = ref.family_id
  ),

  ref_weights AS (
    SELECT
      family_id,
      ref_cluster AS cluster_id,
      SUM(weight) AS ref_weight
    FROM patent_cluster_weights
    WHERE ref_cluster IS NOT NULL AND family_cluster IS NULL
    GROUP BY family_id, ref_cluster
  ),

  cit_weights AS (
    SELECT
      ref_id AS family_id,
      family_cluster AS cluster_id,
      SUM(weight) AS cit_weight
    FROM patent_cluster_weights
    WHERE family_cluster IS NOT NULL AND ref_cluster IS NULL
    GROUP BY ref_id, family_cluster
  ),

  total_weights AS (
    SELECT
      family_id,
      cluster_id,
      (COALESCE(ref_weight, 0.0) + COALESCE(cit_weight, 0.0)) / SQRT(size) AS scaled_total_weight
    FROM ref_weights
    FULL JOIN cit_weights USING (family_id, cluster_id)
    FULL JOIN cluster_sizes USING (cluster_id)
  ),

  weight_rankings AS (
    SELECT
      family_id,
      ARRAY_AGG(cluster_id IGNORE NULLS ORDER BY scaled_total_weight DESC)[0] AS cluster_id
    FROM total_weights
    GROUP BY family_id
  )


  SELECT
    family_id,
    cluster_id
  FROM weight_rankings
  UNION ALL
  SELECT
    family_id,
    cluster_id
  FROM large_cluster_assignments
)
