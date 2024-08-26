-- Finding the patent assignment level for patents in the new clusters,
-- based on each patent's total number of within-cluster links
-- as well as the fraction of patent links that connect within-cluster.
CREATE OR REPLACE TABLE patent_clustering_metrics.patent_assignment_level AS (
  WITH
  -- Finding links that connect patents within the same cluster
  new_within_cluster_links AS (
    SELECT
      staging_patent_clusters.family_references.family_id AS family_id,
      staging_patent_clusters.family_references.family_reference AS ref_id,
      c1.cluster_id AS cluster_id
    FROM staging_patent_clusters.family_references
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped AS c1 ON
        staging_patent_clusters.family_references.family_id = c1.family_id
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped AS c2 ON
        staging_patent_clusters.family_references.family_reference = c2.family_id
    WHERE c1.cluster_id = c2.cluster_id
  ),

  -- Counting within-cluster references
  new_references_within_cluster AS (
    SELECT
      family_id AS document_id,
      cluster_id,
      COUNT(DISTINCT ref_id) AS n_references_cluster
    FROM new_within_cluster_links
    GROUP BY family_id, cluster_id
  ),

  -- Counting within-cluster citations
  new_citations_within_cluster AS (
    SELECT
      ref_id AS document_id,
      cluster_id,
      COUNT(DISTINCT family_id) AS n_citations_cluster
    FROM new_within_cluster_links
    GROUP BY ref_id, cluster_id
  ),

  -- Finding all references for each patent
  new_references_counts AS (
    SELECT
      family_id AS document_id,
      COUNT(DISTINCT family_reference) AS n_references_total
    FROM staging_patent_clusters.family_references
    GROUP BY family_id
  ),

  -- Finding all citations for each article
  new_citations_counts AS (
    SELECT
      family_reference AS document_id,
      COUNT(DISTINCT family_id) AS n_citations_total
    FROM staging_patent_clusters.family_references
    GROUP BY family_reference
  ),

  -- Finding relevant metrics for each article
  new_patent_links AS (
    SELECT
      document_id,
      cluster_id,
      n_references_cluster,
      n_citations_cluster,
      n_references_total,
      n_citations_total,
      COALESCE(n_references_total + n_citations_total, n_references_total, n_citations_total, 0) AS n_links,
      COALESCE(
        n_references_cluster + n_citations_cluster, n_references_cluster, n_citations_cluster, 0
      ) AS cluster_links,
      COALESCE(
        n_references_cluster + n_citations_cluster, n_references_cluster, n_citations_cluster, 0
      ) / COALESCE(n_references_total + n_citations_total, n_references_total, n_citations_total, 1) AS frac
    FROM new_references_within_cluster
    FULL JOIN new_citations_within_cluster USING (document_id, cluster_id)
    FULL JOIN new_references_counts USING (document_id)
    FULL JOIN new_citations_counts USING (document_id)

  ),

  -- Finding fraction thresholds
  new_patent_thres AS (
    SELECT
      *,
      0.1 + 0.2 / 15 * SQRT(ABS(15 * 15 - (cluster_links - 5) * (cluster_links - 5))) AS thres1,
      0.03 + 0.07 / 8 * SQRT(ABS(8 * 8 - (cluster_links - 2) * (cluster_links - 2))) AS thres2
    FROM new_patent_links
  ),

  -- Assigning patent levels
  new_patent_assignment AS (
    SELECT
      *,
      CASE
        -- 4: DEFINITIVE
        WHEN cluster_links >= 5 AND frac >= 0.5 THEN 4
        -- 3: VERY WELL
        WHEN
          (
            cluster_links >= 20 AND frac >= 0.1 AND frac < 0.5
          ) OR (cluster_links >= 5 AND cluster_links <= 20 AND frac >= thres1 AND frac < 0.5) THEN 3
        -- 2: DEFENSIBLE
        WHEN
          frac >= 0.03 AND cluster_links >= 2 AND (
            (
              cluster_links < 5 AND frac >= 0.1
            ) OR (
              frac < 0.1 AND frac >= thres2 AND cluster_links < 10
            ) OR (
              cluster_links >= 10 AND frac < 0.1
            ) OR (cluster_links >= 5 AND frac >= 0.1 AND cluster_links < 20 AND frac < thres1)
          ) THEN 2
        -- 1: WEAK
        WHEN frac >= 0.03 AND cluster_links >= 2 AND frac < thres2 AND cluster_links < 10 THEN 1
        -- 0: OTHER
        ELSE 0
      END AS assignment_level
    FROM new_patent_thres
  )

  SELECT
    document_id AS family_id,
    n_references_cluster,
    n_citations_cluster,
    n_references_total,
    n_citations_total,
    frac,
    cluster_id,
    assignment_level
  FROM new_patent_assignment
)
