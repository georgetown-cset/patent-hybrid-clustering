-- Fraction of total links for each cluster that connect within the same cluster,
-- finds the sum of all links connecting two patents in a cluster divided by
-- the sum of all links with a citation or reference within a cluster
CREATE OR REPLACE TABLE patent_clustering_metrics.cluster_links_fractions_comparison AS (
  WITH
  -- FINDING STATS FOR OLD CLUSTERS
  -- Finding all links in the old clusters and connecting each patent and reference to their assigned clusters
  old_cluster_links AS (
    SELECT
      family_references.family_id AS document_id,
      family_reference AS ref_id,
      c1.cluster_id AS document_cluster,
      c2.cluster_id AS ref_cluster
    FROM staging_patent_clusters.family_references
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped AS c1 ON
        family_references.family_id = c1.family_id
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240726_best_clusters_mapped AS c2 ON
        family_references.family_reference = c2.family_id
  ),

  -- Counting all references for each of the old clusters, as well as within-cluster references
  old_cluster_refs AS (
    SELECT
      document_cluster AS cluster_id,
      SUM(CASE WHEN document_cluster = ref_cluster THEN 1 ELSE 0 END) AS n_within_cluster_ref,
      COUNT(ref_cluster) AS n_cluster_total_ref
    FROM old_cluster_links
    GROUP BY document_cluster
  ),

  -- Counting all citations for each of the old clusters, as well as within-cluster citations
  old_cluster_cits AS (
    SELECT
      ref_cluster AS cluster_id,
      SUM(CASE WHEN document_cluster = ref_cluster THEN 1 ELSE 0 END) AS n_within_cluster_cit,
      COUNT(document_cluster) AS n_cluster_total_cit
    FROM old_cluster_links
    GROUP BY ref_cluster
  ),

  -- Finding the fraction of total links for each cluster that connect within the same cluster
  old_cluster_fracs AS (
    SELECT
      cluster_id,
      COALESCE(
        n_within_cluster_ref + n_within_cluster_cit, n_within_cluster_ref, n_within_cluster_cit, 0
      ) / IF(
        COALESCE(n_cluster_total_ref + n_cluster_total_cit, n_cluster_total_ref, n_cluster_total_cit, 1) = 0,
        1,
        COALESCE(n_cluster_total_ref + n_cluster_total_cit, n_cluster_total_ref, n_cluster_total_cit, 1)
      ) AS frac
    FROM old_cluster_refs
    FULL JOIN old_cluster_cits USING (cluster_id)
    ORDER BY frac DESC
  ),

  -- FINDING STATS FOR NEW CLUSTERS
  -- Finding all links in the new corpus and connecting each article and refernce to their assigned clusters
  new_cluster_links AS (
    SELECT
      family_references.family_id AS document_id,
      family_reference AS ref_id,
      c1.cluster_id AS document_cluster,
      c2.cluster_id AS ref_cluster
    FROM staging_patent_clusters.family_references
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240827_best_clusters_mapped AS c1 ON
        family_references.family_id = c1.family_id
    LEFT JOIN
      patent_cluster_experiments.patent_cluster_intial_experiement_sts_scaling_20240827_best_clusters_mapped AS c2 ON
        family_references.family_reference = c2.family_id
  ),

  -- Counting all references for each of the new clusters, as well as within-cluster references
  new_cluster_refs AS (
    SELECT
      document_cluster AS cluster_id,
      SUM(CASE WHEN document_cluster = ref_cluster THEN 1 ELSE 0 END) AS n_within_cluster_ref,
      COUNT(ref_cluster) AS n_cluster_total_ref
    FROM new_cluster_links
    GROUP BY document_cluster
  ),

  -- Counting all citations for each of the new clusters, as well as within-cluster citations
  new_cluster_cits AS (
    SELECT
      ref_cluster AS cluster_id,
      SUM(CASE WHEN document_cluster = ref_cluster THEN 1 ELSE 0 END) AS n_within_cluster_cit,
      COUNT(document_cluster) AS n_cluster_total_cit
    FROM new_cluster_links
    GROUP BY ref_cluster
  ),

  -- Finding the fraction of total links for each new clutser that connects within the same cluster
  new_cluster_fracs AS (
    SELECT
      cluster_id,
      COALESCE(
        n_within_cluster_ref + n_within_cluster_cit, n_within_cluster_ref, n_within_cluster_cit, 0
      ) / IF(
        COALESCE(n_cluster_total_ref + n_cluster_total_cit, n_cluster_total_ref, n_cluster_total_cit, 1) = 0,
        1,
        COALESCE(n_cluster_total_ref + n_cluster_total_cit, n_cluster_total_ref, n_cluster_total_cit, 1)
      ) AS frac
    FROM new_cluster_refs
    FULL JOIN new_cluster_cits USING (cluster_id)
    ORDER BY frac DESC
  ),

  -- Generating histogram of fractions
  old_histogram AS (
    SELECT
      FLOOR(frac * 100) / 100 AS bin_floor,
      COUNT(cluster_id) AS n_old_clusters
    FROM old_cluster_fracs
    GROUP BY bin_floor
    ORDER BY bin_floor
  ),

  new_histogram AS (
    SELECT
      FLOOR(frac * 100) / 100 AS bin_floor,
      COUNT(cluster_id) AS n_new_clusters
    FROM new_cluster_fracs
    GROUP BY bin_floor
    ORDER BY bin_floor
  )

  SELECT
    bin_floor,
    n_old_clusters,
    n_new_clusters
  FROM old_histogram
  FULL JOIN new_histogram USING (bin_floor)
  ORDER BY bin_floor
)
