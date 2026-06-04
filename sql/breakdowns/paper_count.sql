-- Get patent clusters, with all patent ids in the families
WITH
clusters AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    cluster_assignment.cluster_id
  FROM
    staging_patent_clusters.cluster_assignment
  INNER JOIN
    staging_patent_clusters.patents_last_10_years
    USING (family_id)
),

-- Linking patents to their referenced papers
pat_paper_link AS (
  SELECT
    patent_id,
    merged_id AS ref_id
  FROM
    unified_patents.publication_references
),

-- Linking all papers that are cited by any patent in
-- a patent family in the cluster to the cluster
clust_cit_link AS (
  SELECT DISTINCT
    cluster_id,
    family_id,
    ref_id
  FROM
    clusters
  LEFT JOIN
    pat_paper_link
    USING
      (patent_id)
  WHERE ref_id IS NOT NULL
),

/* add cluster ID and calculate the fraction of patent families that cite a paper */
add_cluster_tab AS (
  SELECT
    cluster_assignment.cluster_id,
    COUNT(DISTINCT ref_id) AS N_pubs,
    -- If ref_id is null, that family doesn't cite a paper
    1 - COUNTIF(ref_id IS NULL) / COUNT(DISTINCT family_id) AS frac_citing_pubs
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    clust_cit_link
    USING
      (family_id)
  GROUP BY
    cluster_id
),

add_fields AS (
  SELECT
    cluster_id,
    N_pubs,
    frac_citing_pubs,
    class_cat
  FROM
    add_cluster_tab
  LEFT JOIN
    staging_patent_clusters.patent_class
    USING (cluster_id)
)

SELECT
  cluster_id,
  n_pubs,
  frac_citing_pubs,
  class_cat,
  PERCENT_RANK() OVER (PARTITION BY class_cat ORDER BY frac_citing_pubs) AS n_pubs_percentile
FROM
  add_fields
