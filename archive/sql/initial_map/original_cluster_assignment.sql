-- Get final cluster assignment for the original map
-- After a wave of patent assignments from patents that were pruned during the clustering phase, a second wave
-- of attachments is done. This is because some patents connect to other patents that were not initially assigned
-- to a cluster. Doing this in waves is necessary to connect patents to clusters, then patents that connect to them
-- which now connect to clusters. However, there is a limit to the number of waves we wish to run. Too many waves, and
-- we kind of dilute the information - it may be better to wait for a patent update from unified patents which may
-- contain better citations or more text information, or re-run with FAISS to get their best matches to already
-- assigned patents.

WITH
cluster_sizes AS (
  SELECT
    cluster_id,
    COUNT(*) AS size
  FROM staging_patent_clusters.cluster_assignment_wave1
  GROUP BY cluster_id
),

patent_cluster_weights AS (
  SELECT DISTINCT
    id AS family_id,
    fam.cluster_id AS family_cluster,
    ref_id,
    ref.cluster_id AS ref_cluster,
    weight
  FROM staging_patent_clusters.hybrid_sts_scaled_weights
  LEFT JOIN staging_patent_clusters.cluster_assignment_wave1 AS fam ON id = fam.family_id
  LEFT JOIN staging_patent_clusters.cluster_assignment_wave1 AS ref ON ref_id = ref.family_id
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
FROM staging_patent_clusters.cluster_assignment_wave1
