-- Aggregates top paper metadata for each cluster
WITH
core_stats AS (
  SELECT DISTINCT
    family_id,
    cluster_id,
    core_title AS title,
    "https://patents.google.com/patent/" || REPLACE(patent_id, "-", "") as google_patent,
    year,
    core_stat,
    citations,
    NULL AS citation_rank,
    core_rank,
  FROM
    staging_patent_clusters.core
),

most_cited_stats AS (
  SELECT
    family_id,
    cluster_id,
    most_cited_title AS title,
    "https://patents.google.com/patent/" || REPLACE(patent_id, "-", "") as google_patent,
    year,
    NULL AS core_stat,
    citations,
    citation_rank,
    NULL AS core_rank
  FROM
    staging_patent_clusters.most_cited_patent_families
),

all_cluster_patents AS (
    SELECT
      family_id,
      cluster_id
    FROM
      core_stats
    UNION DISTINCT
    SELECT
      family_id,
      cluster_id
    FROM
      most_cited_stats
),

paper_stats AS (
  SELECT DISTINCT
    all_cluster_patents.family_id,
    all_cluster_patents.cluster_id,
    most_cited_stats.citation_rank,
    core_stats.core_rank,
    COALESCE(core_stats.title,
      most_cited_stats.title) AS title,
    COALESCE(core_stats.google_patent,
      most_cited_stats.google_patent) AS google_patent,
    COALESCE(core_stats.year,
      most_cited_stats.year) AS year,
    COALESCE(core_stats.core_stat,
      most_cited_stats.core_stat) AS core_stat,
    COALESCE(core_stats.citations,
      most_cited_stats.citations) AS citations
  FROM
    all_cluster_patents
  LEFT JOIN
    core_stats
    USING
      (family_id,
        cluster_id)
  LEFT JOIN
    most_cited_stats
    USING
      (family_id,
        cluster_id)
)

SELECT
  cluster_id,
  ARRAY_AGG(STRUCT(family_id,
      title,
      google_patent,
      year,
      core_stat,
      citations,
      citation_rank,
      core_rank,
      COALESCE(citation_rank <= 10,
        FALSE) AS is_most_cited,
      COALESCE(core_rank <= 10,
        FALSE) AS is_core
  )) AS stats
FROM
  paper_stats
WHERE
  ((citation_rank <= 10)
  OR (core_rank <= 10))
  AND year IS NOT NULL
GROUP BY
  cluster_id
