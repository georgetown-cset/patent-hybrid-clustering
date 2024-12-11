-- Get dummy families
WITH
families_with_dummies AS (
  SELECT
    patent_id,
    COALESCE(family_id,
      "X-" || patent_id) AS family_id
  FROM
    unified_patents.links
),

-- Get patent clusters, with all patent ids in the families
clusters AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    cluster_assignment.cluster_id
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    families_with_dummies
    USING
      (family_id)
),

new_pat AS (
  -- assume 150 DAYS lag between expected publication and expected import date
  -- include any patents from the last 10 years
  -- (in papers we do 5 but patents have a longer lag)
  SELECT
    *
  FROM
    clusters
  INNER JOIN (
    SELECT
      patent_id,
      first_priority_date,
      EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM first_priority_date) AS age
    FROM
      unified_patents.dates
    WHERE
      DATE_DIFF(first_priority_date, DATE_SUB(CURRENT_DATE(), INTERVAL 3800 DAY), DAY) > 0)
    USING
      (patent_id)
),

-- Link weights to families so we can count all
new_cit AS (
  SELECT
    clusters.family_id AS id,
    family_reference AS ref_id
  FROM
    clusters
  LEFT JOIN
    staging_patent_clusters.combined_weights
    USING
      (family_id)
  LEFT JOIN
    unified_patents.links
    ON (family_reference = links.family_id)
  WHERE
    links.patent_id IN (
      SELECT patent_id
      FROM
        new_pat)
    AND clusters.patent_id IN (
      SELECT patent_id
      FROM
        new_pat)
),

-- link references to cluster ids
linked_cits AS (
  SELECT
    id,
    ref_id,
    cluster_id AS clust1
  FROM
    new_cit
  LEFT JOIN
    clusters
    ON
      clusters.family_id = new_cit.id
),

-- merge references with clusters. Calculate same cluster statistics.
new_cit_clust AS (
  SELECT DISTINCT
    id,
    ref_id,
    clust1,
    cluster_id AS clust2,
    IF(clust1 = cluster_id, 1, 0) AS same_clust
  FROM
    linked_cits
  LEFT JOIN
    clusters
    ON
      linked_cits.ref_id = clusters.family_id
),

-- get number of links within clusters and in total for each family_id
Nlinks AS (
  SELECT
    id,
    SUM(same_clust) AS same_clust_N,
    COUNT(*) AS total_linksN
  FROM (
    SELECT
      id,
      clust1 AS cluster_id,
      same_clust
    FROM
      new_cit_clust
    UNION ALL
    SELECT
      ref_id AS id,
      clust2 AS cluster_id,
      same_clust
    FROM
      new_cit_clust
  )
  GROUP BY
    id
),

-- get citation counts by ref_id
ncit_counts AS (
  SELECT
    ref_id,
    COUNT(id) AS Ncit
  FROM
    new_cit
  GROUP BY
    ref_id
),

-- clean up link counts
links_count AS (
  SELECT
    id,
    same_clust_N,
    IF(total_linksN = 0, 1, total_linksN) AS total_linksN,
    Ncit
  FROM
    Nlinks
  LEFT JOIN
    ncit_counts
    ON
      Nlinks.id = ncit_counts.ref_id
),

-- prepare data for core calculation
Links_cit AS (
  SELECT
    id,
    same_clust_N,
    total_linksN,
    Ncit,
    IF(age < 0, 0, age) AS age
  FROM
    links_count
  LEFT JOIN
    new_pat
    ON
      links_count.id = new_pat.family_id
),

-- get share of links in same cluster for each id and cluster
Link_cit_clust AS (
  SELECT
    id,
    cluster_id,
    same_clust_N,
    total_linksN,
    same_clust_N / total_linksN AS same_clust_share,
    Ncit,
    age
  FROM
    Links_cit
  LEFT JOIN
    clusters
    ON
      Links_cit.id = clusters.family_id
),

-- calculate core statistic
core AS (
  SELECT DISTINCT
    id,
    cluster_id,
    (same_clust_N * same_clust_share * LOG(Ncit + 1) / (age + 1)) AS core_stat
  FROM
    Link_cit_clust
),

-- calculate top10 core
top_core AS (
  SELECT
    *
  FROM (
    SELECT
      cluster_id,
      id,
      core_stat,
      ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY core_stat DESC) AS core_rank
    FROM
      core
    WHERE
      core_stat IS NOT NULL
  )
  WHERE
    core_rank < 11
),

-- Get initial title and date info
title_date_info AS (
  SELECT
    patent_id,
    clusters.family_id,
    title,
    title_original,
    IF(priority_year IS NULL, '', CAST(priority_year AS STRING)) AS priority_year,
    patent_date
  FROM
    unified_patents.metadata
  LEFT JOIN (
    SELECT
      patent_id,
      family_id,
      EXTRACT(YEAR
        FROM
        first_priority_date) AS priority_year,
      COALESCE(first_priority_date, application_date, publication_date) AS patent_date
    FROM
      unified_patents.dates
  )
  USING
    (patent_id)
  LEFT JOIN
    clusters
    USING
      (patent_id)
  WHERE title IS NOT NULL
    OR title_original IS NOT NULL
),

get_ordering AS (
  SELECT
    family_id,
    patent_id,
    title,
    title_original,
    priority_year,
    patent_date,
    RANK() OVER (
      PARTITION BY family_id ORDER BY patent_date ASC, LENGTH(COALESCE(title, title_original)) DESC
    ) AS ordering
  FROM
    title_date_info
),

-- Group title and year by family
family_title_date AS (
  SELECT
    family_id,
    COALESCE(title, title_original) AS title,
    priority_year
  FROM
    get_ordering
  WHERE ordering = 1
),

-- prepare titles for cit
title_core AS (
  SELECT
    family_id,
    CONCAT( IF(title IS NULL, '', title), ', ', priority_year ) AS core_title
  FROM
    family_title_date
)

SELECT DISTINCT
  cluster_id,
  id AS family_id,
  core_stat,
  core_rank,
  core_title
FROM
  top_core
LEFT JOIN
  title_core
  ON
    top_core.id = title_core.family_id
ORDER BY
  cluster_id,
  core_rank
