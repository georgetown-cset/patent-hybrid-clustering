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

-- number of patent families in clust
clust_size AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF
  FROM
    clusters
  GROUP BY
    cluster_id
),

-- link assignees to dummy families
family_assignees AS (
  SELECT
    patent_id,
    clusters.family_id,
    assignee,
    ror_id
  FROM
    clusters
  LEFT JOIN
    unified_patents.assignees_normalized
  USING
    (patent_id)
),

-- find backup priority date
backup_date AS (
  SELECT
    family_assignees.family_id,
    MIN(application_date) AS priority_date
  FROM
    family_assignees
  LEFT JOIN
    unified_patents.dates
  USING
    (patent_id)
  GROUP BY
    family_id
),

-- find priority assignees
priority_assignees AS (
  SELECT
    patent_id,
    family_assignees.family_id,
    assignee,
    ror_id,
  FROM
    family_assignees
  LEFT JOIN
    unified_patents.dates
  USING
    (patent_id)
  LEFT JOIN
    backup_date
  ON
    (family_assignees.family_id = backup_date.family_id)
  WHERE
    application_date = first_priority_date
    OR application_date = priority_date
),

-- merge assignees and clusters
assignees AS (
  SELECT
    patent_id,
    clusters.family_id,
    cluster_id,
    assignee,
    ror_id
  FROM
    clusters
  LEFT JOIN
    priority_assignees
  USING
    (patent_id)
),

-- Aggregate.
assignee_rank_tab AS (
  SELECT
    cluster_id,
    assignee,
    COUNT(DISTINCT family_id) AS NPF_assignee,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(DISTINCT family_id) DESC) AS assignee_rank
  FROM (
    SELECT
      patent_id,
      family_id,
      cluster_id,
      assignee,
      ror_id
    FROM
      assignees
    )
  WHERE
    assignee IS NOT NULL
  GROUP BY
    cluster_id,
    assignee
),

-- get top 10 orgs
top10_tab AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF_top10_assignees
  FROM
    assignees
  INNER JOIN (
    SELECT
      *
    FROM
      assignee_rank_tab
    WHERE
      assignee_rank <= 10
    )
  USING
    (cluster_id,
      assignee)
  GROUP BY
    cluster_id
),

-- get missing affiliations
miss_assignee_tab AS (
  SELECT
    cluster_id,
    SUM(miss_org) AS NPF_missing_all_assignees
  FROM (
    SELECT
      DISTINCT cluster_id,
      family_id,
      MIN(
      IF
        (assignee IS NULL, 1, 0)) AS miss_org
    FROM
      assignees
    GROUP BY
      family_id,
      cluster_id
    )
  GROUP BY
    cluster_id
)

-- Get orgs
SELECT
  cluster_id,
  assignee,
  assignee_rank,
  NPF_assignee,
  NPF_top10_assignees,
  NPF_missing_all_assignees
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM
      clust_size
    LEFT JOIN (
      SELECT
        *
      FROM
        assignee_rank_tab)
    USING
      (cluster_id)
    )
  LEFT JOIN
    top10_tab
  USING
    (cluster_id)
  )
LEFT JOIN
  miss_assignee_tab
USING
  (cluster_id)
ORDER BY
  cluster_id,
  assignee_rank