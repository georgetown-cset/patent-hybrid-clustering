-- Find top academic assignees by cluster

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
    ror_id
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
    assignee AS academic_assignee,
    ror_id
  FROM
    clusters
  LEFT JOIN (
    SELECT
      patent_id,
      family_id,
      assignee,
      ror_id
    FROM
      priority_assignees
    WHERE
      ror_id IN (
        SELECT id
        FROM
          gcp_cset_ror.ror,
          UNNEST(types) AS org_type
        WHERE
          org_type = "Education")
    )
    USING
      (patent_id)
),

-- aggregate
assignee_rank_tab AS (
  SELECT
    cluster_id,
    academic_assignee,
    COUNT(DISTINCT family_id) AS NPF_academic_assignee,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(DISTINCT family_id) DESC) AS academic_assignee_rank
  FROM (
    SELECT
      patent_id,
      family_id,
      cluster_id,
      academic_assignee,
      ror_id
    FROM
      assignees
  )
  WHERE
    academic_assignee IS NOT NULL
  GROUP BY
    cluster_id,
    academic_assignee
),

-- get top 10 assignees
top10_tab AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF_top10_academic_assignees
  FROM
    assignees
  INNER JOIN (
    SELECT
      *
    FROM
      assignee_rank_tab
    WHERE
      academic_assignee_rank <= 10
  )
  USING
    (cluster_id,
      academic_assignee)
  GROUP BY
    cluster_id
),

-- get missing assignees
miss_assignee_tab AS (
  SELECT
    cluster_id,
    SUM(miss_org) AS NPF_missing_all_academic_assignees
  FROM (
    SELECT
      cluster_id,
      family_id,
      MIN(
        IF(academic_assignee IS NULL, 1, 0)) AS miss_org
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
  academic_assignee,
  academic_assignee_rank,
  NPF_academic_assignee,
  NPF_top10_academic_assignees,
  NPF_missing_all_academic_assignees,
  NPF
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM (
      SELECT
        *
      FROM
        clust_size
      LEFT JOIN
        assignee_rank_tab
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
  )
ORDER BY
  cluster_id,
  academic_assignee_rank
