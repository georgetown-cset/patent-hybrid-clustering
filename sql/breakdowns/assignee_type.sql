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
family_assignee_ror AS (
  SELECT
    patent_id,
    clusters.family_id,
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
    family_assignee_ror.family_id,
    MIN(application_date) AS priority_date
  FROM
    family_assignee_ror
  LEFT JOIN
    unified_patents.dates
  USING
    (patent_id)
  GROUP BY
    family_id
),

-- find priority assignee ror id
priority_assignee_ror AS (
  SELECT
    patent_id,
    family_assignee_ror.family_id,
    ror_id
  FROM
    family_assignee_ror
  LEFT JOIN
    unified_patents.dates
  USING
    (patent_id)
  LEFT JOIN
    backup_date
  ON
    (family_assignee_ror.family_id = backup_date.family_id)
  WHERE
    application_date = first_priority_date
    OR application_date = priority_date
),

-- merge assignee ror id and clusters
assignee_ror AS (
  SELECT
    patent_id,
    clusters.family_id,
    cluster_id,
    ror_id
  FROM
    clusters
  LEFT JOIN
    priority_assignee_ror
  USING
    (patent_id)
),

org_type AS (
  SELECT
    *
  FROM (
    SELECT
      patent_id,
      family_id,
      cluster_id,
      ror_id
    FROM
      assignee_ror
    ) clust
  LEFT JOIN (
    SELECT
      DISTINCT id,
    IF
      (type = "Company", 1, 0) AS Company_auth,
    IF
      (type = "Education", 1, 0) AS Education_auth,
    IF
      (type = "Nonprofit" OR type = "Healthcare" OR type = "Facility" OR type = "Other", 1, 0) AS Nonprofit_auth,
    IF
      (type = "Archive" OR type = "Government", 1, 0) AS Government_auth
    FROM
      gcp_cset_ror.ror
    CROSS JOIN
      UNNEST(types) AS type
    ) ror_types
  ON
    clust.ror_id = ror_types.id
),

-- Check for missing
add_miss_org AS (
  SELECT
    DISTINCT cluster_id,
    SUM(missing_type) AS NPF_missing_all_assignee_types
  FROM (
    SELECT
      DISTINCT family_id,
      cluster_id,
    IF
      (sum_val IS NULL, 1, 0) AS missing_type
    FROM (
      -- if all orgs are missing in the family_id the sum_val is Null
      SELECT
        DISTINCT family_id,
        cluster_id,
        SUM(Company_auth) AS sum_val
      FROM
        org_type
      GROUP BY
        family_id,
        cluster_id
      )
    )
  GROUP BY
    cluster_id
),

agg_org AS (
  -- export data
  SELECT
    DISTINCT cluster_id,
    Company/check_sum AS Company,
    Education/check_sum AS Education,
    Nonprofit/check_sum AS Nonprofit,
    Government/check_sum AS Government
  FROM (
    SELECT
      DISTINCT cluster_id,
      SUM(Company_auth) AS Company,
      SUM(Education_auth) AS Education,
      SUM(Nonprofit_auth) AS Nonprofit,
      SUM(Government_auth) AS Government,
      SUM(Company_auth+Education_auth+Nonprofit_auth+Government_auth) AS check_sum
    FROM
      org_type
    GROUP BY
      cluster_id
    )
  WHERE
    check_sum > 0
)

SELECT
  *
FROM (
  SELECT
    *
  FROM
    agg_org
  LEFT JOIN
    add_miss_org
  USING
    (cluster_id)
  )
LEFT JOIN (
  SELECT
    COUNT(DISTINCT family_id) AS NPF,
    cluster_id
  FROM
    clusters
  GROUP BY
    cluster_id
  )
USING
  (cluster_id)
ORDER BY
  cluster_id