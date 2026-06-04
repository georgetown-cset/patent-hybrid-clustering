-- Find assignee type proportions by cluster

-- Get patent clusters, with all patent ids in the families
-- patents_last_10_years handles the dummy_family linkage to patents for us
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
    USING
      (family_id)
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
  SELECT DISTINCT
    *
  FROM (
    SELECT
      family_id,
      cluster_id,
      ror_id
    FROM
      assignee_ror
    ) AS clust
  LEFT JOIN (
    SELECT DISTINCT
      id,
      IF(org_type = "company", 1, 0) AS Company_auth,
      IF(org_type = "education", 1, 0) AS Education_auth,
      IF(
        org_type = "nonprofit" OR org_type = "healthcare" OR org_type = "facility" OR org_type = "other", 1, 0
      ) AS Nonprofit_auth,
      IF(org_type = "archive" OR org_type = "government", 1, 0) AS Government_auth
    FROM
      gcp_cset_ror.ror
    CROSS JOIN
      UNNEST(types) AS org_type
    ) AS ror_types
    ON
      clust.ror_id = ror_types.id
),

-- Check for missing
add_miss_org AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT(CASE
      WHEN missing_type = 1 THEN family_id END)) AS NPF_missing_all_assignee_types
  FROM (
    SELECT DISTINCT
      family_id,
      cluster_id,
      IF(sum_val IS NULL, 1, 0) AS missing_type
    FROM (
      -- if all orgs are missing in the family_id the sum_val is Null
      SELECT
        family_id,
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
    cluster_id,
    SUM(Company_auth) / COUNT(Company_auth) AS Company,
    SUM(Education_auth) / COUNT(Education_auth) AS Education,
    SUM(Nonprofit_auth) / COUNT(Nonprofit_auth) AS Nonprofit,
    SUM(Government_auth) / COUNT(Government_auth) AS Government,
    SUM(Company_auth) / COUNT(*) AS Company_incl_miss,
    SUM(Education_auth) / COUNT(*) AS Education_incl_miss,
    SUM(Nonprofit_auth) / COUNT(*) AS Nonprofit_incl_miss,
    SUM(Government_auth) / COUNT(*) AS Government_incl_miss
  FROM
    org_type
  GROUP BY
    cluster_id
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
