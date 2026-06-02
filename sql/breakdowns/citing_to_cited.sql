WITH
year_limit AS (
  SELECT DISTINCT
    family_id,
    extract(YEAR FROM first_priority_date) AS year
  FROM
    unified_patents.dates
  WHERE
    extract(YEAR FROM first_priority_date) >= extract(YEAR FROM current_date()) - 10
),

citing AS (
  --get patents linked to clusters and their references
  SELECT
    cluster_assignment.family_id,
    cluster_id AS citing_cluster,
    metadata.family_id AS cited,
    year
  FROM
    staging_patent_clusters.cluster_assignment
  INNER JOIN
    unified_patents.references
    USING
      (family_id)
  INNER JOIN
    year_limit
    USING
      (family_id)
  INNER JOIN
    unified_patents.metadata
    ON patent_reference = metadata.patent_id
)

--link patents and their clusters to their references and link the references to their respective clusters
SELECT DISTINCT
  citing.family_id,
  year,
  citing_cluster,
  cited,
  cluster_id AS cited_cluster
FROM
  citing
INNER JOIN
  staging_patent_clusters.cluster_assignment
  ON
    cluster_assignment.family_id = cited
