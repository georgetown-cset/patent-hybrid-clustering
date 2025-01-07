-- family count should always stay the same or go up
-- it's *possible* this count will fail in six-month increments when we add new family
-- ids from PATSTAT and remove dummy families; if we see it fail after this change
-- this is probably why; since that's a manual update we should be aware
with staging as (
SELECT
  COUNT(DISTINCT family_id) as staging_count
FROM
  staging_patent_clusters.cluster_assignment
),

prod as (
SELECT
  COUNT(DISTINCT family_id) as prod_count
FROM
  patent_clusters.cluster_assignment
)

SELECT
  staging_count >= prod_count
FROM
  prod, staging