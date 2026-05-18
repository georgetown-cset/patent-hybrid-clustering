-- Aggregates metadata to display in the cluster detail view
-- that we haven't already captured in `locations_with_metadata`
WITH
cluster_ids AS (
  SELECT DISTINCT cluster_id
  FROM
    patent_clusters.cluster_locations
),

year_count AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(NPF_cluster AS NPF,
        year)
      ORDER BY
        year ASC) AS npf_info
  FROM
    staging_patent_clusters.priority_year
  GROUP BY
    cluster_id
),

country AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(country,
        NPF_country AS NPF)
      ORDER BY
        country_rank ASC) AS priority_country_info,
    MAX(NPF_missing_all_countries) AS num_missing_priority_countries,
    MAX(NPF_top10_countries) AS num_in_top_10_priority_countries
  FROM
    staging_patent_clusters.priority_country
  GROUP BY
    cluster_id
),

ind_org AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(industry_assignee,
        country,
        NPF_industry_assignee AS NPF)
      ORDER BY
        industry_assignee_rank ASC) AS industry_org_info,
    MAX(NPF_missing_all_industry_assignees) AS num_missing_industry_org,
    MAX(NPF_top10_industry_assignees) AS num_in_top_10_industry_orgs
  FROM
    staging_patent_clusters.industry_assignees
  GROUP BY
    cluster_id
),

aca_org AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(academic_assignee,
        country,
        NPF_academic_assignee AS NPF)
      ORDER BY
        academic_assignee_rank ASC) AS academic_org_info,
    MAX(NPF_missing_all_academic_assignees) AS num_missing_academic_org,
    MAX(NPF_top10_academic_assignees) AS num_in_top_10_academic_orgs
  FROM
    staging_patent_clusters.academic_assignees
  GROUP BY
    cluster_id
),

invent AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(inventor,
        NPF_inventor AS NPF,
        inventor_country)
      ORDER BY
        inventor_rank ASC) AS inventor_info,
    MAX(NPF_top10_inventors) AS num_in_top_10_inventors,
    MAX(NPF_missing_all_inventors) AS num_missing_inventors
  FROM
    staging_patent_clusters.inventors
  GROUP BY
    cluster_id
),

assignee_orgs AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(assignee,
        country,
        NPF_assignee AS NPF)
      ORDER BY
        assignee_rank ASC) AS assignee_org_info,
    MAX(NPF_top10_assignees) AS num_in_top_10_assignees,
    MAX(NPF_missing_all_assignees) AS num_missing_assignees
  FROM
    staging_patent_clusters.assignees
  GROUP BY
    cluster_id
),

assignee_types AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(ROUND(100 * Company, 2) AS Company,
        ROUND(100 * Education, 2) AS Education,
        ROUND(100 * Nonprofit, 2) AS Nonprofit,
        ROUND(100 * Government, 2) AS Government)) AS assignee_type_info,
    MAX(NPF_missing_all_assignee_types) AS assignee_type_missing
  FROM
    staging_patent_clusters.assignee_type
  GROUP BY
    cluster_id
),

cross_file AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(country_a,
        cross_filings_w_counts)) AS cross_filings
  FROM
    staging_patent_clusters.cross_filing
  GROUP BY
    cluster_id
),

cluster_metrics AS (
  SELECT
    cluster_id,
    (SELECT AS STRUCT locations_with_metadata.* EXCEPT(cluster_id)) AS metrics
  FROM
    staging_map_of_patents.locations_with_metadata
),

paper_affiliations AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(
      org_name,
      affiliation_rank,
      NPF_ref_affiliation AS NPF
    )) AS paper_affiliation_info
  FROM
    staging_patent_clusters.paper_affiliations
  WHERE affiliation_rank < 11
  GROUP BY
    cluster_id
),

paper_titles AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(
      most_cited_title AS title,
      citation_rank,
      citations
    )) AS paper_title_info
  FROM
    staging_patent_clusters.most_cited_papers
  WHERE citation_rank < 11
  GROUP BY
    cluster_id
),

research_clusters AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(
      research_cluster_id,
      cluster_title,
      cluster_summary,
      keywords,
      citation_rank,
      citations
    )) AS research_cluster_info
  FROM
    staging_patent_clusters.most_cited_research_clusters
  WHERE citation_rank < 11
  GROUP BY
    cluster_id
)

SELECT
  cluster_ids.cluster_id,
  year_count.npf_info,
  top_patent_stats.stats AS patent_stats,
  # priority country
  country.priority_country_info,
  country.num_missing_priority_countries,
  country.num_in_top_10_priority_countries,
  # industry orgs
  ind_org.industry_org_info,
  ind_org.num_missing_industry_org,
  ind_org.num_in_top_10_industry_orgs,
  # academic orgs
  aca_org.academic_org_info,
  aca_org.num_missing_academic_org,
  aca_org.num_in_top_10_academic_orgs,
  # inventors
  invent.inventor_info,
  invent.num_missing_inventors,
  invent.num_in_top_10_inventors,
  # assignee org affiliations
  assignee_orgs.assignee_org_info,
  assignee_orgs.num_missing_assignees,
  assignee_orgs.num_in_top_10_assignees,
  # assignee type
  assignee_types.assignee_type_info,
  assignee_types.assignee_type_missing,
  # cross filings
  cross_file.cross_filings,
  # multi country counts
  cluster_multi_country_filing_counts.multi_country_count,
  cluster_multi_country_filing_counts.ten_year_count,
  # metrics
  cluster_metrics.metrics,
  # patent assignees
  paper_affiliations.paper_affiliation_info,
  # paper titles
  paper_titles.paper_title_info,
  # research clusters
  research_clusters.research_cluster_info,
  # import export
  top_exporting_clusters_aggregated.exporting_cluster_counts,
  top_importing_clusters_aggregated.importing_cluster_counts

FROM
  cluster_ids
LEFT JOIN
  year_count USING (cluster_id)
LEFT JOIN
  staging_patent_clusters.top_patent_stats USING (cluster_id)
LEFT JOIN
  country USING (cluster_id)
LEFT JOIN
  ind_org USING (cluster_id)
LEFT JOIN
  aca_org USING (cluster_id)
LEFT JOIN
  invent USING (cluster_id)
LEFT JOIN
  assignee_orgs USING (cluster_id)
LEFT JOIN
  assignee_types USING (cluster_id)
LEFT JOIN
  cross_file USING (cluster_id)
LEFT JOIN
  staging_patent_clusters.cluster_multi_country_filing_counts USING (cluster_id)
LEFT JOIN
  cluster_metrics USING (cluster_id)
LEFT JOIN
  paper_affiliations USING (cluster_id)
LEFT JOIN
  paper_titles USING (cluster_id)
LEFT JOIN
  research_clusters USING (cluster_id)
LEFT JOIN
  staging_map_of_patents.top_exporting_clusters_aggregated USING (cluster_id)
LEFT JOIN
  staging_map_of_patents.top_importing_clusters_aggregated USING (cluster_id)
WHERE
  metrics IS NOT NULL
