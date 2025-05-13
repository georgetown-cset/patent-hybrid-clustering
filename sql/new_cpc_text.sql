-- Get CPC text for all the new patents being added to the clustering

/*
Goal: get CPC text where available, or substitute IPC matches when not available.
*/
WITH stage AS (
  SELECT
    COALESCE(new_metadata.family_id, "X-" || patent_id) AS family_id,
    patent_id,
    REPLACE(cpcs, ' ', '') AS cpcs,
    REPLACE(ipcs, ' ', '') AS ipcs
  FROM staging_patent_clusters.new_metadata
  INNER JOIN unified_patents.classifications USING (patent_id)
  LEFT JOIN UNNEST(cpcs) AS cpcs
  LEFT JOIN UNNEST(ipcs) AS ipcs
),

text_join AS (
  SELECT
    family_id,
    patent_id,
    cpc_td.code AS cpc_code,
    cpc_td.level AS cpc_level,
    cpc_td.text AS cpc_text,
    RANK() OVER(PARTITION BY family_id ORDER BY cpc_td.level DESC) AS cpc_rank,
    ipc_td.code AS ipc_code,
    ipc_td.level AS ipc_level,
    ipc_td.text AS ipc_text,
    RANK() OVER (PARTITION BY family_id ORDER BY ipc_td.level DESC) AS ipc_rank
  FROM stage
  LEFT JOIN cpc_codes.all_cpc_titles AS cpc_td ON(cpcs = cpc_td.code)
  LEFT JOIN cpc_codes.all_cpc_titles AS ipc_td ON(ipcs = ipc_td.code)
  WHERE (cpc_td.code IS NOT NULL OR ipc_td.code IS NOT NULL)
),

cpc_selection AS (
  SELECT
    family_id,
    STRING_AGG(DISTINCT cpc_text, ". ") AS cpc_text
  FROM
    text_join
  WHERE
    cpc_rank = 1
  GROUP BY family_id
),

ipc_selection AS (
  SELECT
    family_id,
    STRING_AGG(DISTINCT ipc_text, ". ") AS ipc_text
  FROM
    text_join
  WHERE
    ipc_rank = 1
  GROUP BY family_id
)

SELECT
  family_id,
  COALESCE(cpc_text, ipc_text) AS text
FROM ipc_selection
FULL OUTER JOIN cpc_selection USING (family_id)
