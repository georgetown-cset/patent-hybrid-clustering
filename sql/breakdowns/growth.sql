WITH
/* get relevant dates for analysis */
patent_dates AS (
  SELECT
    COALESCE(family_id,
      "X-" || patent_id) AS family_id,
    MIN(EXTRACT(YEAR FROM grant_date)) AS grant_year,
    MIN(grant_date) AS grant_date,
    MAX(EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM dates.first_priority_date)) AS age
  FROM
    unified_patents.dates
  GROUP BY 1
),

--get patent family counts
family_counts AS (
  SELECT
    family_id,
    cluster_assignment.cluster_id,
    COUNT(DISTINCT family_id) OVER (PARTITION BY cluster_assignment.cluster_id) AS NPF_all_years,
    IF(patents_last_10_years.family_id IS NULL, 0, 1) AS last_ten_years
  FROM staging_patent_clusters.cluster_assignment
  LEFT JOIN staging_patent_clusters.patents_last_10_years USING (family_id)
),

year_ranges AS (
  /* calculate publication periods */
  SELECT
    family_id,
    age,
    grant_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS cur_year,
    DATETIME_SUB(CURRENT_DATE(), INTERVAL 5 YEAR) AS five,
    /* patents up to a year from the current date */
    IF(grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), 1, 0) AS year_n,
    /* patents 1-2 years from the current date */
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), 1, 0) AS year_n_1,
    /* patents 2-3 years from the current date */
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), 1, 0) AS year_n_2,
    /* patents 3-4 years from the current date */
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 4 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 3 YEAR), 1, 0) AS year_n_3,
    /* patents 4-5 years from the current date */
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 5 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 4 YEAR), 1, 0) AS year_n_4,
    /* patents 5-6 years from the current date */
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 6 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), 1, 0) AS year_n_5,
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 6 YEAR), 1, 0) AS year_n_6,
    IF( grant_date > DATETIME_SUB(CURRENT_DATE(), INTERVAL 8 YEAR)
      AND grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 7 YEAR), 1, 0) AS year_n_7,
    IF( grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), 1, 0) AS pre_year_n,
    IF( grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 2 YEAR), 1, 0) AS pre_year_n_1,
    IF( grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 3 YEAR), 1, 0) AS pre_year_n_2,
    IF( grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 4 YEAR), 1, 0) AS pre_year_n_3,
    IF( grant_date < DATETIME_SUB(CURRENT_DATE(), INTERVAL 5 YEAR), 1, 0) AS pre_year_n_4
  FROM patent_dates
),

/* get global number of patents*/
global_shares AS (
  SELECT DISTINCT
    SUM(year_n) AS g_year_n,
    SUM(year_n_1) AS g_year_n_1,
    SUM(year_n_2) AS g_year_n_2,
    SUM(year_n_3) AS g_year_n_3,
    SUM(year_n_4) AS g_year_n_4,
    SUM(year_n_5) AS g_year_n_5
  FROM family_counts
  INNER JOIN year_ranges USING (family_id)
),

growth_calc AS ( /* calculate number of patents in each year */
  SELECT DISTINCT
    cluster_id,
    AVG(IF(age < 1, 0, age)) AS age,
    AVG(1 / (IF(age < 1, 1, age))) AS patent_vit,
    NPF_all_years,
    SUM(last_ten_years) AS NPF_last_10_year,
    SUM(year_n) AS year_n,
    SUM(year_n_1) AS year_n_1,
    SUM(year_n_2) AS year_n_2,
    SUM(year_n_3) AS year_n_3,
    SUM(year_n_4) AS year_n_4,
    SUM(year_n_5) AS year_n_5,
    SUM(year_n_6) AS year_n_6,
    SUM(year_n_7) AS year_n_7
  FROM family_counts
  LEFT JOIN year_ranges USING (family_id)
  GROUP BY cluster_id, NPF_all_years
),

/* add global share to local shares and calculate growth */
growth_measure AS ( /* growth in the number growth_1yr of papers */
  SELECT
    cluster_id,
    /* we add 1 to denominator to prevent division by zero*/
    /* yrly growth refers to the different in the number of publications between 2 12-month periods. */
    (year_n - year_n_1 + 1) / (year_n_1 + 1) AS growth_1yrly,
    (1 + year_n_1 - year_n_2) / (year_n_2 + 1) AS growth_2yrly,
    (1 + year_n_2 - year_n_3) / (year_n_3 + 1) AS growth_3yrly,
    (1 + year_n_3 - year_n_4) / (year_n_4 + 1) AS growth_4yrly,
    (1 + year_n_4 - year_n_5) / (year_n_5 + 1) AS growth_5yrly,
    /* growth in shares (no growth is rc growth with the same rate as a science as whole */
    ((year_n + 1) / g_year_n - year_n_1 / g_year_n_1) / (( year_n_1 + 1) / g_year_n_1) AS share_growth_1yrly,
    ((year_n_1 + 1) / g_year_n_1 - year_n_2 / g_year_n_2) / ((year_n_2 + 1) / g_year_n_2) AS share_growth_2yrly,
    ((year_n_2 + 1) / g_year_n_2 - year_n_3 / g_year_n_3) / ((year_n_3 + 1) / g_year_n_3) AS share_growth_3yrly,
    ((year_n_3 + 1) / g_year_n_3 - year_n_4 / g_year_n_4) / ((year_n_4 + 1) / g_year_n_4) AS share_growth_4yrly,
    ((year_n_4 + 1) / g_year_n_4 - year_n_5 / g_year_n_5) / ((year_n_5 + 1) / g_year_n_5) AS share_growth_5yrly,
    (1 + year_n) / ((1 + year_n_1 + year_n_2 + year_n_3 + year_n_4 + year_n_5)) / 5 AS mean_growth_year_n,
    (1 + year_n_1) / ((1 + year_n_2 + year_n_3 + year_n_4 + year_n_5 + year_n_6)) / 5 AS mean_growth_year_n_1,
    (1 + year_n_2) / ((1 + year_n_3 + year_n_4 + year_n_5 + year_n_6 + year_n_7)) / 5 AS mean_growth_year_n_2
  FROM
    growth_calc,
    global_shares
),

/* calculate growth averages between for 1-year, 3-year and 5-year growth periods */
growth_measure_2 AS (
  SELECT
    cluster_id,
    mean_growth_year_n,
    mean_growth_year_n_1,
    mean_growth_year_n_2,
    GREATEST(mean_growth_year_n, mean_growth_year_n_1, mean_growth_year_n_2) AS mean_growth_3_year,
    growth_1yrly AS growth_1yr,
    share_growth_1yrly AS share_growth_1yr,
    (growth_1yrly + growth_2yrly + growth_3yrly) / 3 AS growth_3yr,
    (share_growth_1yrly + share_growth_2yrly + share_growth_3yrly) / 3 AS share_growth_3yr,
    (growth_1yrly + growth_2yrly + growth_3yrly + growth_4yrly + growth_5yrly) / 5 AS growth_5yr,
    (
      share_growth_1yrly + share_growth_2yrly + share_growth_3yrly + share_growth_4yrly + share_growth_5yrly
    ) / 5 AS share_growth_5yr
  FROM growth_measure
),

/* growth percentile */
growth_perc AS (
  SELECT
    cluster_id,
    growth_1yr,
    growth_3yr,
    growth_5yr,
    share_growth_1yr,
    share_growth_3yr,
    share_growth_5yr,
    mean_growth_3_year,
    PERCENT_RANK() OVER (ORDER BY growth_1yr) AS growth_1yr_p_rank,
    PERCENT_RANK() OVER (ORDER BY growth_3yr) AS growth_3yr_p_rank,
    PERCENT_RANK() OVER (ORDER BY growth_5yr) AS growth_5yr_p_rank,
    PERCENT_RANK() OVER (ORDER BY mean_growth_3_year) AS mean_growth_3_year_p_rank
  FROM growth_measure_2
)

/* merge all the tables */
SELECT
  *
FROM growth_calc
LEFT JOIN growth_measure USING (cluster_id)
LEFT JOIN growth_perc USING (cluster_id)
