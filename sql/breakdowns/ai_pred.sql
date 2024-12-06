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

/* AI prediction */ ai_tab AS (
  SELECT
    patent_id,
    family_id,
    -- If the patent is in the AI table, it's AI
    1 as ai,
    CAST(Physical_Sciences_and_Engineering AS INT64) AS Physical_Sciences_and_Engineering,
    CAST(Life_Sciences AS INT64) AS Life_Sciences,
    CAST(Security__eg_cybersecurity AS INT64) AS Security__eg_cybersecurity,
    CAST(Transportation AS INT64) AS Transportation,
    CAST(Industrial_and_Manufacturing AS INT64) AS Industrial_and_Manufacturing,
    CAST(Education AS INT64) AS Education,
    CAST(Document_Mgt_and_Publishing AS INT64) AS Document_Mgt_and_Publishing,
    CAST(Military AS INT64) AS Military,
    CAST(Agricultural AS INT64) AS Agricultural,
    CAST(Computing_in_Government AS INT64) AS Computing_in_Government,
    CAST(Personal_Devices_and_Computing AS INT64) AS Personal_Devices_and_Computing,
    CAST(Banking_and_Finance AS INT64) AS Banking_and_Finance,
    CAST(Telecommunications AS INT64) AS Telecommunications,
    CAST(Networks__eg_social_IOT_etc AS INT64) AS Networks__eg_social_IOT_etc,
    CAST(Business AS INT64) AS Business,
    CAST(Energy_Management AS INT64) AS Energy_Management,
    CAST(Entertainment AS INT64) AS Entertainment,
    CAST(Nanotechnology AS INT64) AS Nanotechnology,
    CAST(Semiconductors AS INT64) AS Semiconductors,
    CAST(Language_Processing AS INT64) AS Language_Processing,
    CAST(Speech_Processing AS INT64) AS Speech_Processing,
    CAST(Knowledge_Representation AS INT64) AS Knowledge_Representation,
    CAST(Planning_and_Scheduling AS INT64) AS Planning_and_Scheduling,
    CAST(Control AS INT64) AS Control,
    CAST(Distributed_AI AS INT64) AS Distributed_AI,
    CAST(Robotics AS INT64) AS Robotics,
    CAST(Computer_Vision AS INT64) AS Computer_Vision,
    CAST(Analytics_and_Algorithms AS INT64) AS Analytics_and_Algorithms,
    CAST(Measuring_and_Testing AS INT64) AS Measuring_and_Testing,
    CAST(Logic_Programming AS INT64) AS Logic_Programming,
    CAST(Fuzzy_Logic AS INT64) AS Fuzzy_Logic,
    CAST(Probabilistic_Reasoning AS INT64) AS Probabilistic_Reasoning,
    CAST(Ontology_Engineering AS INT64) AS Ontology_Engineering,
    CAST(Machine_Learning AS INT64) AS Machine_Learning,
    CAST(Search_Methods AS INT64) AS Search_Methods
  FROM
    unified_patents.ai_patents
),

/* Merge clusters and AI predictions, including each only once per family id */
merged AS (
  SELECT
    DISTINCT
    clusters.family_id,
    cluster_id,
    COALESCE(ai, 0) as ai,
    Physical_Sciences_and_Engineering,
    Life_Sciences,
    Security__eg_cybersecurity,
    Transportation,
    Industrial_and_Manufacturing,
    Education,
    Document_Mgt_and_Publishing,
    Military,
    Agricultural,
    Computing_in_Government,
    Personal_Devices_and_Computing,
    Banking_and_Finance,
    Telecommunications,
    Networks__eg_social_IOT_etc,
    Business,
    Energy_Management,
    Entertainment,
    Nanotechnology,
    Semiconductors,
    Language_Processing,
    Speech_Processing,
    Knowledge_Representation,
    Planning_and_Scheduling,
    Control,
    Distributed_AI,
    Robotics,
    Computer_Vision,
    Analytics_and_Algorithms,
    Measuring_and_Testing,
    Logic_Programming,
    Fuzzy_Logic,
    Probabilistic_Reasoning,
    Ontology_Engineering,
    Machine_Learning,
    Search_Methods
  FROM
    clusters
  LEFT JOIN
    ai_tab
  USING
    (patent_id)
)

SELECT
  cluster_id,
  SUM(ai)/NULLIF(COUNT(family_id), 0) AS pred_ai,
  SUM(Physical_Sciences_and_Engineering)/NULLIF(COUNT(family_id), 0) AS Physical_Sciences_and_Engineering_pred,
  SUM(Life_Sciences)/NULLIF(COUNT(family_id), 0) AS Life_Sciences_pred,
  SUM(Security__eg_cybersecurity)/NULLIF(COUNT(family_id), 0) AS Security__eg_cybersecurity_pred,
  SUM(Transportation)/NULLIF(COUNT(family_id), 0) AS Transportation_pred,
  SUM(Industrial_and_Manufacturing)/NULLIF(COUNT(family_id), 0) AS Industrial_and_Manufacturing_pred,
  SUM(Education)/NULLIF(COUNT(family_id), 0) AS Education_pred,
  SUM(Document_Mgt_and_Publishing)/NULLIF(COUNT(family_id), 0) AS Document_Mgt_and_Publishing_pred,
  SUM(Military)/NULLIF(COUNT(family_id), 0) AS Military_pred,
  SUM(Agricultural)/NULLIF(COUNT(family_id), 0) AS Agricultural_pred,
  SUM(Computing_in_Government)/NULLIF(COUNT(family_id), 0) AS Computing_in_Government_pred,
  SUM(Personal_Devices_and_Computing)/NULLIF(COUNT(family_id), 0) AS Personal_Devices_and_Computing_pred,
  SUM(Banking_and_Finance)/NULLIF(COUNT(family_id), 0) AS Banking_and_Finance_pred,
  SUM(Telecommunications)/NULLIF(COUNT(family_id), 0) AS Telecommunications_pred,
  SUM(Networks__eg_social_IOT_etc)/NULLIF(COUNT(family_id), 0) AS Networks__eg_social_IOT_etc_pred,
  SUM(Business)/NULLIF(COUNT(family_id), 0) AS Business_pred,
  SUM(Energy_Management)/NULLIF(COUNT(family_id), 0) AS Energy_Management_pred,
  SUM(Entertainment)/NULLIF(COUNT(family_id), 0) AS Entertainment_pred,
  SUM(Nanotechnology)/NULLIF(COUNT(family_id), 0) AS Nanotechnology_pred,
  SUM(Semiconductors)/NULLIF(COUNT(family_id), 0) AS Semiconductors_pred,
  SUM(Language_Processing)/NULLIF(COUNT(family_id), 0) AS Language_Processing_pred,
  SUM(Speech_Processing)/NULLIF(COUNT(family_id), 0) AS Speech_Processing_pred,
  SUM(Knowledge_Representation)/NULLIF(COUNT(family_id), 0) AS Knowledge_Representation_pred,
  SUM(Planning_and_Scheduling)/NULLIF(COUNT(family_id), 0) AS Planning_and_Scheduling_pred,
  SUM(Control)/NULLIF(COUNT(family_id), 0) AS Control_pred,
  SUM(Distributed_AI)/NULLIF(COUNT(family_id), 0) AS Distributed_AI_pred,
  SUM(Robotics)/NULLIF(COUNT(family_id), 0) AS Robotics_pred,
  SUM(Computer_Vision)/NULLIF(COUNT(family_id), 0) AS Computer_Vision_pred,
  SUM(Analytics_and_Algorithms)/NULLIF(COUNT(family_id), 0) AS Analytics_and_Algorithms_pred,
  SUM(Measuring_and_Testing)/NULLIF(COUNT(family_id), 0) AS Measuring_and_Testing_pred,
  SUM(Logic_Programming)/NULLIF(COUNT(family_id), 0) AS Logic_Programming_pred,
  SUM(Fuzzy_Logic)/NULLIF(COUNT(family_id), 0) AS Fuzzy_Logic_pred,
  SUM(Probabilistic_Reasoning)/NULLIF(COUNT(family_id), 0) AS Probabilistic_Reasoning_pred,
  SUM(Ontology_Engineering)/NULLIF(COUNT(family_id), 0) AS Ontology_Engineering_pred,
  SUM(Machine_Learning)/NULLIF(COUNT(family_id), 0) AS Machine_Learning_pred,
  SUM(Search_Methods)/COUNT(family_id) AS Search_Methods_pred,
FROM
  merged
GROUP BY
  cluster_id
ORDER BY
  cluster_id