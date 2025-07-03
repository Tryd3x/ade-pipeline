## Case Study 1: Drug Safety Signal Detection for Risk Management

![Case_1](../images/case1.png)

### Business Impact
Early detection prevents costly recalls (average drug recall costs $100M+), protects brand reputation, and enables proactive regulatory communication.

### Question 1.1: Which three drugs have the highest volume of adverse event reports in the last 12 months?
**Purpose**: Identify drugs requiring immediate attention due to high report volume

```sql
SELECT 
  medicinal_product
  , COUNT(*) as total_reports
FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction`
WHERE 
    -- Partitioned column
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    -- Filters
    AND medicinal_product IS NOT NULL
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
GROUP BY 
    medicinal_product
HAVING 
    total_reports >= 100
ORDER BY 
    total_reports DESC
LIMIT 3;
```

![Answer 1.1](../images/case1_1.png)

### Question 1.2: What percentage of reports are classified as serious (death and hospitalization) for each of the three high-volume (reports) drug?
**Purpose**: Assess severity patterns to prioritize safety investigations

```sql
WITH
-- Top 3 drugs with highest volume of reports in the past 12 months
high_volume_drugs AS (
  SELECT 
    medicinal_product,
    COUNT(*) AS total_reports
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND medicinal_product IS NOT NULL
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    medicinal_product
  HAVING 
    total_reports >= 100
  ORDER BY 
    total_reports DESC
  LIMIT 3
)
-- Analysis of reports based on serious_type
, event_reports AS (
  SELECT 
    p.medicinal_product
    , COUNT(*) AS total_reports
    , SUM(IF(serious_type IN ('Death', 'Hospitalization'), 1, 0)) AS serious_reports
    -- Calculate serious event rate (key regulatory metric)
    , ROUND(SAFE_DIVIDE(SUM(IF(serious_type IN ('Death', 'Hospitalization'), 1, 0)) * 100.0, COUNT(*)), 2) AS serious_rate
    -- Break down by specific serious event types
    , ROUND(SAFE_DIVIDE(SUM(IF(serious_type = 'Death', 1, 0)) * 100.0, COUNT(*)), 2) AS death_rate
    , ROUND(SAFE_DIVIDE(SUM(IF(serious_type = 'Hospitalization', 1, 0)) * 100.0, COUNT(*)), 2) AS hospitalization_rate
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction` p
    JOIN high_volume_drugs h ON p.medicinal_product = h.medicinal_product
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND p.medicinal_product IS NOT NULL
    AND fda_last_updated >= CURRENT_DATE() - INTERVAL 1 YEAR
  GROUP BY 
    p.medicinal_product
  HAVING 
    total_reports >= 100
)
SELECT
  medicinal_product
  , death_rate
  , hospitalization_rate
FROM event_reports
```

![Answer 1.2](../images/case1_2.png)

### Question 1.3: Are adverse event reports increasing over the months for each of the high-volume (reports) drugs?
**Purpose**: Detect emerging safety signals through trend analysis

```sql
-- Top 3 drugs with highest volume of reports in the past 12 months
WITH high_volume_drugs AS (
  SELECT 
    medicinal_product,
    COUNT(*) as total_reports
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND medicinal_product IS NOT NULL
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    medicinal_product
  HAVING 
    total_reports >= 100
  ORDER BY 
    total_reports DESC
  LIMIT 3
)
-- Fetch monthly report count and monthly serious count for each of the three high-volume reports drugs
, monthly_reports AS (
  SELECT 
    p.medicinal_product
    , DATE_TRUNC(fda_last_updated, MONTH) AS report_month
    , COUNT(*) AS monthly_count
    , SUM(IF(serious_type IN ('Death', 'Hospitalization'),1,0)) AS monthly_serious
    , ROUND(SAFE_DIVIDE(100 * SUM(IF(serious_type IN ('Death', 'Hospitalization'),1,0)),COUNT(*)),2) AS serious_rate
  FROM  
    `ade-pipeline.ade_dev_core.patient_drug_reaction` p
    JOIN `high_volume_drugs` h ON p.medicinal_product = h.medicinal_product
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    1, 2
  HAVING
    -- Ensure statistical significance for trend analysis
    monthly_count >= 50
    AND monthly_serious >= 10
)
-- Create pivot table showing serious event rates by month for easy comparison
SELECT 
  report_month
  , IFNULL(actemra, 0) AS actemra
  , IFNULL(methotrexate, 0) AS methotrexate
  , IFNULL(prednisone, 0) AS prednisone
FROM (
  SELECT 
    report_month
    , medicinal_product
    , serious_rate
  FROM monthly_reports
)
PIVOT (
  MAX(serious_rate)
  FOR medicinal_product IN ('actemra', 'methotrexate', 'prednisone')
)
```

![Answer 1.3](../images/case1_3-4.png)

### Question 1.4: Detect unusually high rates of expedited reports among the high-volume drugs?
**Purpose**: Identify drugs with urgent safety concerns requiring immediate regulatory attention

```sql
-- Top 3 drugs with highest volume of reports in the past 12 months
WITH
high_volume_drugs AS (
  SELECT 
    medicinal_product,
    COUNT(*) as total_reports
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND medicinal_product IS NOT NULL
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    medicinal_product
  HAVING 
    total_reports >= 100
  ORDER BY 
    total_reports DESC
  LIMIT 3
)
-- Fetch reports associated with serious events and calculate percentage of expedited reports
, expedited_analysis AS (
  SELECT 
    p.medicinal_product
    , DATE_TRUNC(fda_last_updated, MONTH) AS report_month
    , COUNT(*) as total_reports
    , ROUND(
        SAFE_DIVIDE(
          SUM(
            IF(expedited_process = 'Yes' AND serious_type IN ('Death', 'Hospitalization'), 1, 0)
          ) * 100.0
        , COUNT(*)
      ), 2) as expedited_rate
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction` p
    JOIN `high_volume_drugs` h ON p.medicinal_product = h.medicinal_product
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    1, 2
  HAVING 
    total_reports >= 100
)
-- Previous Month Expedited Rate 
, expedited_trends AS (
  SELECT
    medicinal_product
    , report_month
    , expedited_rate
    , LAG(expedited_rate) OVER (
        PARTITION BY medicinal_product 
        ORDER BY report_month
      ) AS prev_expedited_rate
  FROM expedited_analysis
),
-- Calculate percentage change in expedited reporting rate
delta_percent AS (
  SELECT
    medicinal_product
    , report_month
    , ROUND(
        SAFE_DIVIDE(
          (expedited_rate - prev_expedited_rate) * 100
          , prev_expedited_rate
        )
      , 2) AS expedited_rate_delta_percent
  FROM expedited_trends
)
-- Create pivot showing month-over-month percentage changes in expedited reporting
SELECT
  report_month
  , IFNULL(actemra, 0) AS actemra
  , IFNULL(methotrexate, 0) AS methotrexate
  , IFNULL(prednisone, 0) AS prednisone
FROM (
  SELECT
    medicinal_product
    , report_month
    , expedited_rate_delta_percent
  FROM delta_percent
)
PIVOT (
  MAX(expedited_rate_delta_percent)
  FOR medicinal_product IN ('actemra', 'methotrexate', 'prednisone')
)
ORDER BY report_month ASC;

```

![Answer 1.4](../images/case1_3-4.png)

### Question 1.5: What are the three most common serious adverse reactions of the three high-volume drugs?
**Purpose**: Understand specific safety concerns to guide risk mitigation strategies

```sql
WITH 
-- Top 3 drugs with highest volume of reports in the past 12 months
high_volume_drugs AS (
  SELECT 
    medicinal_product,
    COUNT(*) as total_reports
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND medicinal_product IS NOT NULL
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  GROUP BY 
    medicinal_product
  HAVING 
    total_reports >= 100
  ORDER BY 
    total_reports DESC
  LIMIT 3
),
-- Calculate count and rate for serious reactions and fatalities
serious_reactions AS (
  SELECT 
    p.medicinal_product,
    p.patient_reaction,
    COUNT(*) as reaction_count,
    SUM(IF(serious_type IN ('Death', 'Hospitalization', 'Lifethreatening'), 1, 0)) as serious_reaction_count,
    SUM(IF(reaction_outcome = 'Fatal', 1, 0)) as fatal_count,
    ROUND(
      SAFE_DIVIDE(
        SUM(IF(serious_type IN ('Death', 'Hospitalization', 'Lifethreatening'), 1, 0)) * 100.0,
        COUNT(*)
      ), 2
    ) as serious_reaction_rate,
    ROUND(
      SAFE_DIVIDE(
        SUM(IF(reaction_outcome = 'Fatal', 1, 0)) * 100.0,
        COUNT(*)
      ), 2
    ) as fatality_rate
  FROM 
    `ade-pipeline.ade_dev_core.patient_drug_reaction` p
    JOIN high_volume_drugs h ON p.medicinal_product = h.medicinal_product
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    AND patient_reaction IS NOT NULL
    AND (
      serious_type IN ('Death', 'Hospitalization', 'Lifethreatening') 
      OR reaction_outcome IN ('Fatal', 'Not recovered')
    )
  GROUP BY 
    p.medicinal_product, 
    p.patient_reaction
  HAVING 
    -- Ensure statistical significance
    reaction_count >= 20
    AND serious_reaction_count >= 10
),
-- Rank by serious reaction count within each drug
ranked_reactions AS (
  SELECT 
    medicinal_product,
    patient_reaction,
    reaction_count,
    serious_reaction_count,
    fatal_count,
    serious_reaction_rate,
    fatality_rate,
    RANK() OVER (
      PARTITION BY medicinal_product 
      ORDER BY serious_reaction_count DESC, fatal_count DESC
    ) AS severity_rank
  FROM serious_reactions
  -- Focus on predominantly serious reactions
  WHERE serious_reaction_rate >= 80  
)
-- Top 5 most serious reactions per drug
SELECT 
  medicinal_product,
  patient_reaction,
  serious_reaction_count,
  fatal_count,
FROM ranked_reactions
WHERE severity_rank <= 5  
ORDER BY medicinal_product, severity_rank;
```

![Answer 1.5](../images/case1_5.png)