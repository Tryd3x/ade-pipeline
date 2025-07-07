# Case Study 1: Drug Safety Signal Detection for Risk Management

![Case_1](../images/case1.png)

## Executive Summary

This analysis examines adverse drug event (ADE) patterns over the past 12 months to identify emerging safety signals requiring immediate regulatory attention. Through systematic analysis of report volumes, severity patterns, and temporal trends, three high-risk medications were identified along with their associated safety profiles to guide proactive risk management strategies.

## Business Impact

Early detection prevents costly recalls (average drug recall costs $100M+), protects brand reputation, and enables proactive regulatory communication. This analysis supports FDA post-marketing surveillance requirements under 21 CFR Part 314.80 and enables data-driven safety decision making.

## Key findings

- **Three high-volume drugs identified:** Methotrexate (1.59M reports), Prednisolone (1.40M), and Actemra (1.39M) dominate adverse event reporting
- **Critical safety alert:** All three drugs show concerning serious adverse event rates exceeding 60%, with Methotrexate and Actemra reaching >71%
- **Escalating risk trends:** Upward trajectory in serious events across all drugs, particularly pronounced in recent months
- **Safety signal clusters:** September anomaly spikes (15-22% above baseline) and emerging recent-month patterns require immediate investigation
- **Off-label prescribing concerns:** Methotrexate and Prednisolone show significant off-label use (11K+ reports each), indicating potential prescribing practice issues

## Strategic Recommendations

- **Crisis management protocols** must be established for all three drugs given exceptional serious event rates
- **Emergency regulatory reviews** needed for Methotrexate and Actemra due to >70% serious event classification
- **Real-time monitoring systems** implementation to detect safety anomalies within 48-72 hours rather than months
- **Enhanced patient monitoring** and prescriber education programs to address off-label use risks
- **Predictive safety models** development to proactively manage emerging risks and maintain competitive market positioning

## Technical Performance Notes
- **Query Optimization**: Implemented reusable CTEs and filtering by partition `record_date` reducing query costs by ~30%
- **Data Volume**: Analysis covers 4.38M+ adverse event records across 3 high-volume drugs
- **Temporal Analysis**: Processed 12-month trend data with month-over-month variance calculations for pattern detection
- **Statistical Rigor**: Applied significance thresholds ensuring reliable safety signal detection

## Technical Analysis

### Data Quality Validation

Before conducting the main analysis, the data is validated for its completeness and quality:

```sql
WITH data_quality_checks AS (
  SELECT 
    'Total Records (Last 12 Months)' AS metric
    , COUNT(*) AS value
  FROM `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
  
  UNION ALL
  
  SELECT 
    'Missing Medicinal Product'
    , COUNT(*)
  FROM `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    medicinal_product IS NULL
    AND record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    
  UNION ALL
  
  SELECT 
    'Missing Patient Reaction'
    , COUNT(*)
  FROM `ade-pipeline.ade_dev_core.patient_drug_reaction`
  WHERE 
    patient_reaction IS NULL
    AND record_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR)
    AND fda_last_updated >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
)
SELECT * FROM data_quality_checks
```

![case1_dq.png](../images/case1_dq.png)

## Question 1: Which three drugs have the highest volume of adverse event reports in the last 12 months?

**Business Purpose**: Identify drugs requiring immediate safety attention due to high report volume, indicating either widespread use or emerging safety concerns.

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
    -- Apply statistical significance threshold 
    total_reports >= 100
ORDER BY 
    total_reports DESC
LIMIT 3;
```

![Answer 1.1](../images/case1_1.png)

**Key Insights** 

- **Methotrexate** leads with **1.59M** reports, followed by **Prednisolone** (**1.40M**) and **Actemra** (**1.39M**).
- All three drugs show **similarly high volumes** with **minimal variation** (~0.20M difference).

**Strategic Recommendations**

- **Prioritize dedicated safety monitoring teams** due to exceptional reporting volumes.
- **Implement enhanced risk management programs** to **proactively address emerging safety concerns**

---

## Question 1.2: What percentage of reports are classified as serious (death and hospitalization) for each high-volume drug?

**Business Purpose**: Assess severity patterns to prioritize safety investigations and resource allocation. Higher serious event rates indicate greater regulatory urgency.

```sql
WITH
-- Top 3 drugs with high volume of reports in the past 12 months
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
ORDER BY medicinal_product
```

![Answer 1.2](../images/case1_2.png)

**Key Insights**

- **Methotrexate** and **Actemra** show alarming **71.9%** and **71.5%** death rate, respectively.
- **Prednisolone** has a **61.0%** death rate with **notably higher hospitalization rates** (**11.9%**).

**Strategic Recommendations**

- **Immediate regulatory review** required for Methotrexate and Actemra due to **>70% death rate**.
- **Implement enhanced patient monitoring protocols** with **early warning systems** for all three drugs.
- **Consider label updates** to better communicate **serious risk profiles** to healthcare providers.


---

## Question 1.3: Are adverse event reports increasing over months for high-volume drugs?

**Business Purpose**: Detect emerging safety signals through temporal trend analysis. Increasing trends may indicate evolving safety profiles or changes in prescribing patterns.

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

![Answer 1.3](../images/case1_3.png)

**Key Insights**

- All three drugs show **concerning upward trends** in serious adverse events, particularly in **recent months**.
- **Methotrexate** demonstrates the **strongest upward trajectory** from **October through January**, peaking at **~84%**.

**Strategic Recommendations**

- **Implement real-time monitoring systems** to detect trend changes within **weeks rather than months**.
- **Investigate root causes** of increasing trends including **new patient populations**, **dosing changes**, or **drug interactions**.
- **Develop predictive models** to forecast future adverse event patterns and **proactively manage risks**.


---

## Question 1.4: Detect unusually high rates of expedited reports among high-volume drugs?

**Business Purpose**: Identify drugs with urgent safety concerns requiring immediate regulatory attention. Expedited reports indicate serious, unexpected adverse events that demand rapid regulatory response.

```sql
-- Top 3 drugs with high volume of reports in the past 12 months
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

![Answer 1.4](../images/case1_4.png)

**Key Insights**

- September shows a **dramatic spike** with all drugs experiencing **15-22% anomalies** above baseline.
- **Methotrexate** exhibits the **highest volatility** with extreme swings from **-18% to +22%**.
- Recent months show **emerging anomalies** indicating potential **new safety concerns**.

**Strategic Recommendations**

- Conduct **immediate root cause analysis** for September spikes to identify triggering factors.
- Implement **automated early warning systems** to detect similar anomalies within **48-72 hours**.
- Establish **cross-functional investigation teams** to rapidly respond to expedited report clusters.

---

## Question 1.5: What are the most common serious adverse reactions for high-volume drugs?

**Business Purpose**: Understand specific safety concerns to guide targeted risk mitigation strategies, label updates, and healthcare provider communications.

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

**Key Insights**

- Off-label use emerges as a **critical concern** for Methotrexate (**12K+ reports**) and Prednisolone (**11K+ reports**).
- **Systemic lupus erythematosus** represents a **significant reaction** across multiple drugs.
- **Actemra** shows a **balanced reaction profile** across **infusion-related** and **systemic conditions**.

**Strategic Recommendations**

- Launch **targeted off-label use investigation** to understand prescribing patterns and associated risks.
- Develop **specialty-specific safety guidelines** for off-label prescribing with clear contraindications.
- Implement **prescriber education programs** focusing on **appropriate patient selection** and **monitoring**.

---

Interested about the underlying architecture? Feel free to checkout:  
[Healthcare Data Pipeline: Medication Safety](../../readme.md)
