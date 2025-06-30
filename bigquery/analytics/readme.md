# OpenFDA Adverse Event Data: Modular Business Intelligence Case Study

## Executive Summary

This case study demonstrates how OpenFDA adverse event reporting data can drive critical business decisions through focused, actionable queries. Each business case is broken down into 5 specific questions with individual SQL queries for maximum transparency and usability.

---



---

## Case Study 2: Competitive Intelligence & Market Positioning

### Business Impact
Informs pricing and marketing strategies, identifies competitive advantages in safety, and guides R&D investment decisions.

### Question 2.1: What is the market landscape for specific therapeutic indications?
**Purpose**: Understand competitive landscape and market players

```sql
-- Therapeutic area competitive landscape
SELECT 
  drugindication as therapeutic_area,
  COUNT(DISTINCT medicinalproduct) as competing_drugs,
  COUNT(*) as total_market_reports,
  COUNT(DISTINCT safetyreportid) as unique_cases,
  COUNT(DISTINCT occurcountry) as global_reach
FROM `your_project.your_dataset.adverse_events`
WHERE drugindication IS NOT NULL
  AND medicinalproduct IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY drugindication
HAVING competing_drugs >= 3
  AND total_market_reports >= 100
ORDER BY total_market_reports DESC
LIMIT 20;
```

### Question 2.2: How do drugs compare within the same therapeutic category for safety?
**Purpose**: Benchmark safety performance against direct competitors

```sql
-- Safety comparison within therapeutic categories
SELECT 
  drugindication as therapeutic_area,
  medicinalproduct,
  COUNT(*) as total_reports,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_events,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate_percent,
  SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) as death_events,
  ROUND((SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as death_rate_percent
FROM `your_project.your_dataset.adverse_events`
WHERE drugindication IS NOT NULL
  AND medicinalproduct IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
  AND drugindication IN (
    -- Focus on major therapeutic areas
    'DIABETES MELLITUS', 'HYPERTENSION', 'DEPRESSION', 'PAIN', 'INFECTION'
  )
GROUP BY drugindication, medicinalproduct
HAVING total_reports >= 20
ORDER BY drugindication, serious_rate_percent DESC;
```

### Question 2.3: What is the average safety profile benchmark for each therapeutic area?
**Purpose**: Establish category benchmarks for competitive positioning

```sql
-- Therapeutic area safety benchmarks
SELECT 
  drugindication as therapeutic_area,
  COUNT(DISTINCT medicinalproduct) as drugs_in_category,
  AVG(serious_rate) as avg_serious_rate_percent,
  STDDEV(serious_rate) as stddev_serious_rate,
  MIN(serious_rate) as best_serious_rate,
  MAX(serious_rate) as worst_serious_rate,
  AVG(total_reports) as avg_reports_per_drug
FROM (
  SELECT 
    drugindication,
    medicinalproduct,
    COUNT(*) as total_reports,
    ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate
  FROM `your_project.your_dataset.adverse_events`
  WHERE drugindication IS NOT NULL
    AND medicinalproduct IS NOT NULL
    AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
  GROUP BY drugindication, medicinalproduct
  HAVING total_reports >= 20
) drug_metrics
GROUP BY drugindication
HAVING drugs_in_category >= 3
ORDER BY avg_serious_rate_percent ASC;
```

### Question 2.4: Which drugs are safety leaders vs. laggards in their categories?
**Purpose**: Identify competitive advantages and disadvantages in safety

```sql
-- Safety leaders and laggards by category
WITH drug_safety_metrics AS (
  SELECT 
    drugindication,
    medicinalproduct,
    COUNT(*) as total_reports,
    ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate
  FROM `your_project.your_dataset.adverse_events`
  WHERE drugindication IS NOT NULL
    AND medicinalproduct IS NOT NULL
    AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
  GROUP BY drugindication, medicinalproduct
  HAVING total_reports >= 30
),
category_benchmarks AS (
  SELECT 
    drugindication,
    AVG(serious_rate) as category_avg_serious_rate,
    STDDEV(serious_rate) as category_stddev
  FROM drug_safety_metrics
  GROUP BY drugindication
  HAVING COUNT(*) >= 3
)
SELECT 
  dsm.drugindication as therapeutic_area,
  dsm.medicinalproduct,
  dsm.total_reports,
  dsm.serious_rate as drug_serious_rate_percent,
  ROUND(cb.category_avg_serious_rate, 2) as category_avg_serious_rate,
  ROUND(dsm.serious_rate - cb.category_avg_serious_rate, 2) as vs_category_avg,
  CASE 
    WHEN dsm.serious_rate < (cb.category_avg_serious_rate - cb.category_stddev) THEN 'SAFETY LEADER'
    WHEN dsm.serious_rate > (cb.category_avg_serious_rate + cb.category_stddev) THEN 'SAFETY LAGGARD'
    ELSE 'AVERAGE SAFETY'
  END as competitive_position
FROM drug_safety_metrics dsm
JOIN category_benchmarks cb ON dsm.drugindication = cb.drugindication
ORDER BY dsm.drugindication, vs_category_avg ASC;
```

### Question 2.5: What are the unique adverse event profiles for competing drugs?
**Purpose**: Identify differentiated safety concerns for competitive messaging

```sql
-- Unique adverse event signatures by competitor
SELECT 
  drugindication as therapeutic_area,
  medicinalproduct,
  reactionmeddrapt as adverse_reaction,
  COUNT(*) as reaction_frequency,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY medicinalproduct)), 2) as pct_of_drug_reactions,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_reactions
FROM `your_project.your_dataset.adverse_events`
WHERE drugindication IS NOT NULL
  AND medicinalproduct IS NOT NULL
  AND reactionmeddrapt IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
  AND drugindication IN ('DIABETES MELLITUS', 'HYPERTENSION', 'DEPRESSION')
GROUP BY drugindication, medicinalproduct, reactionmeddrapt
HAVING reaction_frequency >= 10
ORDER BY drugindication, medicinalproduct, pct_of_drug_reactions DESC;
```

---

## Case Study 3: Patient Demographics & Risk Stratification

### Business Impact
Enables targeted risk evaluation and mitigation strategies (REMS), supports precision medicine initiatives, and guides clinical trial design.

### Question 3.1: What is the age distribution of patients experiencing adverse events?
**Purpose**: Understand age-related safety patterns for targeted interventions

```sql
-- Age distribution analysis
SELECT 
  CASE 
    WHEN patientonsetage IS NULL THEN 'Unknown'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) < 12 THEN 'Pediatric (<12)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 12 AND 17 THEN 'Adolescent (12-17)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 18 AND 64 THEN 'Adult (18-64)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 65 AND 79 THEN 'Elderly (65-79)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) >= 80 THEN 'Very Elderly (80+)'
    ELSE 'Unknown'
  END as age_group,
  COUNT(*) as total_cases,
  COUNT(DISTINCT safetyreportid) as unique_reports,
  COUNT(DISTINCT medicinalproduct) as unique_drugs,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 2) as percentage_of_total
FROM `your_project.your_dataset.adverse_events`
WHERE EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY age_group
ORDER BY total_cases DESC;
```

### Question 3.2: How do serious adverse event rates vary by age and gender?
**Purpose**: Identify high-risk demographic segments

```sql
-- Age and gender risk stratification
SELECT 
  CASE 
    WHEN patientonsetage IS NULL THEN 'Unknown Age'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) < 18 THEN 'Pediatric (<18)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 18 AND 64 THEN 'Adult (18-64)'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) >= 65 THEN 'Elderly (65+)'
    ELSE 'Unknown Age'
  END as age_group,
  COALESCE(patientsex, 'Unknown') as gender,
  COUNT(*) as total_cases,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_cases,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate_percent,
  SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) as death_cases,
  ROUND((SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as death_rate_percent
FROM `your_project.your_dataset.adverse_events`
WHERE EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY age_group, gender
HAVING total_cases >= 100
ORDER BY serious_rate_percent DESC, death_rate_percent DESC;
```

### Question 3.3: Which patient populations have the highest hospitalization rates?
**Purpose**: Focus resources on populations most likely to require intensive medical intervention

```sql
-- Hospitalization risk by demographics
SELECT 
  CASE 
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) < 18 THEN 'Pediatric'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 18 AND 64 THEN 'Adult'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) >= 65 THEN 'Elderly'
    ELSE 'Unknown Age'
  END as age_group,
  COALESCE(patientsex, 'Unknown') as gender,
  COUNT(*) as total_cases,
  SUM(CASE WHEN seriousnesshospitalization = '1' THEN 1 ELSE 0 END) as hospitalization_cases,
  ROUND((SUM(CASE WHEN seriousnesshospitalization = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as hospitalization_rate_percent,
  SUM(CASE WHEN seriousnesslifethreatening = '1' THEN 1 ELSE 0 END) as life_threatening_cases,
  ROUND((SUM(CASE WHEN seriousnesslifethreatening = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as life_threatening_rate_percent
FROM `your_project.your_dataset.adverse_events`
WHERE EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY age_group, gender
HAVING total_cases >= 50
ORDER BY hospitalization_rate_percent DESC;
```

### Question 3.4: Are there age-specific adverse reaction patterns?
**Purpose**: Identify age-related safety concerns for targeted labeling and education

```sql
-- Age-specific adverse reaction patterns
SELECT 
  CASE 
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) < 18 THEN 'Pediatric'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 18 AND 64 THEN 'Adult'
    WHEN SAFE_CAST(patientonsetage AS FLOAT64) >= 65 THEN 'Elderly'
    ELSE 'Unknown'
  END as age_group,
  reactionmeddrapt as adverse_reaction,
  COUNT(*) as reaction_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 
    CASE 
      WHEN SAFE_CAST(patientonsetage AS FLOAT64) < 18 THEN 'Pediatric'
      WHEN SAFE_CAST(patientonsetage AS FLOAT64) BETWEEN 18 AND 64 THEN 'Adult'
      WHEN SAFE_CAST(patientonsetage AS FLOAT64) >= 65 THEN 'Elderly'
      ELSE 'Unknown'
    END)), 2) as pct_within_age_group,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_reactions
FROM `your_project.your_dataset.adverse_events`
WHERE reactionmeddrapt IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY age_group, reactionmeddrapt
HAVING reaction_count >= 20
ORDER BY age_group, pct_within_age_group DESC;
```

### Question 3.5: What is the weight distribution impact on adverse event severity?
**Purpose**: Understand dosing and safety implications for different patient weights

```sql
-- Weight-based adverse event analysis
SELECT 
  CASE 
    WHEN patientweight IS NULL THEN 'Unknown Weight'
    WHEN SAFE_CAST(patientweight AS FLOAT64) < 50 THEN 'Underweight (<50kg)'
    WHEN SAFE_CAST(patientweight AS FLOAT64) BETWEEN 50 AND 80 THEN 'Normal Weight (50-80kg)'
    WHEN SAFE_CAST(patientweight AS FLOAT64) BETWEEN 80 AND 100 THEN 'Overweight (80-100kg)'
    WHEN SAFE_CAST(patientweight AS FLOAT64) > 100 THEN 'Obese (>100kg)'
    ELSE 'Unknown Weight'
  END as weight_category,
  COUNT(*) as total_cases,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_cases,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate_percent,
  AVG(SAFE_CAST(patientweight AS FLOAT64)) as avg_weight_kg,
  COUNT(DISTINCT medicinalproduct) as unique_drugs_involved
FROM `your_project.your_dataset.adverse_events`
WHERE EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY weight_category
HAVING total_cases >= 100
ORDER BY serious_rate_percent DESC;
```

---

## Case Study 4: Geographic Risk Assessment & Regulatory Strategy

### Business Impact
Guides global regulatory filing strategies, identifies markets requiring enhanced pharmacovigilance, and informs market entry/exit decisions.

### Question 4.1: Which countries have the highest volume of adverse event reports?
**Purpose**: Identify key markets requiring regulatory attention and resources

```sql
-- Global adverse event reporting volume
SELECT 
  COALESCE(occurcountry, 'Unknown') as country,
  COUNT(*) as total_reports,
  COUNT(DISTINCT safetyreportid) as unique_cases,
  COUNT(DISTINCT medicinalproduct) as unique_drugs,
  COUNT(DISTINCT companynumb) as unique_companies,
  MAX(PARSE_DATE('%Y%m%d', receiptdate)) as latest_report_date
FROM `your_project.your_dataset.adverse_events`
WHERE occurcountry IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY country
ORDER BY total_reports DESC
LIMIT 25;
```

### Question 4.2: What are the serious adverse event rates by country?
**Purpose**: Identify countries with concerning safety patterns requiring investigation

```sql
-- Country-specific serious adverse event rates
SELECT 
  occurcountry as country,
  COUNT(*) as total_reports,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_reports,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate_percent,
  SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) as death_reports,
  ROUND((SUM(CASE WHEN seriousnessdeath = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as death_rate_percent,
  SUM(CASE WHEN fulfillexpeditecriteria = '1' THEN 1 ELSE 0 END) as expedited_reports,
  ROUND((SUM(CASE WHEN fulfillexpeditecriteria = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as expedited_rate_percent
FROM `your_project.your_dataset.adverse_events`
WHERE occurcountry IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY country
HAVING total_reports >= 100
ORDER BY serious_rate_percent DESC, death_rate_percent DESC
LIMIT 20;
```

### Question 4.3: How do reporting patterns differ between countries?
**Purpose**: Understand regional regulatory compliance and reporting quality

```sql
-- Country reporting pattern analysis
SELECT 
  occurcountry as country,
  COUNT(*) as total_reports,
  COUNT(DISTINCT companynumb) as reporting_companies,
  AVG(CASE WHEN duplicate = '1' THEN 1 ELSE 0 END) * 100 as duplicate_rate_percent,
  COUNT(CASE WHEN qualification = '1' THEN 1 END) as healthcare_professional_reports,
  COUNT(CASE WHEN qualification = '2' THEN 1 END) as lawyer_reports,
  COUNT(CASE WHEN qualification = '3' THEN 1 END) as consumer_reports,
  ROUND((COUNT(CASE WHEN qualification = '1' THEN 1 END) * 100.0 / COUNT(*)), 2) as hcp_report_percentage
FROM `your_project.your_dataset.adverse_events`
WHERE occurcountry IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY country
HAVING total_reports >= 200
ORDER BY total_reports DESC;
```

### Question 4.4: Which countries show increasing adverse event trends?
**Purpose**: Identify emerging markets with growing safety concerns

```sql
-- Country-level adverse event trends
WITH country_yearly_data AS (
  SELECT 
    occurcountry as country,
    EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) as report_year,
    COUNT(*) as yearly_reports,
    SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as yearly_serious_reports
  FROM `your_project.your_dataset.adverse_events`
  WHERE occurcountry IS NOT NULL
    AND receiptdate IS NOT NULL
    AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) BETWEEN 2021 AND 2024
  GROUP BY country, report_year
)
SELECT 
  country,
  report_year,
  yearly_reports,
  yearly_serious_reports,
  LAG(yearly_reports) OVER (PARTITION BY country ORDER BY report_year) as prev_year_reports,
  ROUND((yearly_reports - LAG(yearly_reports) OVER (PARTITION BY country ORDER BY report_year)) * 100.0 / 
        LAG(yearly_reports) OVER (PARTITION BY country ORDER BY report_year), 2) as growth_rate_percent
FROM country_yearly_data
WHERE LAG(yearly_reports) OVER (PARTITION BY country ORDER BY report_year) IS NOT NULL
  AND yearly_reports >= 50
ORDER BY growth_rate_percent DESC
LIMIT 25;
```

### Question 4.5: What are the most common adverse reactions by geographic region?
**Purpose**: Identify region-specific safety patterns for targeted interventions

```sql
-- Geographic adverse reaction patterns
SELECT 
  occurcountry as country,
  reactionmeddrapt as adverse_reaction,
  COUNT(*) as reaction_count,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY occurcountry)), 2) as pct_of_country_reactions,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_reactions,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_reaction_rate
FROM `your_project.your_dataset.adverse_events`
WHERE occurcountry IS NOT NULL
  AND reactionmeddrapt IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
  AND occurcountry IN ('US', 'CANADA', 'UNITED KINGDOM', 'GERMANY', 'FRANCE', 'JAPAN', 'AUSTRALIA')
GROUP BY country, adverse_reaction
HAVING reaction_count >= 20
ORDER BY country, pct_of_country_reactions DESC;
```

---

## Case Study 5: Adverse Event Pattern Recognition & Drug Development

### Business Impact
Guides preclinical and clinical development strategies, identifies opportunities for safer drug formulations, and informs biomarker development.

### Question 5.1: What are the most frequently reported adverse reactions across all drugs?
**Purpose**: Identify common safety concerns for drug development planning

```sql
-- Most common adverse reactions overall
SELECT 
  reactionmeddrapt as adverse_reaction,
  COUNT(*) as total_occurrences,
  COUNT(DISTINCT medicinalproduct) as affected_drugs,
  COUNT(DISTINCT safetyreportid) as unique_cases,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) as serious_occurrences,
  ROUND((SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as serious_rate_percent,
  SUM(CASE WHEN seriousnesshospitalization = '1' THEN 1 ELSE 0 END) as hospitalizations
FROM `your_project.your_dataset.adverse_events`
WHERE reactionmeddrapt IS NOT NULL
  AND EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', receiptdate)) >= 2022
GROUP BY adverse_reaction
HAVING total_occurrences >= 100
ORDER BY total_occurrences DESC
LIMIT 30;
```

### Question 5.2: How do adverse reaction patterns vary by therapeutic indication?
**Purpose**: Understand indication-specific safety patterns for targeted drug development

```sql
-- Adverse reactions by therapeutic indication
SELECT 
  drugindication as therapeutic_area,
  reactionmeddrapt as adverse_reaction,
  COUNT(*) as reaction_frequency,
  COUNT(DISTINCT medicinalproduct) as drugs_with_reaction,
  ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY