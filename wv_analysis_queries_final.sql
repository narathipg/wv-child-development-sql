-- ============================================================
-- World Vision SQL Portfolio Project
-- Database: wv_child_development.db (UNICEF Data Warehouse)
-- Dataset: 7 SE Asian countries, 83 child development indicators
-- Schema: Star Schema — 1 fact table + 4 dimension tables (18,923 rows)
-- Author: Narathip Gitgrailerk
-- Date: March 2026
-- ============================================================

-- Q1: DATA QUALITY AUDIT

-- Q1a: Record count per country
-- Purpose: Check whether data volume is balanced across countries
SELECT 
    dim_country.country_name, COUNT(*) AS record_count
FROM fact_observation
LEFT JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
GROUP BY dim_country.country_name
ORDER BY record_count ASC;
-- Result: 7 rows — Lao PDR lowest (2,540), Viet Nam highest (2,831)
-- Insight: Relatively balanced — no major country-level data gap

-- Q1b: Record count per country per domain
-- Purpose: Identify which domains have sparse data per country
SELECT
    dim_country.country_name, dim_indicator.domain, COUNT(dim_indicator.domain)
    AS record_count
FROM fact_observation
LEFT JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
LEFT JOIN dim_indicator
    ON fact_observation.indicator_code = dim_indicator.indicator_code
GROUP BY dim_country.country_name, dim_indicator.domain
ORDER by record_count;
-- Result: 77 rows (7 countries × 11 domains)
-- Insight: Early Childhood Development + Poverty have very few records across all countries

-- Q1c: Filter domains with sparse data (HAVING < 10 records) 
-- Purpose: Flag domain-country combinations too small for reliable analysis
SELECT
    dim_country.country_name, dim_indicator.domain, COUNT(dim_indicator.domain)
    AS record_count
FROM fact_observation
LEFT JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
LEFT JOIN dim_indicator
    ON fact_observation.indicator_code = dim_indicator.indicator_code
GROUP BY dim_country.country_name, dim_indicator.domain
HAVING record_count < 10
ORDER by record_count;
-- Result: 12 rows — Early Childhood Development (1-6 rows) + Poverty (4-9 rows)
-- DE note: sample sizes too small for cross-country comparison in these domains

-- Q1d: DTP3 indicator data scope check per country
-- Purpose: Verify year coverage before cross-country comparison (Q2b)
SELECT 
    dim_country.country_name,
    MIN(fact_observation.year) AS earliest_year,
    MAX(fact_observation.year) AS latest_year,
    COUNT(*) AS record_count
FROM fact_observation
LEFT JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'IM_DTP3'
    AND fact_observation.sex_code = '_T'
GROUP BY dim_country.country_name
ORDER BY dim_country.country_name;
-- Result: All 7 countries have data 1980-2024, 41-45 records each
-- Coverage is comparable — safe to compare directly

-- Q1e: NULL check on critical columns
-- Purpose: Verify no missing values in columns used for analysis
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_value,
    SUM(CASE WHEN country_code IS NULL THEN 1 ELSE 0 END) AS null_country,
    SUM(CASE WHEN indicator_code IS NULL THEN 1 ELSE 0 END) AS null_indicator,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
    SUM(CASE WHEN sex_code IS NULL THEN 1 ELSE 0 END) AS null_sex
FROM fact_observation;
-- Result: 18,923 total rows
-- DE note: if any NULL counts > 0, those rows need investigation before analysis

-- Q1f: Orphan record check — FK values with no matching dimension
-- Purpose: Verify referential integrity between fact and dimension tables
SELECT 'country' AS check_type, COUNT(*) AS orphan_count
FROM fact_observation
WHERE country_code NOT IN (SELECT country_code FROM dim_country)
UNION ALL
SELECT 'indicator', COUNT(*)
FROM fact_observation
WHERE indicator_code NOT IN (SELECT indicator_code FROM dim_indicator)
UNION ALL
SELECT 'sex', COUNT(*)
FROM fact_observation
WHERE sex_code NOT IN (SELECT sex_code FROM dim_sex);
-- Result: all orphan_count should be 0 — every FK has a matching dimension record
-- DE note: if orphan_count > 0, those rows would be silently dropped by INNER JOIN

-- Q2: CROSS-COUNTRY COMPARISON BY DOMAIN

-- Q2a: AVG value per country per domain
-- Purpose: Attempt cross-domain comparison — intentionally flawed to demonstrate awareness
-- NOTE: AVG across indicators within a domain is misleading
-- because units differ (%, per 1000, absolute count)
-- A more meaningful approach: filter specific indicator first (see Q2b)
SELECT
    dim_country.country_name, dim_indicator.domain,
    AVG (fact_observation.value) AS average_value
FROM fact_observation
LEFT JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
LEFT JOIN dim_indicator
    ON fact_observation.indicator_code = dim_indicator.indicator_code
GROUP BY dim_country.country_name, dim_indicator.domain
ORDER BY average_value DESC;
-- Result: 77 rows — numbers are meaningless as-is (mixed units)
-- DE note: this query exists to show why unit validation matters before aggregation

-- Q2b-1: DTP3 immunization coverage — raw data all years
-- Purpose: View full trend data before aggregating
-- Data scope verified in Q1d: all countries 1980-2024
SELECT 
    dim_country.country_name,
    fact_observation.year,
    fact_observation.value AS percent_of_value
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'IM_DTP3'
    AND fact_observation.sex_code = '_T'
ORDER BY dim_country.country_name, fact_observation.year;
-- Result: 306 rows — Cambodia starts at 17% (1984), data looks consistent

-- Q2b-2: DTP3 coverage — latest year snapshot (2024)
-- Purpose: Rank current immunization coverage across 7 countries
SELECT 
    dim_country.country_name,
    fact_observation.year,
    fact_observation.value AS percent_of_value
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'IM_DTP3'
    AND fact_observation.sex_code = '_T'
    AND fact_observation.year = 2024
ORDER BY fact_observation.value DESC;
-- Result: 7 rows — Viet Nam highest (97%), Lao PDR lowest (67%)

-- Q2b-3: DTP3 coverage comparison — decade trend (1994-2024)
-- Purpose: Compare immunization progress across 7 countries over 30 years
-- Resolves Q2a limitation: single indicator (%) = comparable across countries
SELECT 
    dim_country.country_name,
    fact_observation.year,
    fact_observation.value AS percent_of_value
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'IM_DTP3'
    AND fact_observation.sex_code = '_T'
    AND fact_observation.year IN (1994, 2004, 2014, 2024)
ORDER BY dim_country.country_name, fact_observation.year;
-- Result: 28 rows (7 countries × 4 decades)
-- Insight: Not all countries show linear improvement
-- Cambodia & Indonesia peaked around 2014 then declined by 2024
-- Flag for analyst: investigate cause of coverage drop in recent decade

-- Q3: UNDER-5 MORTALITY RATE TREND (30-YEAR SPAN)
-- Purpose: Track child survival progress across SE Asia — core World Vision mission
-- Indicator: CME_MRY0T4 (deaths per 1,000 live births)
SELECT 
    dim_country.country_name,
    fact_observation.year,
    ROUND(fact_observation.value, 2) AS deaths_per_1000_live_births
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'CME_MRY0T4'
    AND fact_observation.sex_code = '_T'
    AND fact_observation.year IN (1994, 2004, 2014, 2024)
ORDER BY dim_country.country_name, fact_observation.year;
-- Result: 28 rows — all countries show consistent decline
-- Cambodia most dramatic: 117.98 → 18.41 (~84% reduction)
-- Unlike DTP3 (Q2b), no country shows reversal in mortality trend

-- Q4: GENDER DISPARITY IN UNDER-5 MORTALITY (2024)

-- Q4-1: Male vs Female mortality by country
-- Purpose: Identify male vs female mortality gap — detect bias in child outcomes
-- Indicator: CME_MRY0T4 (deaths per 1,000 live births)
-- JD Match: AI & Analytics — pattern detection in demographic data
SELECT 
    dim_country.country_name,
    dim_sex.sex_name,
    ROUND(fact_observation.value, 2) AS deaths_per_1000_live_births_2024
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
INNER JOIN dim_sex
    ON fact_observation.sex_code = dim_sex.sex_code
WHERE fact_observation.indicator_code = 'CME_MRY0T4'
    AND fact_observation.year = 2024
    AND fact_observation.sex_code NOT IN ('_T')
ORDER BY dim_country.country_name, dim_sex.sex_name;
-- Result: 14 rows (7 countries × 2 sexes)
-- Insight: Male mortality higher than Female in ALL 7 countries
-- Largest gap: Myanmar (40.49 vs 33.02), Lao PDR (32.91 vs 25.55)
-- This is consistent with global pattern — male under-5 mortality is biologically higher
-- DE note: consistent pattern = reliable data, no anomaly to flag

-- Q4-2: Gender Gap calculation — Under-5 Mortality (2024)
-- Purpose: Quantify male-female mortality difference per country
-- Technique: Self-join — join fact_observation to itself to put M and F on same row
SELECT 
    dim_country.country_name,
    ROUND(female_data.value, 2) AS female_rate,
    ROUND(male_data.value, 2) AS male_rate,
    ROUND(male_data.value - female_data.value, 2) AS gender_gap
FROM fact_observation AS male_data
INNER JOIN fact_observation AS female_data
    ON male_data.country_code = female_data.country_code
    AND male_data.indicator_code = female_data.indicator_code
    AND male_data.year = female_data.year
INNER JOIN dim_country
    ON male_data.country_code = dim_country.country_code
WHERE male_data.indicator_code = 'CME_MRY0T4'
    AND male_data.year = 2024
    AND male_data.sex_code = 'M'
    AND female_data.sex_code = 'F'
ORDER BY gender_gap DESC;
-- Result: 7 rows — Myanmar largest gap (7.47), Thailand smallest (1.84)
-- Technique: self-join reshapes row-level data for side-by-side comparison
-- Performance note: at scale, Conditional Aggregation (MAX(CASE WHEN ...)) scans fact table
-- once vs self-join scanning twice — more efficient for large datasets
-- Key assumption: country_code + indicator_code + year + sex_code is unique per row
-- (verified for CME_MRY0T4 — no duplicates. Note: 17 duplicate combinations exist
-- in Nutrition indicators (VNM, NT_ANT_*) but do not affect this query)

-- Q5: INCOME GROUP VS CHILD OUTCOMES (2024)

-- Q5-1: AVG mortality by income group
-- Purpose: Test whether country income level correlates with child mortality
-- Indicator: CME_MRY0T4 (deaths per 1,000 live births)
-- JD Match: Data Architecture — linking dimensional attributes to outcomes
SELECT 
    dim_country.income_group,
    ROUND(AVG(fact_observation.value), 2) AS avg_mortality_rate,
    COUNT(*) AS country_count
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'CME_MRY0T4'
    AND fact_observation.sex_code = '_T'
    AND fact_observation.year = 2024
GROUP BY dim_country.income_group
ORDER BY avg_mortality_rate DESC;
-- Result: 3 rows — clear correlation: higher income = lower mortality
-- Low Income: 36.93 (1 country — Myanmar only, not a true average)
-- Lower Middle Income: 22.87 (4 countries)
-- Upper Middle Income: 13.33 (2 countries — Thailand + Indonesia)
-- Caveat: uneven sample sizes — Low Income has only 1 country
-- Note: this is an unweighted average of country rates, not a population-weighted average
-- A true income group mortality rate would require weighting by each country's birth population
-- DE note: if scaling this analysis, need more countries per group for statistical validity

-- Q5-2: Countries above/below regional average — Under-5 Mortality (2024)
-- Purpose: Compare each country's mortality rate against regional average
-- Technique: SUBQUERY (calculate regional avg) + CASE WHEN (label status)
SELECT 
    dim_country.country_name,
    ROUND(fact_observation.value, 2) AS mortality_rate,
    (SELECT ROUND(AVG(f2.value), 2) 
     FROM fact_observation AS f2 
     WHERE f2.indicator_code = 'CME_MRY0T4' 
       AND f2.year = 2024 
       AND f2.sex_code = '_T') AS regional_avg,
    CASE 
        WHEN fact_observation.value > 
            (SELECT AVG(f3.value) 
             FROM fact_observation AS f3 
             WHERE f3.indicator_code = 'CME_MRY0T4' 
               AND f3.year = 2024 
               AND f3.sex_code = '_T') 
        THEN 'Above Average'
        ELSE 'Below Average'
    END AS status
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
WHERE fact_observation.indicator_code = 'CME_MRY0T4'
    AND fact_observation.year = 2024
    AND fact_observation.sex_code = '_T'
ORDER BY mortality_rate DESC;
-- Result: 7 rows — regional avg = 22.15
-- Above Average: Myanmar (36.93), Lao PDR (29.35), Philippines (26.46)
-- Below Average: Cambodia (18.41), Indonesia (17.69), Viet Nam (17.27), Thailand (8.96)
-- DE note: subquery runs independently to calculate benchmark, then CASE labels each row

-- Q5-2b: CTE refactor — same logic as Q5-2, eliminates duplicate subquery
-- Purpose: Demonstrate CTE as a cleaner alternative to repeated subqueries
-- Technique: WITH...AS calculates regional avg once, CROSS JOIN attaches it to every row
WITH regional AS (
    SELECT ROUND(AVG(fact_observation.value), 2) AS avg_rate
    FROM fact_observation
    WHERE fact_observation.indicator_code = 'CME_MRY0T4'
      AND fact_observation.year = 2024
      AND fact_observation.sex_code = '_T'
)
SELECT 
    dim_country.country_name,
    ROUND(fact_observation.value, 2) AS mortality_rate,
    regional.avg_rate AS regional_avg,
    CASE 
        WHEN fact_observation.value > regional.avg_rate THEN 'Above Average'
        ELSE 'Below Average'
    END AS status
FROM fact_observation
INNER JOIN dim_country
    ON fact_observation.country_code = dim_country.country_code
CROSS JOIN regional
WHERE fact_observation.indicator_code = 'CME_MRY0T4'
    AND fact_observation.year = 2024
    AND fact_observation.sex_code = '_T'
ORDER BY mortality_rate DESC;
-- Result: identical to Q5-2 — 7 rows, same values
-- Advantage: regional avg calculated once (not repeated), easier to read and maintain
