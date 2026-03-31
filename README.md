World Vision SQL Portfolio Project

SQL analysis of UNICEF child development data across 7 Southeast Asian countries. Built as a portfolio project to demonstrate SQL skills, Star Schema design, and data quality awareness — with a focus on indicators relevant to World Vision Foundation of Thailand's mission areas.

Background

Why This Dataset
UNICEF's Data Warehouse tracks hundreds of child development indicators worldwide. I selected 7 SE Asian countries and filtered down to 83 indicators that map directly to World Vision's program areas: child health, education, nutrition, child protection, water & sanitation, and migration.

Two indicators are used heavily throughout the analysis:

DTP3 immunization coverage (IM_DTP3) — a standard proxy for healthcare system reach. If a child completes 3 doses of DTP vaccine, it signals that the health system is functioning. WHO and UNICEF use this as a benchmark indicator. Unit: % (comparable across countries).

Under-5 mortality rate (CME_MRY0T4) — a core child survival metric and UN SDG indicator. Directly aligned with World Vision's mission. Unit: deaths per 1,000 live births (comparable across countries).

Both were chosen because they have consistent units across all countries, are internationally recognized standards, and connect to World Vision's work.

Countries Covered
| Code | Country | Income Group |
|------|---------|-------------|
| THA | Thailand | Upper Middle Income |
| IDN | Indonesia | Upper Middle Income |
| PHL | Philippines | Lower Middle Income |
| VNM | Viet Nam | Lower Middle Income |
| KHM | Cambodia | Lower Middle Income |
| LAO | Lao PDR | Lower Middle Income |
| MMR | Myanmar | Low Income |

ETL Process
| Step | Who | Details |
|------|-----|---------|
| Extract | Me | Downloaded CSV from UNICEF Data Warehouse |
| Scope decisions | Me | Selected 7 countries, defined indicator filtering criteria |
| Transform + Load | Python script (AI-assisted) | Filtered 341→83 indicators, fixed year formats, built Star Schema, loaded into SQLite |
| Schema design direction | Me | Chose Star Schema structure, defined dimensions |
| Schema implementation | Python script (AI-assisted) | Created 5 tables per design direction |

The ETL pipeline was executed via Python. The system architecture, database schema, data transformation logic, and quality rules were explicitly designed and strictly defined by me. Code generation for the pipeline execution was accelerated using AI (Claude).

Database Schema

Star Schema — 5 tables, 18,923 rows

```

                      dim_country (7 rows)

                        ↑ country_code

                        |

dim_sex (3 rows) ←── fact_observation (18,923 rows) ──→ dim_indicator (83 rows)

   sex_code ↑                                                  ↑ indicator_code

                                                                |

                                                          dim_domain (11 rows)

```
(linked via dim_indicator.domain)

Tables
fact_observation — 18,923 rows

id, country_code, indicator_code, sex_code, year, value
lower_bound, upper_bound, data_source, obs_status

dim_country — 7 rows

country_code (PK), country_name, region, income_group

dim_indicator — 83 rows

indicator_code (PK), indicator_name, unit_of_measure, domain

dim_sex — 3 rows

sex_code (PK): F (Female), M (Male), _T (Total)

dim_domain — 11 rows

domain_name, mapped to World Vision program areas

Schema Note
The fact table's primary key is id — in retrospect, observation_id would be clearer for readability in JOINs. The composite key country_code + indicator_code + year + sex_code is unique for most records, with 17 known duplicates in Vietnam's Nutrition indicators (years 2000, 2020). These duplicates do not affect the analysis queries, which focus on mortality and immunization indicators.

Analysis Questions (16 queries)

Q1: Data Quality Audit (6 queries)

What it checks: Record distribution across countries and domains, sparse data flags, NULL values in critical columns, orphan records (FK integrity), and indicator-level data scope.

Key findings:

Data volume is balanced across countries (2,540–2,831 rows each)

Early Childhood Development and Poverty domains have fewer than 10 records per country — too sparse for reliable analysis

DTP3 indicator: all 7 countries have comparable coverage (1980–2024, 41–45 records each)

No NULL values found in critical columns (value, country_code, indicator_code, year, sex_code)

No orphan records — all foreign keys match their dimension tables

Q2: Cross-Country Comparison (4 queries)

What it checks: Immunization coverage (DTP3) trends across countries — raw data, 2024 snapshot, and decade-over-decade comparison.

Key findings:

Averaging across indicators with different units (%, per 1000, count) produces meaningless results — Q2a exists to demonstrate this data quality issue

DTP3 2024: Viet Nam highest (97%), Lao PDR lowest (67%)

Not all countries show linear improvement — Cambodia and Indonesia peaked around 2014 then declined by 2024

Q3: Under-5 Mortality Trend (1 query)

What it checks: 30-year mortality trend (1994–2024) for all 7 countries, including calculating Decade-over-Decade (DoD) percentage improvements.

Key findings:

All countries show consistent decline — no reversals (unlike DTP3)

Cambodia most dramatic improvement: 117.98 → 18.41 per 1,000 (~84% reduction)

Technique Note: Utilized Window Function (LAG()) to compute historical percentage changes.

Q4: Gender Disparity (2 queries)

What it checks: Male vs female under-5 mortality rates (2024), and quantified gender gap.

Key findings:

Male mortality higher than female in all 7 countries — consistent with known biological pattern

Myanmar largest gap (7.47), Thailand smallest (1.84)

Technique Note: Contrasted standard Self-Join with Conditional Aggregation (MAX(CASE WHEN...)) to demonstrate performance optimization (O(N) vs O(N²)) suitable for large-scale data lakes.

Q5: Income Group vs Child Outcomes (3 queries)

What it checks: Correlation between country income group and mortality rate, and building a reusable Data Mart view for BI integration.

Key findings:

Clear pattern: Low Income (36.93) > Lower Middle (22.87) > Upper Middle (13.33)

Caveat: Low Income group = Myanmar only (1 country) — not a statistically valid group average

Note: these are unweighted averages of country rates, not population-weighted averages

Above regional average (22.15): Myanmar, Lao PDR, Philippines

Below regional average: Cambodia, Indonesia, Viet Nam, Thailand

Technique Note: Packaged complex CTE and CROSS JOIN logic into a Data Mart (CREATE VIEW) to enable direct downstream dashboard connectivity.

SQL Techniques Used

DDL & Data Modeling: CREATE VIEW for BI readiness

Advanced Analytics: Window Functions (LAG(), OVER(), PARTITION BY)

Optimization: Conditional Aggregation vs. Self-Join

CTE (WITH...AS) + CROSS JOIN

JOIN (INNER, LEFT) across Star Schema tables

GROUP BY with multiple columns

Aggregate functions: COUNT, AVG, MIN, MAX, SUM

CASE WHEN for conditional labeling

UNION ALL for combining validation checks

NULL checking with CASE WHEN + IS NULL pattern

Data Quality Notes

17 duplicate records exist in the dataset: all are Vietnam (VNM), Nutrition domain indicators (NT_ANT_HAZ_NE2, NT_ANT_WAZ_NE2, NT_ANT_WHZ_NE2), years 2000 and 2020. These do not affect any analysis queries in this project, which use mortality (CME_MRY0T4) and immunization (IM_DTP3) indicators.

No NULL values in the 5 critical columns of the fact table.

No orphan records — every foreign key in the fact table has a matching dimension record.

Cross-indicator averaging is invalid without verifying that all indicators share the same unit of measure. This is demonstrated explicitly in Q2a.

Data Source

UNICEF Data Warehouse — https://data.unicef.org

Raw data: 57,558 rows, 341 indicators, 7 countries

After filtering: 18,923 rows, 83 indicators aligned with World Vision mission areas

Years covered: 1970–2024

Data Governance
This project uses publicly available, aggregated national-level statistics from UNICEF. No personally identifiable information (PII) is present in the dataset. All data points are country-level aggregates published for public use.

In a production environment with individual-level data, compliance with data protection regulations (e.g., Thailand's PDPA) would be required — including access controls, data classification, consent management, and audit trails.

Tools
| Tool | Purpose |
|------|---------|
| SQLite | Database engine |
| DB Browser for SQLite | Data exploration and validation |
| VS Code + SQLite extension | Query writing and execution |
| Python (SQLite3) | ETL pipeline (AI-assisted) |
| GitHub | Version control and documentation |
