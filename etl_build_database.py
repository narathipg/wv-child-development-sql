"""
ETL Script: UNICEF Data Warehouse → wv_child_development.db
Purpose: Build SQLite Star Schema database for World Vision SQL Portfolio Project
Source: UNICEF Data Warehouse (https://data.unicef.org/dv_index/)
Input: UNICEF_DB.csv — all indicators, 7 SE Asia countries, 1970-2024
Output: wv_child_development.db — Star Schema (5 tables, ~18,900 rows)

ETL Process:
1. EXTRACT: Read CSV downloaded from UNICEF Data Warehouse
2. TRANSFORM: Filter 341 → 83 indicators matching World Vision mission areas,
   parse country/indicator/sex codes, clean units, fix year formats
3. LOAD: Create Star Schema in SQLite — 1 fact table + 4 dimension tables
"""

import csv
import sqlite3
import os

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_CSV = "UNICEF_DB.csv"
OUTPUT_DB = "wv_child_development.db"

# =============================================================================
# STEP 1: Define World Vision relevant indicators (83 out of 341)
# Mapped to domains matching World Vision Thailand work areas
# =============================================================================

WV_INDICATORS = {
    # --- Child Health (mortality, care-seeking, treatment) ---
    "CME_MRY0": "Child Health",
    "CME_MRY0T4": "Child Health",
    "CME_MRY1T4": "Child Health",
    "CME_MRY5T14": "Child Health",
    "CME_MRM0": "Child Health",
    "CME_TMY0T4": "Child Health",
    "MNCH_DIARCARE": "Child Health",
    "MNCH_PNEUCARE": "Child Health",
    "MNCH_ORS": "Child Health",
    "MNCH_UHC": "Child Health",

    # --- Maternal Health ---
    "MNCH_MMR": "Maternal Health",
    "MNCH_SAB": "Maternal Health",
    "MNCH_ANC4": "Maternal Health",
    "MNCH_INSTDEL": "Maternal Health",
    "MNCH_CSEC": "Maternal Health",

    # --- Nutrition (stunting, wasting, breastfeeding, diet) ---
    "NT_ANT_HAZ_NE2": "Nutrition",
    "NT_ANT_HAZ_NE2_MOD": "Nutrition",
    "NT_ANT_WAZ_NE2": "Nutrition",
    "NT_ANT_WHZ_NE2": "Nutrition",
    "NT_ANT_WHZ_NE3": "Nutrition",
    "NT_ANT_WHZ_PO2": "Nutrition",
    "NT_ANT_WHZ_PO2_MOD": "Nutrition",
    "NT_BF_EXBF": "Nutrition",
    "NT_BF_EIBF": "Nutrition",
    "NT_BW_LBW": "Nutrition",
    "NT_CF_MAD": "Nutrition",
    "NT_CF_MDD": "Nutrition",
    "NT_VAS_TWODOSE": "Nutrition",

    # --- Immunization ---
    "IM_BCG": "Immunization",
    "IM_DTP3": "Immunization",
    "IM_MCV1": "Immunization",
    "IM_MCV2": "Immunization",
    "IM_POL3": "Immunization",
    "IM_HEPB3": "Immunization",

    # --- Education ---
    "ED_CR_L1": "Education",
    "ED_CR_L2": "Education",
    "ED_CR_L3": "Education",
    "ED_ROFST_L1": "Education",
    "ED_ROFST_L2": "Education",
    "ED_ROFST_L3": "Education",
    "ED_ANAR_L1": "Education",
    "ED_ANAR_L2": "Education",
    "ED_MAT_L1": "Education",
    "ED_READ_L1": "Education",
    "ED_15-24_LR": "Education",

    # --- Early Childhood Development ---
    "ECD_CHLD_36-59M_EDU-PGM": "Early Childhood Development",
    "ECD_CHLD_LMPSL": "Early Childhood Development",

    # --- Child Protection ---
    "PT_CHLD_Y0T4_REG": "Child Protection",
    "PT_CHLD_5-17_LBR_ECON": "Child Protection",
    "PT_CHLD_5-17_LBR_ECON-HC": "Child Protection",
    "PT_CHLD_1-14_PS-PSY-V_CGVR": "Child Protection",
    "PT_F_20-24_MRD_U18": "Child Protection",
    "PT_F_20-24_MRD_U15": "Child Protection",
    "PT_ST_13-15_BUL_30-DYS": "Child Protection",
    "PT_CHLD_DN": "Child Protection",
    "MNCH_ABR": "Child Protection",
    "MNCH_BIRTH18": "Child Protection",

    # --- Poverty & Livelihood ---
    "PV_CHLD_INCM-PL": "Poverty",
    "PV_CHLD_DPRV-L1-HS": "Poverty",
    "PV_CHLD_DPRV-L3-HS": "Poverty",
    "SPP_GDPPC": "Poverty",
    "SPP_GINI": "Poverty",
    "SPP_CHLD_SOC_PROT": "Poverty",

    # --- Water & Sanitation (WASH) ---
    "WS_PPL_W-SM": "Water & Sanitation",
    "WS_PPL_W-ALB": "Water & Sanitation",
    "WS_PPL_S-SM": "Water & Sanitation",
    "WS_PPL_S-ALB": "Water & Sanitation",
    "WS_PPL_S-OD": "Water & Sanitation",
    "WS_PPL_H-B": "Water & Sanitation",
    "WS_SCH_W-B": "Water & Sanitation",
    "WS_SCH_S-B": "Water & Sanitation",
    "WS_SCH_H-B": "Water & Sanitation",

    # --- Demographics ---
    "DM_POP_TOT": "Demographics",
    "DM_POP_U5": "Demographics",
    "DM_POP_U18": "Demographics",
    "DM_POP_CHILD_PROP": "Demographics",
    "DM_LIFE_EXP": "Demographics",
    "DM_BRTS": "Demographics",
    "DM_FRATE_TOT": "Demographics",

    # --- Migration & Displacement ---
    "MG_RFGS_CNTRY_ASYLM": "Migration",
    "MG_RFGS_CNTRY_ORIGIN": "Migration",
    "MG_INTERNAL_DISP_PERS": "Migration",
    "MG_NEW_INTERNAL_DISP": "Migration",
}

# Country metadata — income classification from World Bank
COUNTRY_METADATA = {
    "THA": ("Southeast Asia", "Upper Middle Income"),
    "IDN": ("Southeast Asia", "Upper Middle Income"),
    "PHL": ("Southeast Asia", "Lower Middle Income"),
    "VNM": ("Southeast Asia", "Lower Middle Income"),
    "KHM": ("Southeast Asia", "Lower Middle Income"),
    "LAO": ("Southeast Asia", "Lower Middle Income"),
    "MMR": ("Southeast Asia", "Low Income"),
}

# Domain → World Vision program mapping
DOMAIN_WV_MAPPING = {
    "Child Health": "Child Development",
    "Maternal Health": "Child Development",
    "Nutrition": "Child Development",
    "Immunization": "Child Development",
    "Education": "Education",
    "Early Childhood Development": "Education",
    "Child Protection": "Child Protection",
    "Poverty": "Livelihood",
    "Water & Sanitation": "WASH",
    "Demographics": "Demographics",
    "Migration": "Migration",
}

# =============================================================================
# STEP 2: EXTRACT — Read and filter CSV
# =============================================================================

print("=" * 60)
print("EXTRACT: Reading UNICEF CSV...")
print("=" * 60)

rows_kept = []
rows_total = 0

with open(INPUT_CSV, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        rows_total += 1
        indicator_code = row["INDICATOR:Indicator"].split(": ")[0]
        if indicator_code in WV_INDICATORS:
            rows_kept.append(row)

print(f"  Total rows in CSV: {rows_total:,}")
print(f"  Rows matching WV indicators: {len(rows_kept):,}")
print(f"  Filtered out: {rows_total - len(rows_kept):,}")

# =============================================================================
# STEP 3: TRANSFORM — Parse codes, clean data, extract dimensions
# =============================================================================

print("\n" + "=" * 60)
print("TRANSFORM: Parsing and cleaning data...")
print("=" * 60)

countries_found = {}
indicators_found = {}

for row in rows_kept:
    # Parse country
    code, name = row["REF_AREA:Geographic area"].split(": ", 1)
    countries_found[code] = name

    # Parse indicator
    ind_code, ind_name = row["INDICATOR:Indicator"].split(": ", 1)
    if ind_code not in indicators_found:
        unit_raw = row.get("UNIT_MEASURE:Unit of measure", "")
        unit_clean = unit_raw.split(": ", 1)[1] if ": " in unit_raw else unit_raw
        indicators_found[ind_code] = (ind_name, unit_clean)

print(f"  Countries: {len(countries_found)}")
for code, name in sorted(countries_found.items()):
    print(f"    {code}: {name}")

print(f"  Indicators: {len(indicators_found)}")

# Count by domain
domain_counts = {}
for row in rows_kept:
    ind_code = row["INDICATOR:Indicator"].split(": ")[0]
    domain = WV_INDICATORS[ind_code]
    domain_counts[domain] = domain_counts.get(domain, 0) + 1

print(f"  Rows by domain:")
for domain in sorted(domain_counts.keys()):
    print(f"    {domain:35s} {domain_counts[domain]:>6,}")

# =============================================================================
# STEP 4: LOAD — Create SQLite Star Schema
# =============================================================================

print("\n" + "=" * 60)
print("LOAD: Building SQLite Star Schema...")
print("=" * 60)

# Remove existing DB
if os.path.exists(OUTPUT_DB):
    os.remove(OUTPUT_DB)

conn = sqlite3.connect(OUTPUT_DB)
c = conn.cursor()

# --- dim_country ---
c.execute("""
    CREATE TABLE dim_country (
        country_code TEXT PRIMARY KEY,
        country_name TEXT NOT NULL,
        region TEXT,
        income_group TEXT
    )
""")

for code, name in sorted(countries_found.items()):
    region, income = COUNTRY_METADATA.get(code, ("Unknown", "Unknown"))
    c.execute("INSERT INTO dim_country VALUES (?, ?, ?, ?)",
              (code, name, region, income))

print(f"  dim_country: {len(countries_found)} rows")

# --- dim_indicator ---
c.execute("""
    CREATE TABLE dim_indicator (
        indicator_code TEXT PRIMARY KEY,
        indicator_name TEXT NOT NULL,
        unit_of_measure TEXT,
        domain TEXT NOT NULL
    )
""")

for code, (name, unit) in sorted(indicators_found.items()):
    domain = WV_INDICATORS[code]
    c.execute("INSERT INTO dim_indicator VALUES (?, ?, ?, ?)",
              (code, name, unit, domain))

print(f"  dim_indicator: {len(indicators_found)} rows")

# --- dim_sex ---
c.execute("""
    CREATE TABLE dim_sex (
        sex_code TEXT PRIMARY KEY,
        sex_name TEXT NOT NULL
    )
""")

for code, name in [("F", "Female"), ("M", "Male"), ("_T", "Total")]:
    c.execute("INSERT INTO dim_sex VALUES (?, ?)", (code, name))

print(f"  dim_sex: 3 rows")

# --- dim_domain ---
c.execute("""
    CREATE TABLE dim_domain (
        domain_name TEXT PRIMARY KEY,
        wv_program TEXT NOT NULL
    )
""")

for domain, wv_program in DOMAIN_WV_MAPPING.items():
    c.execute("INSERT INTO dim_domain VALUES (?, ?)", (domain, wv_program))

print(f"  dim_domain: {len(DOMAIN_WV_MAPPING)} rows")

# --- fact_observation ---
c.execute("""
    CREATE TABLE fact_observation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_code TEXT NOT NULL,
        indicator_code TEXT NOT NULL,
        sex_code TEXT NOT NULL,
        year INTEGER NOT NULL,
        value REAL,
        lower_bound REAL,
        upper_bound REAL,
        data_source TEXT,
        obs_status TEXT,
        FOREIGN KEY (country_code) REFERENCES dim_country(country_code),
        FOREIGN KEY (indicator_code) REFERENCES dim_indicator(indicator_code),
        FOREIGN KEY (sex_code) REFERENCES dim_sex(sex_code)
    )
""")


def safe_float(s):
    """Convert string to float, return None if invalid."""
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


insert_count = 0
for row in rows_kept:
    country_code = row["REF_AREA:Geographic area"].split(": ")[0]
    indicator_code = row["INDICATOR:Indicator"].split(": ")[0]
    sex_code = row["SEX:Sex"].split(": ")[0]

    # Handle year formats like "2000" or "2000-05"
    year_str = row["TIME_PERIOD:Time period"]
    try:
        year = int(year_str)
    except ValueError:
        year = int(year_str.split("-")[0])

    value = safe_float(row["OBS_VALUE:Observation Value"])
    lower = safe_float(row["LOWER_BOUND:Lower Bound"])
    upper = safe_float(row["UPPER_BOUND:Upper Bound"])

    data_source = row.get("DATA_SOURCE:Data Source", "")
    obs_status = row.get("OBS_STATUS:Observation Status", "")
    if ": " in obs_status:
        obs_status = obs_status.split(": ")[1]

    c.execute(
        """INSERT INTO fact_observation
           (country_code, indicator_code, sex_code, year, value,
            lower_bound, upper_bound, data_source, obs_status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (country_code, indicator_code, sex_code, year,
         value, lower, upper, data_source, obs_status),
    )
    insert_count += 1

conn.commit()
print(f"  fact_observation: {insert_count:,} rows")

# =============================================================================
# STEP 5: VERIFY — Run sample queries
# =============================================================================

print("\n" + "=" * 60)
print("VERIFY: Running sample queries...")
print("=" * 60)

# Table summary
print("\n  Table row counts:")
for table in ["dim_country", "dim_indicator", "dim_sex", "dim_domain", "fact_observation"]:
    c.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"    {table}: {c.fetchone()[0]:,}")

# Sample: Under-5 mortality 2024
print("\n  Under-5 Mortality Rate by Country (2024, Total):")
c.execute("""
    SELECT c.country_name, ROUND(f.value, 1) AS mortality_rate
    FROM fact_observation f
    JOIN dim_country c ON f.country_code = c.country_code
    WHERE f.indicator_code = 'CME_MRY0T4'
      AND f.sex_code = '_T'
      AND f.year = 2024
    ORDER BY f.value DESC
""")
for row in c.fetchall():
    print(f"    {row[0]:35s} {row[1]}")

# Sample: Domain coverage
print("\n  Records per domain:")
c.execute("""
    SELECT i.domain, COUNT(*) as cnt
    FROM fact_observation f
    JOIN dim_indicator i ON f.indicator_code = i.indicator_code
    GROUP BY i.domain
    ORDER BY cnt DESC
""")
for row in c.fetchall():
    print(f"    {row[0]:35s} {row[1]:>6,}")

conn.close()

print("\n" + "=" * 60)
print(f"DONE: {OUTPUT_DB} created successfully!")
print("=" * 60)
