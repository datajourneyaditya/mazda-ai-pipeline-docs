# Databricks notebook source


# COMMAND ----------

# ============================================================
# NOTEBOOK 1 — SCHEMA PROFILER
# Reads all 9 Mazda CSVs, computes column-level stats,
# detects FK candidates, saves profile to Delta + JSON
# ============================================================

import json, hashlib, datetime, os
from pyspark.sql import functions as F

VOLUME_PATH    = "/Volumes/workspace/default/mazda_raw"
PROFILE_TABLE  = "workspace.mazda_metadata.source_profiles"
REGISTRY_TABLE = "workspace.mazda_metadata.file_registry"
SPEC_VOLUME    = "/Volumes/workspace/default/mazda_raw"

# All 9 source files confirmed in Phase 2
SOURCE_FILES = [
    "crm_leads.csv",
    "customer_survey_responses.csv",
    "dealer_inventory_snapshots.csv",
    "employee_records.csv",
    "manufacturing_production_log.csv",
    "parts_supply_transactions.csv",
    "service_repair_orders.csv",
    "vehicle_sales.csv",
    "warranty_claims.csv"
]

print(f"✓ Config loaded. Profiling {len(SOURCE_FILES)} source files.")
print(f"✓ Volume path: {VOLUME_PATH}")

# COMMAND ----------

def profile_table(file_name: str) -> dict:
    """
    Reads one CSV and returns a complete profile dictionary.
    Computes: schema, row count, per-column cardinality,
    null %, min/max, sample values, PK likelihood.
    """
    path       = f"{VOLUME_PATH}/{file_name}"
    table_name = file_name.replace(".csv", "")

    df = spark.read\
              .option("header", True)\
              .option("inferSchema", True)\
              .csv(path)

    row_count = df.count()
    columns   = []

    for col_name, dtype in df.dtypes:
        try:
            stats = df.agg(
                F.countDistinct(F.col(col_name)).alias("cardinality"),
                F.count(F.when(F.col(col_name).isNull(), 1)).alias("null_count"),
                F.min(F.col(col_name).cast("string")).alias("min_val"),
                F.max(F.col(col_name).cast("string")).alias("max_val")
            ).collect()[0]

            cardinality = int(stats["cardinality"])
            null_count  = int(stats["null_count"])
            sample      = [
                str(r[col_name])
                for r in df.select(col_name).limit(3).collect()
                if r[col_name] is not None
            ]

            columns.append({
                "name":          col_name,
                "type":          dtype,
                "cardinality":   cardinality,
                "null_pct":      round(null_count / row_count * 100, 2) if row_count > 0 else 0,
                "min_val":       str(stats["min_val"])[:100],
                "max_val":       str(stats["max_val"])[:100],
                "sample_values": sample,
                "is_likely_pk":  (cardinality == row_count) and (null_count == 0)
            })
        except Exception as e:
            columns.append({
                "name":          col_name,
                "type":          dtype,
                "cardinality":   -1,
                "null_pct":      -1,
                "min_val":       "error",
                "max_val":       "error",
                "sample_values": [],
                "is_likely_pk":  False,
                "error":         str(e)
            })

    schema_hash = hashlib.md5(
        json.dumps([(c["name"], c["type"]) for c in columns]).encode()
    ).hexdigest()

    return {
        "table_name":   table_name,
        "file_name":    file_name,
        "row_count":    row_count,
        "column_count": len(columns),
        "columns":      columns,
        "schema_hash":  schema_hash,
        "profiled_at":  datetime.datetime.utcnow().isoformat()
    }

print("✓ Profile function defined")

# COMMAND ----------

def detect_fk_candidates(profiles: list) -> dict:
    """
    Finds column names that appear in more than one table.
    These are FK overlap candidates — passed to the LLM
    as join key hints.
    """
    col_map = {}
    for p in profiles:
        for c in p["columns"]:
            col_map.setdefault(c["name"], []).append(p["table_name"])

    fk_candidates = {
        col: tables
        for col, tables in col_map.items()
        if len(tables) > 1
    }
    return fk_candidates

print("✓ FK candidate function defined")

# COMMAND ----------

print("Starting profiler run...")
print("-" * 55)

all_profiles = []
for file_name in SOURCE_FILES:
    print(f"  Profiling {file_name}...", end=" ")
    try:
        profile = profile_table(file_name)
        all_profiles.append(profile)
        pk_cols = [c["name"] for c in profile["columns"] if c["is_likely_pk"]]
        print(f"✓  {profile['row_count']:>8,} rows  "
              f"{profile['column_count']:>3} cols  "
              f"PK hint: {pk_cols}")
    except Exception as e:
        print(f"✗  ERROR: {e}")

print("-" * 55)
print(f"✓ Profiled {len(all_profiles)} tables successfully")

# COMMAND ----------

fk_candidates = detect_fk_candidates(all_profiles)

full_profile = {
    "profiled_at":   datetime.datetime.utcnow().isoformat(),
    "source_count":  len(all_profiles),
    "total_rows":    sum(p["row_count"] for p in all_profiles),
    "tables":        all_profiles,
    "fk_candidates": fk_candidates
}

print(f"✓ Total rows across all tables : {full_profile['total_rows']:,}")
print(f"\n✓ FK candidates detected ({len(fk_candidates)}):")
for col, tables in sorted(fk_candidates.items()):
    print(f"   {col:<40} → {tables}")

# COMMAND ----------

# DBTITLE 1,Cell 7
now = datetime.datetime.utcnow().isoformat()

profile_rows = [
    (
        p["table_name"],
        p["file_name"],
        p["schema_hash"],
        p["row_count"],
        p["column_count"],
        p["profiled_at"]
    )
    for p in all_profiles
]

profile_df = spark.createDataFrame(
    profile_rows,
    ["table_name", "file_name", "schema_hash",
     "row_count", "column_count", "profiled_at"]
).select(
    F.col("table_name"),
    F.col("file_name"),
    F.col("schema_hash"),
    F.col("row_count"),
    F.col("column_count").cast("int").alias("column_count"),
    F.col("profiled_at")
)

profile_df.write\
          .format("delta")\
          .mode("append")\
          .saveAsTable(PROFILE_TABLE)

print("✓ Schema snapshots saved to workspace.mazda_metadata.source_profiles")
display(spark.table(PROFILE_TABLE))

# COMMAND ----------

# Save to a dedicated specs volume
spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.default.mazda_specs
""")

profile_json_str = json.dumps(full_profile, indent=2)

# Write via dbutils
dbutils.fs.put(
    "/Volumes/workspace/default/mazda_specs/profile.json",
    profile_json_str,
    overwrite=True
)

# Verify it landed
size = os.path.getsize("/Volumes/workspace/default/mazda_specs/profile.json")
print(f"✓ profile.json saved  ({size / 1024:.1f} KB)")
print(f"  Path: /Volumes/workspace/default/mazda_specs/profile.json")

# Quick preview
preview = json.loads(
    open("/Volumes/workspace/default/mazda_specs/profile.json").read()
)
print(f"\n  Tables profiled : {preview['source_count']}")
print(f"  Total rows      : {preview['total_rows']:,}")
print(f"  FK candidates   : {len(preview['fk_candidates'])}")

# COMMAND ----------

print("=" * 55)
print("NOTEBOOK 1 — PROFILER COMPLETE")
print("=" * 55)

# Show one table's profile as a sample
sample = all_profiles[0]
print(f"\nSample profile — {sample['table_name']}:")
print(f"  Rows         : {sample['row_count']:,}")
print(f"  Columns      : {sample['column_count']}")
print(f"  Schema hash  : {sample['schema_hash']}")
print(f"\n  Column breakdown:")
for c in sample["columns"][:5]:
    print(f"    {c['name']:<35} {c['type']:<12} "
          f"cardinality={c['cardinality']:>7,}  "
          f"null={c['null_pct']}%  "
          f"pk={c['is_likely_pk']}")

print(f"\n✓ profile.json ready for Notebook 2 (LLM Designer)")
print(f"✓ source_profiles table updated")
print("=" * 55)