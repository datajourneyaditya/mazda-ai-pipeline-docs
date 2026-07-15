# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

response = w.serving_endpoints.query(
    name="databricks-meta-llama-3-3-70b-instruct",
    messages=[ChatMessage(role=ChatMessageRole.USER, content="Reply with exactly: LLM connection confirmed.")],
    max_tokens=50
)

print(response.choices[0].message.content)

# COMMAND ----------

# Create the volume for raw Mazda CSV files
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")

spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.default.mazda_raw
""")

print("✓ Volume created: /Volumes/workspace/default/mazda_raw/")

# COMMAND ----------

schemas = [
    "workspace.mazda_bronze",
    "workspace.mazda_silver", 
    "workspace.mazda_dimensional",
    "workspace.mazda_gold",
    "workspace.mazda_products",
    "workspace.mazda_metadata"
]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"✓ Schema ready: {schema}")

# COMMAND ----------

# Source profile snapshots — used for drift detection
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.source_profiles (
        table_name    STRING,
        file_name     STRING,
        schema_hash   STRING,
        row_count     LONG,
        column_count  INT,
        profiled_at   STRING
    )
    USING DELTA
""")

# File registry — tracks which files have been seen
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.file_registry (
        file_name     STRING,
        first_seen    STRING,
        last_seen     STRING,
        is_active     BOOLEAN
    )
    USING DELTA
""")

# Pipeline run log
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.pipeline_runs (
        run_id         STRING,
        started_at     STRING,
        completed_at   STRING,
        tables_written INT,
        layer_detail   STRING,
        status         STRING
    )
    USING DELTA
""")

# Agent decision log — every LLM call recorded here
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.agent_decisions (
        decision_type   STRING,
        timestamp       STRING,
        reasoning       STRING,
        input_summary   STRING,
        output_summary  STRING
    )
    USING DELTA
""")

# Pipeline spec version history
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.pipeline_spec_history (
        spec_version   STRING,
        created_at     STRING,
        spec_json      STRING,
        is_current     BOOLEAN
    )
    USING DELTA
""")

# Data quality scores
spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.mazda_metadata.data_quality_scores (
        run_id             STRING,
        table_name         STRING,
        completeness_pct   DOUBLE,
        uniqueness_pct     DOUBLE,
        dq_score           DOUBLE,
        scored_at          STRING
    )
    USING DELTA
""")

print("✓ All metadata tables created")

# Verify
tables = spark.sql("SHOW TABLES IN workspace.mazda_metadata").collect()
for t in tables:
    print(f"  - {t['tableName']}")

# COMMAND ----------

# DBTITLE 1,Cell 5
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

print("=" * 50)
print("MAZDA POC — PHASE 1 VERIFICATION")
print("=" * 50)

# Check 1 — Spark
print(f"\n✓ Spark version     : {spark.version}")

# Check 2 — LLM
w = WorkspaceClient()
r = w.serving_endpoints.query(
    name="databricks-meta-llama-3-3-70b-instruct",
    messages=[ChatMessage(role=ChatMessageRole.USER, content="Say OK")],
    max_tokens=10
)
print(f"✓ LLM response      : {r.choices[0].message.content.strip()}")

# Check 3 — Volume exists
import os
vol_path = "/Volumes/workspace/default/mazda_raw"
print(f"✓ Volume path       : {vol_path}")

# Check 4 — All schemas exist
schemas = spark.sql("SHOW SCHEMAS IN workspace").collect()
schema_names = [s['databaseName'] for s in schemas]
required = ["mazda_bronze","mazda_silver","mazda_dimensional",
            "mazda_gold","mazda_products","mazda_metadata"]
for s in required:
    status = "✓" if s in schema_names else "✗ MISSING"
    print(f"{status} Schema           : workspace.{s}")

# Check 5 — All metadata tables exist
tables = spark.sql("SHOW TABLES IN workspace.mazda_metadata").collect()
table_names = [t['tableName'] for t in tables]
required_tables = ["source_profiles","file_registry","pipeline_runs",
                   "agent_decisions","pipeline_spec_history","data_quality_scores"]
for t in required_tables:
    status = "✓" if t in table_names else "✗ MISSING"
    print(f"{status} Metadata table   : {t}")

print("\n" + "=" * 50)
print("Phase 1 complete — ready for Phase 2")
print("=" * 50)

# COMMAND ----------

import os

volume_path = "/Volumes/workspace/default/mazda_raw"
files = os.listdir(volume_path)

print(f"Files found in volume: {len(files)}")
print("-" * 40)
for f in sorted(files):
    size_mb = os.path.getsize(f"{volume_path}/{f}") / (1024 * 1024)
    print(f"  ✓ {f:<40} {size_mb:.2f} MB")

# COMMAND ----------

import datetime

volume_path = "/Volumes/workspace/default/mazda_raw"
files = os.listdir(volume_path)
csv_files = [f for f in files if f.endswith(".csv")]

now = datetime.datetime.utcnow().isoformat()

registry_rows = [(f, now, now, True) for f in csv_files]

registry_df = spark.createDataFrame(
    registry_rows,
    ["file_name", "first_seen", "last_seen", "is_active"]
)

registry_df.write.format("delta")\
           .mode("overwrite")\
           .saveAsTable("workspace.mazda_metadata.file_registry")

print(f"✓ {len(csv_files)} files registered in file_registry")
display(spark.table("workspace.mazda_metadata.file_registry"))

# COMMAND ----------

volume_path = "/Volumes/workspace/default/mazda_raw"
csv_files = [f for f in os.listdir(volume_path) if f.endswith(".csv")]

print("Quick read check — all source files")
print("-" * 55)

total_rows = 0
for f in sorted(csv_files):
    df = spark.read\
              .option("header", True)\
              .option("inferSchema", True)\
              .csv(f"{volume_path}/{f}")
    rows = df.count()
    cols = len(df.columns)
    total_rows += rows
    print(f"  ✓ {f:<35} {rows:>8,} rows  {cols:>3} cols")

print("-" * 55)
print(f"  Total rows across all files: {total_rows:,}")