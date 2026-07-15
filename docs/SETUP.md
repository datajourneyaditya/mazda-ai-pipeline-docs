# Setup Guide

Complete step-by-step instructions to get the Mazda AI Pipeline running from scratch. No local installs required — everything runs in the browser.

## Prerequisites

- A web browser
- Your 9 Mazda CSV source files
- Approximately 1 hour for initial setup and first run

---

## Phase 1 — Databricks Free Edition

### 1.1 Sign up

Go to: https://www.databricks.com/try-databricks

Fill in your details. On the next screen look for "Get started with Free Edition" — not the 14-day trial. Click Free Edition specifically.

If you do not see the Free Edition link, try: https://databricks.com/learn/free-edition

Verify your email and log in to the Databricks workspace.

### 1.2 Understand the compute model

Free Edition uses serverless compute — no cluster to create or manage. In any notebook, click Connect (top right) and choose Serverless. Databricks provisions compute automatically.

You will see a SQL Warehouse under Compute > SQL Warehouses. This is used by Genie. Serverless compute for Python notebooks is separate and automatic.

### 1.3 Create workspace folder

Left sidebar > Workspace > your username > right-click > Create > Folder
Name: mazda-ai-poc

---

## Phase 2 — Groq API Key

### 2.1 Sign up

Go to: https://console.groq.com/

Sign up with Google or email. No credit card required.

### 2.2 Create API key

Left sidebar > API Keys > Create API Key
Name: mazda-poc
Copy the key immediately — it starts with gsk_ and you only see it once.

### 2.3 Free tier limits

Groq free tier provides: 30 requests/minute, 6,000 tokens/minute, 500,000 tokens/day.

This pipeline makes 2 LLM calls per full run (Stage 1: warehouse design, Stage 2: pipeline spec + products). Approximately 8,000–10,000 total tokens per run. Well within the daily limit.

---

## Phase 3 — Environment verification

### 3.1 Create notebook 00_verify_setup

In your mazda-ai-poc folder: Create > Notebook
Name: 00_verify_setup, Language: Python
Click Connect > Serverless

### 3.2 Run setup

Copy the contents of notebooks/00_verify_setup.py and run each cell. The final cell should print:

    ✓ Spark version     : 3.5.x
    ✓ Schema            : workspace.mazda_bronze
    ✓ Schema            : workspace.mazda_silver
    ✓ Schema            : workspace.mazda_dimensional
    ✓ Schema            : workspace.mazda_gold
    ✓ Schema            : workspace.mazda_products
    ✓ Schema            : workspace.mazda_metadata
    ✓ Metadata table    : source_profiles
    ✓ Metadata table    : file_registry
    ✓ Metadata table    : pipeline_runs
    ✓ Metadata table    : agent_decisions
    ✓ Metadata table    : pipeline_spec_history
    ✓ Metadata table    : data_quality_scores

---

## Phase 4 — Upload source data

### 4.1 Open the volume

Left sidebar > Catalog > workspace > default > Volumes > mazda_raw

### 4.2 Upload all 9 CSVs

Click Upload to this volume. Drag and drop all 9 CSV files. Wait for all to show green checkmarks.

### 4.3 Verify

In 00_verify_setup, run the file verification cell. You should see all 9 files listed with sizes and total row counts.

---

## Phase 5 — Run the pipeline

Run notebooks in this exact order. Each must complete before running the next.

### Notebook 01 — Schema Profiler (5–10 min)

Create notebook 01_schema_profiler from notebooks/01_schema_profiler.py.

What it does:
- Reads all 9 CSVs with PySpark
- Computes per-column statistics: cardinality, null %, sample values, PK likelihood
- Detects FK candidate columns shared across 3+ tables
- Saves a schema hash per table for drift detection
- Writes profile.json to /Volumes/workspace/default/mazda_specs/
- Appends snapshots to mazda_metadata.source_profiles

Expected output: 9 tables profiled, 38 FK candidates detected, profile.json saved.

### Notebook 02 — LLM Designer (2–3 min)

Create notebook 02_llm_designer from notebooks/02_llm_designer.py.

IMPORTANT: Paste your Groq API key into the GROQ_API_KEY variable before running.

What it does:
- Loads profile.json
- Builds an ultra-slim prompt (under 6,000 tokens) from key columns only
- Stage 1 call: designs fact tables, dimension tables, grain, SCD types — agent reasons from metadata only
- Stage 2 call: designs silver transforms, gold aggregations, data product definitions
- Saves pipeline_spec.json to the specs volume
- Saves versioned spec to mazda_metadata.pipeline_spec_history
- Logs agent reasoning to mazda_metadata.agent_decisions

Expected output: 4 facts, 6 dims, gold tables, and data products designed by the agent.

### Notebook 03 — Pipeline Executor (15–20 min)

Create notebook 03_pipeline_executor_v2 from notebooks/03_pipeline_executor_v2.py.

What it does:
- Bronze: ingests all 9 CSVs to Delta tables, schema preserved
- Silver: applies explicit SQL transforms per table — proper type casts, bad value handling, FK column protection
  - sale_quarter: REGEXP_REPLACE strips 'Q' prefix before INT cast
  - customer_satisfaction: CASE statement maps 'Satisfied'/'Neutral'/'Dissatisfied' to numeric scores
- Dimensional: builds 6 dimension tables with UUID surrogate keys and ROW_NUMBER deduplication
- Dimensional: builds 4 fact tables with LEFT JOINs to all relevant dimensions
- Gold: 6 pre-aggregated tables using real column names from the actual silver/dimensional tables
- Scores data quality on every table (completeness + uniqueness)
- Logs run to mazda_metadata.pipeline_runs

Expected output: 30 tables across 4 layers, approximately 12.7M total rows.

### Notebook 04 — Data Products (5 min)

Create notebook 04_data_products_v2 from notebooks/04_data_products_v2.py.

What it does:
- Drops any stale product tables from previous runs
- Builds 6 data product tables on top of gold with enriched derived metrics
- dp_customer_360 adds: customer_segment (High Value/Returning/Single Purchase/Prospect), warranty_risk_tier, lifetime spend, tenure days
- dp_dealer_scorecard adds: composite health score 0–100, revenue_rank_national, revenue_rank_in_region
- Writes product contracts as COMMENT ON TABLE statements
- Creates dp_catalog registry with all 6 products, their owners, SLAs, and Genie question sets

Expected output: 6 data product tables + dp_catalog, all with metadata contracts.

---

## Phase 6 — Genie setup

See docs/GENIE_SETUP.md for the complete configuration.

---

## Phase 7 — Schedule the orchestrator

### 7.1 Create notebook 05_orchestrator

Create notebook 05_orchestrator from notebooks/05_orchestrator.py.

Find your username: spark.sql("SELECT current_user()").show()
Replace {your_username} in all dbutils.notebook.run() calls.

### 7.2 Create the Databricks Job

Left sidebar > Workflows > Create job

    Job name  : mazda-pipeline-orchestrator
    Notebook  : /Users/{your_username}/mazda-ai-poc/05_orchestrator
    Schedule  : 0 6 * * * (daily at 06:00 UTC)
    Timeout   : 3600 seconds

Click Create, then click Resume to activate.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| No module named 'groq' | Library not installed | Add %pip install groq -q as first cell, restart kernel |
| 413 Request too large | Prompt over Groq token limit | Two-stage prompting handles this. Verify slim_profile has 9 tables. |
| CAST_INVALID_INPUT | Text in numeric column | Check for Q-prefixed quarters (REGEXP_REPLACE) or text satisfaction scores (CASE) |
| UNRESOLVED_COLUMN | LLM used wrong column name | Executor uses real bronze column names — this is handled. If it appears, re-run executor. |
| Table not found | Schema not created | Run 00_verify_setup first |
| NameError: profile not defined | Kernel restarted after pip install | Re-run config cell to reload all variables |
| Genie returns empty results | Table not added to Space | Add all workspace.mazda_products.* tables to the Genie Space |
| fact_crm_leads has 3M rows | Cartesian product from dim_model join | Re-run the fact_crm_leads fix cell — remove the dim_model join |
