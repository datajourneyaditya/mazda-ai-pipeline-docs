# Architecture Guide

Detailed explanation of every architectural decision in the Mazda AI Pipeline POC.

---

## Layer model

The pipeline has 6 distinct layers. Each is a separate Unity Catalog schema. Data flows one way — downstream layers never write back upstream.

### Layer 1 — Bronze (workspace.mazda_bronze)

Raw ingest. One table per source CSV. Schema is exactly as Spark inferred it — no casts, no drops, no transforms. The purpose is to have a complete, immutable copy of the source data that can be re-read if anything downstream breaks.

All 9 source tables land here on every full run (overwrite mode). Row counts match the source CSVs exactly.

### Layer 2 — Silver (workspace.mazda_silver)

One cleaned table per source. Silver applies explicit SQL transforms:
- DATE and TIMESTAMP casts on date columns
- INT and DOUBLE casts on numeric columns
- REGEXP_REPLACE to strip non-numeric characters (e.g. 'Q2' → 2 for quarter columns)
- CASE statements to map text categorical values to numeric scores
- WHERE filters to remove rows with null primary keys or null FK columns
- Boolean columns handled with CASE when text values like 'Probably Yes' appear

FK columns (vin, customer_id, dealer_code, customer_phone) are protected from accidental drops by an explicit FK_PROTECT set in the executor.

Silver tables are source-aligned — they mirror the source structure with better types. No joins, no aggregations at this layer.

### Layer 3 — Dimensional (workspace.mazda_dimensional)

The warehouse layer. Designed entirely by the LLM agent from metadata. Contains 4 fact tables and 6 dimension tables plus a generated date spine.

Dimensions are built with:
- UUID() surrogate keys (different from natural keys)
- ROW_NUMBER() deduplication to get one row per natural key (most recent record wins)
- SCD2 dimensions carry: effective_start DATE, effective_end DATE (null = current), is_current BOOLEAN
- SCD1 dimensions are simple deduplicated tables

Facts are built with:
- LEFT JOINs to all relevant dimensions on natural keys
- Both the natural key (e.g. customer_id) AND the surrogate key (customer_sk) retained
- All measures cast to correct types
- Derived measures computed at this layer (e.g. total_discount_usd = msrp - final_sale_price_usd)

### Layer 4 — Gold (workspace.mazda_gold)

Pre-aggregated tables built on top of the dimensional layer. Each gold table is a grouped aggregation with real column references. Gold tables are designed for performance — Genie queries could join fact + dim tables directly, but pre-aggregating here makes queries instant.

6 gold tables:
- sales_summary — by year, month, quarter, model, segment, region
- service_summary — by year, month, service type, category, region, model
- warranty_summary — by year, month, defect category, severity, supplier, model
- crm_lead_summary — by year, month, source, channel, device, segment
- customer_360 — one row per customer, all lifetime metrics
- dealer_scorecard — one row per dealer, sales + service + warranty KPIs combined via CTEs

The dealer_scorecard uses CTEs to pre-aggregate each fact table separately before joining to dim_dealer — this avoids cartesian products from 3-way fact joins.

### Layer 5 — Data Products (workspace.mazda_products)

Named business assets built on top of gold. Each product:
- Reads from one or more gold tables
- Adds enriched derived metrics not in gold (e.g. composite dealer health score, customer segment labels, warranty risk tiers)
- Carries 5 metadata columns: _product_name, _product_version, _domain, _owner, _refreshed_at
- Has a table comment with owner, SLA, and refresh cadence written via COMMENT ON TABLE
- Is registered in dp_catalog with its Genie question set

Data products are what Genie queries. Gold tables are never directly exposed to Genie.

### Layer 6 — Databricks Genie

Conversational NL query interface. Configured with:
- All 6 data product tables + dp_catalog as registered tables
- Business context in the Instructions field (column descriptions, domain terminology)
- Certified questions pre-loaded per product
- Genie generates SQL against the data product tables and renders results as charts or tables

---

## LLM design decisions

### Why two-stage prompting

The full profile JSON for 9 tables with all column stats exceeds Groq's 12,000 token/minute free tier limit. The solution: compress the profile to key columns only (PK cols, highest-cardinality cols, lowest-cardinality cols), then split into two calls.

Stage 1 — warehouse design (facts, dims, SCD types): ~1,400 tokens input, 3,000 tokens output
Stage 2 — pipeline spec + products (given Stage 1 output + table summary): ~3,000 tokens input, 4,000 tokens output

Total: ~11,400 tokens per full LLM redesign. Well within the 500,000 token/day free limit.

### Why not send all columns to the LLM

The LLM does not need to see all 60 columns of vehicle_sales to decide it is a fact table. It needs to see: the PK column, a few high-cardinality columns (potential measures), and a few low-cardinality columns (potential FK references to dimensions). Sending all columns wastes tokens and can confuse the model with noise.

### How the agent reasons about SCD type

The agent looks at:
- Column semantics: words like 'status', 'address', 'classification', 'segment' suggest mutable attributes → SCD2
- Cardinality patterns: if a dimension has columns with varying cardinalities that suggest they change over time → SCD2
- Stability: reference tables with fixed lookup values → SCD1

The agent documents its reasoning in the 'scd_reasoning' field of every dimension, which is stored in the pipeline spec and logged to the agent_decisions table.

### Why the executor uses explicit SQL not the spec

The first version of the executor parsed the LLM's pipeline spec to generate transforms dynamically. This failed because:
- The LLM invented column names that did not exist in the actual tables
- Generic CAST actions failed on columns with text values like 'Q2' or 'Satisfied'
- Gold aggregations referenced columns from different tables

The fix: inspect actual bronze column names first, then write explicit SQL for every table. The LLM's warehouse_design section (facts, dims, SCD types) is still used — only the transform SQL is hardcoded based on real column inspection.

---

## Schema drift handling

Every run the orchestrator:
1. Lists all files in the raw volume
2. Computes an MD5 hash of the schema (column names + types) for each file
3. Compares to the hash stored in mazda_metadata.source_profiles from the previous run
4. Routes to the correct execution path based on what changed

Three scenarios:
- NEW_SOURCE: file not in registry → triggers LLM full redesign + re-run all notebooks
- SCHEMA_DRIFT: hash changed → triggers LLM patch prompt for silver transforms only + re-run executor + products
- INCREMENTAL: row count increased, schema unchanged → re-run bronze + silver + executor, no LLM call
- SKIP: nothing changed → exit immediately

The LLM is only called when structure changes. Row-only updates (Scenario A) never incur an API call.

---

## Data quality scoring

After every table write in silver and gold, a DQ score is computed:
- Completeness: average non-null rate across all columns (100% = no nulls)
- Uniqueness: distinct count of first column / total rows (100% = all PKs unique)
- DQ score: average of completeness and uniqueness

Scores are appended to mazda_metadata.data_quality_scores with the run_id. This table is registered in Genie so users can ask "which product has degraded quality this week?"

---

## Metadata tables

All in workspace.mazda_metadata:

| Table | Purpose | Written by |
|---|---|---|
| source_profiles | Schema hash + row count per table, every run | Profiler + Orchestrator |
| file_registry | Known source files, first/last seen | Orchestrator |
| pipeline_runs | Every run: tables written, status, layer detail | Executor |
| agent_decisions | Every LLM call: type, reasoning, input/output summary | LLM Designer + Orchestrator |
| pipeline_spec_history | Versioned pipeline specs, is_current flag | LLM Designer |
| data_quality_scores | DQ scores per table per run | Executor + Data Products |
