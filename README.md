# Mazda AI-Powered Data Pipeline POC

An end-to-end AI-driven data warehouse built on Databricks Free Edition using Groq (Llama 3.3 70B) as the LLM agent and Databricks Genie as the conversational AI/BI layer. The agent profiles raw source data, designs the warehouse schema autonomously, builds a full medallion pipeline, and surfaces everything through natural language queries at zero cost.

## What makes this different

Most data pipelines are hardcoded. This one is not. The LLM agent receives raw metadata and decides from first principles: which tables are facts vs dimensions, the grain of each fact, SCD Type 1 vs Type 2 per dimension, join keys, gold aggregations, and data product definitions. No schema is pre-defined. No table names are hardcoded in the design phase.

## Architecture

```
9 Raw CSVs (Unity Catalog Volume)
         |
    BRONZE layer        workspace.mazda_bronze.*
    9 tables, 2,913,337 rows — raw ingest, schema preserved
         |
    SILVER layer        workspace.mazda_silver.*
    9 tables, 2,913,337 rows — typed, cleaned, FK-protected
         |
    DIMENSIONAL layer   workspace.mazda_dimensional.*
    11 tables, 3,047,457 rows — LLM-designed star schema
    4 facts + 6 dims + date spine, surrogate keys, SCD1/SCD2
         |
    GOLD layer          workspace.mazda_gold.*
    6 tables, 856,286 rows — pre-joined aggregations
         |
    DATA PRODUCTS       workspace.mazda_products.*
    6 products, 762,059 rows — named, versioned, self-describing
         |
    DATABRICKS GENIE    Natural language query interface
```

## Tech stack (100% free)

| Component | Tool | Cost |
|---|---|---|
| Compute + storage | Databricks Free Edition | Free forever |
| LLM agent | Groq API, Llama 3.3 70B | Free tier |
| Delta tables | Delta Lake on Databricks | Included |
| File storage | Unity Catalog Volumes | Included |
| AI/BI layer | Databricks Genie | Included |
| Orchestration | Databricks Jobs | Included |

## Source data

9 synthetic Mazda CSV files, 2,913,337 rows total across vehicle sales, service repair orders, CRM leads, parts supply, dealer inventory, customer surveys, warranty claims, manufacturing logs, and employee records.

## Dimensional model (agent-designed)

Facts: fact_vehicle_sales, fact_service_repair_orders, fact_warranty_claims, fact_crm_leads

Dimensions: dim_customer (SCD2), dim_dealer (SCD2), dim_employee (SCD2), dim_vehicle (SCD1), dim_model (SCD1), dim_date (generated spine)

## Data products

dp_vehicle_sales_intelligence, dp_service_operations_intelligence, dp_warranty_risk_intelligence, dp_customer_360, dp_dealer_scorecard, dp_crm_lead_intelligence

Each product carries: owner, domain, SLA %, refresh cadence, column descriptions, 5 Genie questions, version tag.

## Notebooks

| Notebook | Purpose |
|---|---|
| 00_verify_setup.py | Environment check |
| 01_schema_profiler.py | Profile CSVs, detect FKs |
| 02_llm_designer.py | LLM warehouse design (2 API calls) |
| 03_pipeline_executor_v2.py | Bronze to Gold pipeline |
| 04_data_products_v2.py | Data products + catalog |
| 05_orchestrator.py | Drift detection + scheduling |

## Quick start

1. Sign up at databricks.com/try-databricks, choose Free Edition
2. Sign up at console.groq.com, get free API key
3. Upload 9 CSVs to Unity Catalog Volume
4. Run notebooks 00 through 04 in order
5. Configure Genie Space on workspace.mazda_products tables
6. Schedule notebook 05 as a daily Databricks Job

See docs/SETUP.md for full step-by-step instructions.
