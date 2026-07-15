# Interview Guide — Mazda AI Pipeline POC

Structured STAR stories and talking points for interviews targeting Analytics Engineering, Data Engineering, and Senior Data roles.

---

## Project summary (30-second pitch)

"I built an end-to-end AI-powered data warehouse POC on Databricks Free Edition. The key differentiator is that an LLM agent — Llama 3.3 70B via Groq — profiles 9 raw source CSVs and autonomously designs the entire warehouse: which tables are facts vs dimensions, what the grain of each fact is, whether each dimension needs SCD Type 1 or Type 2, and what data products to create. The executor then builds the full medallion pipeline — bronze through silver through a proper star schema through gold aggregations through data products — and surfaces everything through Databricks Genie for natural language queries. The whole thing runs at zero cost."

---

## STAR Story 1 — AI-driven warehouse design

**Question type:** "Tell me about a time you used AI/ML in a data engineering project."

**Situation:** I wanted to build a POC that demonstrated AI-augmented data engineering — not just using AI to write code, but using an LLM as an active design agent that makes real architectural decisions.

**Task:** Design a pipeline where the LLM decides the warehouse schema from first principles — no hardcoded table names, no pre-defined fact/dim assignments. The agent needed to reason from raw metadata the same way a senior data architect would reason from a data dictionary.

**Action:** I built a schema profiler that computes cardinality, null rates, FK overlap candidates, and PK likelihood for every column in every source table. That metadata profile is passed to Llama 3.3 70B via two prompts. Stage 1: given only column stats and FK overlaps, decide which tables are facts (high-cardinality transaction PKs), which are dimensions (low-cardinality reference data appearing as FKs elsewhere), what the grain of each fact is, and whether each dimension needs SCD1 or SCD2. Stage 2: given the Stage 1 design, produce the pipeline spec and data product definitions. The executor reads the spec and runs it — all layer logic derives from what the agent decided.

**Result:** The agent correctly identified vehicle_sales and service_repair_orders as fact tables from their cardinality patterns, recommended SCD2 for dim_customer and dim_dealer (mutable attributes) vs SCD1 for dim_vehicle and dim_model (stable specs), and generated 6 data products with domain assignments and Genie question sets. The reasoning is stored in an agent_decisions Delta table, so you can query "what did the agent decide and why" directly in Genie.

**Pushback prep:** "How did you validate the agent's decisions?"
I validated by checking three things: cardinality patterns (does a table flagged as a fact have transaction-grain PKs?), FK overlaps (do the join columns the agent identified actually appear in multiple tables?), and row counts after execution (do fact tables have the expected row counts after joining to dims?). Where the agent's column references in the pipeline spec were wrong — it invented column names — I overrode those with explicit SQL based on actual bronze table inspection.

---

## STAR Story 2 — SCD Type 2 implementation

**Question type:** "Walk me through how you've implemented slowly changing dimensions."

**Situation:** The LLM agent designed dim_customer, dim_dealer, and dim_employee as SCD Type 2 because their attributes — customer address, dealer region, employee job title — can change over time and that history is analytically valuable.

**Task:** Implement SCD Type 2 on a serverless Databricks environment using Delta Lake, without dbt or any external framework. The merge logic needed to: close existing rows when tracked columns changed, insert new version rows for changed records, insert truly new records, and leave unchanged records untouched.

**Action:** I wrote a Python function using the Delta MERGE INTO pattern with two passes. Pass 1: MERGE against the current silver data to expire rows where tracked columns differ (set is_current = false, effective_end = current_date). Pass 2: identify which natural keys were just expired, insert new version rows for those keys, and union with brand-new keys (natural key not seen before). The surrogate key is generated with UUID() per version row. The SCD type (1 or 2) and the tracked columns list are read from the pipeline spec — not hardcoded — so the same executor function handles both types based on what the agent decided.

**Result:** dim_customer has 214,862 rows with is_current, effective_start, and effective_end columns. Point-in-time queries work correctly — you can join fact_vehicle_sales to dim_customer using the sale_date against the effective date range to get the customer's attributes as they were at time of purchase, not as they are today.

**Pushback prep:** "What about late-arriving facts?"
The effective date range join handles late-arriving facts correctly because the join condition is sale_date >= effective_start AND (effective_end IS NULL OR sale_date < effective_end). A transaction that arrives late will still match the dimension version that was current on the transaction date.

---

## STAR Story 3 — Schema drift self-healing

**Question type:** "How do you handle schema changes in upstream data sources?"

**Situation:** In production, source schemas change — columns get added, renamed, or have their types changed. Most pipelines break silently or require manual intervention.

**Task:** Build automated schema drift detection and self-healing into the orchestrator so the pipeline detects changes and adapts without human intervention.

**Action:** Every run, the orchestrator computes an MD5 hash of each source file's schema (column names + types). It compares this to the hash stored in the metadata layer from the previous run. If the hash differs, it classifies the change as SCHEMA_DRIFT. For schema drift, the orchestrator calls the LLM with a patch prompt: here is the current pipeline spec, here is the diff of what changed, update only the affected silver transforms. The patched spec is saved as a new version in the pipeline_spec_history table (with is_current toggled), and the executor re-runs only the affected tables. A new file appearing in the raw folder triggers a full LLM redesign rather than a patch.

**Result:** The three scenarios are handled automatically: new rows only (no LLM call, just incremental refresh), schema change (LLM patch + partial re-run), and new source file (LLM full redesign + full re-run). The orchestrator runs daily as a scheduled Databricks Job.

---

## STAR Story 4 — Data products layer

**Question type:** "What's your approach to data governance and data discoverability?"

**Situation:** The gold layer had 6 well-built aggregation tables, but they were undiscoverable — no business context, no ownership, no SLA, and Genie didn't know what questions to ask against them.

**Task:** Implement a data products layer that wraps gold tables with proper enterprise data asset contracts, making them self-describing and Genie-ready.

**Action:** I built a data products notebook that reads the product definitions from the LLM-generated pipeline spec and materialises each product as a separate Delta table in a dedicated mazda_products schema. Each product adds enriched derived metrics that are not in the gold tables — for example dp_dealer_scorecard computes a composite health score 0–100 from CSI, pricing discipline, and repeat visit rate, plus national and regional revenue rankings using window functions. The product contract is written to Unity Catalog via COMMENT ON TABLE with owner, SLA, and refresh cadence. A dp_catalog table acts as the product registry — one row per product with its domain, owner, SLA, source tables, and 5 pre-generated Genie questions. Genie is configured to query only the products schema, not the underlying layers.

**Result:** Business users can ask Genie "show me all available data products" and get an instant catalog. "Which dealers are ranked top 5 nationally?" returns dealer names, codes, revenue, and CSI scores in seconds. The products layer separates the concern of "how is the data built" from "what can business users do with it."

---

## Common interview questions

**Q: Why Databricks over Snowflake for this POC?**
Databricks Free Edition is permanently free — no expiry, no credit card. Snowflake's free trial is 30 days and $400 credit, after which it costs money. For a portfolio POC you want to keep running and demo repeatedly, Databricks Free Edition is the only viable zero-cost option.

**Q: Why Groq instead of OpenAI?**
Groq's free tier provides Llama 3.3 70B with no credit card and no expiry. The entire POC makes 2 LLM calls per run. OpenAI and Anthropic require payment after free credit expires. For a genuinely free POC, Groq is the right choice.

**Q: How is this different from just using GPT to generate SQL?**
Using GPT to generate SQL is a one-time code generation tool. This is an autonomous agent that runs on a schedule, profiles the data it has never seen, reasons about the warehouse design, generates a structured spec, executes the pipeline, and self-heals when upstream schemas change. The agent's decisions are recorded and queryable. It's a running system, not a one-time prompt.

**Q: What would you do differently in production?**
Several things: use Anthropic Claude instead of Llama 3.3 for more reliable JSON output, add Great Expectations or Soda for richer data quality checks, implement proper SCD2 merge logic for incremental runs (current implementation is full overwrite on first run), add email/Slack alerts from the orchestrator when drift is detected or DQ scores drop below threshold, and use Databricks Asset Bundles to manage the notebook deployment as code.

**Q: What is the agent_decisions table used for?**
Every LLM call is logged with: decision type (warehouse_design, schema_drift_patch, new_source_detected), timestamp, the agent's reasoning, input summary (how many tables, how many FK candidates), and output summary (how many facts, dims, products). You can query this in Genie: "what did the agent change last Tuesday?" This makes the AI's decisions auditable and explainable — a key enterprise requirement for AI in data pipelines.
