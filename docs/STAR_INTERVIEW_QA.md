# STAR Interview Q&A

Structured interview answers for this project using the Context → Options → Criteria → Decision → Tradeoffs → Outcome framework.

---

## Q1: Tell me about a data engineering project you built end to end

**Context:**
I wanted to build a portfolio project that demonstrated AI-augmented data engineering — not just a medallion pipeline, but one where an LLM agent makes the actual data modelling decisions. I chose an automotive dataset with 9 source files and 2.9M rows as the domain.

**Options:**
I could have (a) hardcoded a star schema upfront and built the pipeline around it, (b) used dbt for transforms with a pre-designed model, or (c) let an LLM agent profile the raw data and design the dimensional model from scratch.

**Criteria:**
The goal was to demonstrate that AI can make credible warehouse design decisions from metadata alone — without domain knowledge baked in. That ruled out options A and B.

**Decision:**
I built a 5-notebook pipeline on Databricks Free Edition where the LLM receives only column statistics — cardinality, null rates, FK overlaps — and reasons about which tables should be facts, which should be dimensions, what the grain is, and what SCD type each dimension needs.

**Tradeoffs:**
The LLM doesn't always get column names right on the first pass — it designed gold aggregations using column names that didn't exist in the actual dimensional tables. I had to add a column-existence check before aggregating, and eventually rewrote the silver and dimensional layers with explicit SQL after the spec-driven approach produced tables that were effectively copies of the source.

**Outcome:**
A running 5-layer pipeline across 41 Delta tables, 12.5M total rows, with 6 named data products queryable through Databricks Genie using natural language. The entire stack runs at zero cost on Databricks Free Edition and Groq's free API tier.

---

## Q2: How did you handle schema drift in this pipeline?

**Context:**
In any production pipeline, source schemas change — new columns get added, columns get renamed, entirely new source files appear. The pipeline needed to detect and handle all three scenarios without manual intervention.

**Options:**
I could have (a) re-run the full pipeline on every schedule regardless of changes, (b) checked only row counts for changes, or (c) built a three-scenario detection system using schema hashing.

**Criteria:**
Re-running the full pipeline every time is wasteful — 2.9M rows takes 20+ minutes. Row count alone misses schema changes. I needed granular detection that only triggers re-work where something actually changed.

**Decision:**
I implemented three detection scenarios in the orchestrator notebook. Scenario A (new rows, same schema) triggers a bronze/silver re-run with no LLM call. Scenario B (new file appears) triggers a full LLM redesign of the warehouse. Scenario C (column added or renamed) calls the LLM with a patch prompt containing only the diff and the current spec, then re-runs affected layers only.

**Tradeoffs:**
Schema hashing uses MD5 of column name+type pairs, which means column reordering registers as drift even if the actual schema didn't change meaningfully. A more robust approach would compare column sets explicitly. I accepted this tradeoff because false positives in drift detection are cheap — the worst case is an unnecessary LLM call and a silver re-run.

**Outcome:**
The orchestrator runs daily as a Databricks Job. On most days all files show SKIP. When a file changes, only the affected tables are re-processed. The LLM is never called on a row-count-only change, keeping API usage well within the free tier.

---

## Q3: Walk me through your SCD implementation

**Context:**
The LLM agent classified `dim_customer`, `dim_dealer`, and `dim_employee` as SCD Type 2 — because customer addresses and dealer names can change over time and history is worth preserving. The executor needed to handle SCD2 correctly for initial load and incremental updates.

**Options:**
I could have (a) used a full overwrite on every run (simpler but loses history), (b) used Delta's built-in MERGE INTO for upserts, or (c) implemented a three-step SCD2 pattern: expire changed rows, insert new versions, insert brand new records.

**Decision:**
I used option C — the three-step pattern with MERGE INTO. On initial load, all records are inserted with `effective_start = current_date`, `effective_end = NULL`, `is_current = TRUE`. On subsequent runs: Step 1 closes rows where tracked columns changed. Step 2 inserts new version rows for those closed records. Step 3 inserts genuinely new natural keys.

**Tradeoffs:**
The tracked columns for SCD2 are determined by the LLM — it decides which columns represent mutable attributes worth versioning. This is powerful but requires the LLM to be right. In the Mazda case it tracked `dealer_name`, `dealer_state` for `dim_dealer` and `email`, `segment` for `dim_customer`, which are reasonable choices. But if the LLM missed an important tracked column, history wouldn't be captured for changes to that attribute.

**Outcome:**
SCD2 dimensions correctly carry `effective_start`, `effective_end`, and `is_current` columns. Facts join to the dimension version that was current at the time of the transaction using a date range condition: `fact.sale_date >= dim.effective_start AND (fact.sale_date < dim.effective_end OR dim.effective_end IS NULL)`.

---

## Q4: Why did you choose Databricks Free Edition over Snowflake?

**Context:**
The project required a platform that would run at zero cost indefinitely — not just during a trial period.

**Options:**
Snowflake offers a $400 free trial with Cortex Analyst (a powerful NL query layer). Databricks Free Edition is permanently free but uses Genie (slightly less capable). I also considered a hybrid approach.

**Criteria:**
The hard constraint was zero cost after the build. A 30-day trial that expires doesn't meet that bar — you can't maintain or demo a POC you no longer have access to.

**Decision:**
Databricks Free Edition. Permanently free, Genie for NL queries, serverless compute, and Unity Catalog included. Groq's free API tier replaces the Anthropic API for LLM calls.

**Tradeoffs:**
Databricks Community Edition (the predecessor) supported all-purpose clusters with custom library installs. Free Edition is serverless-only, which means `%pip install` inside notebooks is the only way to add libraries, and outbound internet is restricted to trusted domains. This blocked the Anthropic API — which is why I switched to Groq.

**Outcome:**
Total cost of the entire POC including all development, all pipeline runs, and all LLM calls: $0.

---

## Q5: How did you approach data quality in this pipeline?

**Context:**
Source data in any real pipeline contains surprises — the Mazda CSVs had `sale_quarter` stored as `Q1/Q2/Q3/Q4` instead of integers, and `customer_satisfaction` stored as text strings (`Satisfied`, `Neutral`) instead of numeric scores.

**Options:**
I could have (a) used `TRY_CAST` everywhere and silently dropped bad values, (b) stopped the pipeline on the first cast error, or (c) detected bad values upfront with a diagnostic query and then fixed them explicitly.

**Decision:**
Option C. I ran a diagnostic across all columns marked for casting to find any non-numeric values before rewriting the transforms. This surfaced exactly 2 problem columns across 9 tables. I then applied targeted fixes — `REGEXP_REPLACE` for the quarter column and a `CASE` expression with a lookup map for the satisfaction column — rather than sprinkling `TRY_CAST` everywhere.

**Outcome:**
The silver layer handles both known issues explicitly and logs data quality scores (completeness % and uniqueness %) for every table after each run to `mazda_metadata.data_quality_scores`. These scores are queryable in Genie: "Which tables had DQ issues in the last run?"

---

## Q6: How does the Genie AI/BI layer work?

**Context:**
The business value of a data pipeline is in its answers, not its tables. I needed a way for non-technical users to query the data without writing SQL.

**Decision:**
Databricks Genie points at the data products layer (not gold or dimensional directly). Each data product has LLM-generated column descriptions embedded in Unity Catalog table comments. Genie uses these descriptions as context when generating SQL. Certified questions — pre-tested SQL queries with business labels — are added to the Genie Space to handle the most common questions reliably.

**What Genie can answer:**
- "Which dealers are ranked top 5 nationally?" → queries `dp_dealer_scorecard`, returns name, revenue, health score
- "How many customers are in the High Value segment?" → queries `dp_customer_360`, filters `customer_segment = 'High Value'`
- "Show me all available data products" → queries `dp_catalog`, returns 6-row table

**Tradeoffs:**
Genie generates SQL at query time and sometimes hallucinates column names that don't exist. Certified questions mitigate this for the most important queries. The business glossary in the instructions field (column value examples, segment definitions) reduces hallucination for ad-hoc queries.
