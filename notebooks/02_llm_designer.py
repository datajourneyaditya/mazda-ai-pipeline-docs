# Databricks notebook source
# MAGIC %pip install groq -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json, re, os, datetime
from groq import Groq

# ── config ───────────────────────────────────────────────
GROQ_API_KEY = "# paste your key here"   
PROFILE_PATH = "/Volumes/workspace/default/mazda_specs/profile.json"
SPEC_PATH    = "/Volumes/workspace/default/mazda_specs/pipeline_spec.json"
SPEC_TABLE   = "workspace.mazda_metadata.pipeline_spec_history"
AGENT_TABLE  = "workspace.mazda_metadata.agent_decisions"
LLM_MODEL    = "llama-3.3-70b-versatile"

# ── Groq client ──────────────────────────────────────────
client = Groq(api_key=GROQ_API_KEY)

# ── reload profile from disk ─────────────────────────────
with open(PROFILE_PATH, "r") as f:
    profile = json.load(f)

# ── LLM helpers ──────────────────────────────────────────
def call_llm(prompt: str, max_tokens: int = 3000) -> str:
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=0.1
    )
    return response.choices[0].message.content

def parse_llm_json(raw: str) -> dict:
    cleaned = re.sub(r"```json|```", "", raw).strip()
    start   = cleaned.find("{")
    end     = cleaned.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError(f"No JSON found:\n{cleaned[:300]}")
    return json.loads(cleaned[start:end])

def log_agent_decision(decision_type, reasoning, input_summary, output_summary):
    safe = lambda s: str(s)[:2000]
    row  = [(decision_type,
             datetime.datetime.now(datetime.timezone.utc).isoformat(),
             safe(reasoning), safe(input_summary), safe(output_summary))]
    df = spark.createDataFrame(
        row,
        ["decision_type", "timestamp", "reasoning",
         "input_summary", "output_summary"]
    )
    df.write.format("delta").mode("append").saveAsTable(AGENT_TABLE)

# ── build slim profile ────────────────────────────────────
slim_tables = []
for t in profile["tables"]:
    slim_cols = []
    for c in t["columns"]:
        slim_cols.append({
            "n":  c["name"],
            "t":  c["type"],
            "c":  c["cardinality"],
            "np": c["null_pct"],
            "pk": c["is_likely_pk"]
        })
    slim_tables.append({
        "table": t["table_name"],
        "rows":  t["row_count"],
        "cols":  slim_cols
    })

strong_fks = {
    col: tables
    for col, tables in profile["fk_candidates"].items()
    if len(tables) >= 3
}

slim_profile = {
    "source_count":  profile["source_count"],
    "total_rows":    profile["total_rows"],
    "tables":        slim_tables,
    "fk_candidates": strong_fks
}

# ── verify ────────────────────────────────────────────────
print(f"✓ Groq client ready")
print(f"✓ Profile loaded   : {profile['source_count']} tables  {profile['total_rows']:,} rows")
print(f"✓ Slim profile     : {len(slim_profile['tables'])} tables  {len(slim_profile['fk_candidates'])} strong FKs")
print(f"✓ All helpers defined")
print(f"\nStrong FK candidates:")
for col, tables in slim_profile["fk_candidates"].items():
    print(f"  {col:<35} → {tables}")

# COMMAND ----------

# Build ultra-slim prompt — PKs + 5 cols per table only
table_lines = []
for t in profile["tables"]:
    pk_cols   = [c for c in t["columns"] if c["is_likely_pk"]]
    high_card = sorted(t["columns"], key=lambda x: x["cardinality"], reverse=True)[:3]
    low_card  = sorted(t["columns"], key=lambda x: x["cardinality"])[:2]
    key_cols  = {c["name"]: c for c in pk_cols + high_card + low_card}

    col_summary = []
    for c in key_cols.values():
        col_summary.append(
            f"  {c['name']} ({c['type']}, card={c['cardinality']}, "
            f"null={c['null_pct']}%, pk={c['is_likely_pk']})"
        )

    table_lines.append(
        f"\n{t['table_name']} — {t['row_count']:,} rows, {t['column_count']} cols\n"
        + "\n".join(col_summary)
        + f"\n  ... +{t['column_count'] - len(key_cols)} more columns"
    )

fk_lines = [
    f"  {col} → {', '.join(tables)}"
    for col, tables in profile["fk_candidates"].items()
    if len(tables) >= 3
]

stage1_prompt = f"""
You are a senior data architect. Design a star schema from these 9 source tables.
You do not know the business domain — reason only from metadata.

SOURCE TABLES (key columns shown per table):
{"".join(table_lines)}

FK OVERLAPS (column appears in 3+ tables):
{chr(10).join(fk_lines)}

DESIGN RULES:
- High row count + unique PK → fact table
- Low cardinality columns shared across tables → dimension
- SCD2: mutable attributes (status, address, classification)
- SCD1: stable reference data
- Max 4 facts, 6 dims

Output ONLY valid JSON. No markdown. No text outside JSON:
{{
  "reasoning": "which tables are facts vs dims and why, grain of each fact, SCD type per dim and why",
  "fact_tables": [
    {{
      "name": "fact_...",
      "source_table": "exact_table_name",
      "grain": "one row per ...",
      "measures": ["col1", "col2"],
      "foreign_keys": [{{"column": "col", "references_dim": "dim_name"}}]
    }}
  ],
  "dimension_tables": [
    {{
      "name": "dim_...",
      "source_table": "exact_table_name",
      "scd_type": 1,
      "scd_reasoning": "why",
      "tracked_columns": [],
      "natural_key": "col_name",
      "attributes": ["col1", "col2"]
    }}
  ]
}}
"""

tokens_est = len(stage1_prompt) // 4
print(f"✓ Ultra-slim Stage 1 prompt built")
print(f"  Chars  : {len(stage1_prompt):,}")
print(f"  Tokens : ~{tokens_est:,}")

if tokens_est > 10000:
    print(f"  ⚠ Still large")
else:
    print(f"  ✓ Within Groq free limit")

print(f"\nCalling Groq — Stage 1: warehouse design...")
print("-" * 55)

raw_stage1 = call_llm(stage1_prompt, max_tokens=3000)

print(f"✓ Response received ({len(raw_stage1):,} chars)")
print(f"\nFirst 500 chars:")
print(raw_stage1[:500])

# COMMAND ----------

stage1 = parse_llm_json(raw_stage1)

facts = stage1["fact_tables"]
dims  = stage1["dimension_tables"]

print("✓ Stage 1 parsed successfully")
print(f"\nAGENT REASONING:")
print("=" * 55)
print(stage1["reasoning"])

print(f"\nFACT TABLES ({len(facts)}):")
for f in facts:
    print(f"\n  {f['name']}")
    print(f"    Source  : {f['source_table']}")
    print(f"    Grain   : {f['grain']}")
    print(f"    Measures: {f['measures']}")
    for fk in f.get("foreign_keys", []):
        print(f"    FK      : {fk['column']} → {fk['references_dim']}")

print(f"\nDIMENSION TABLES ({len(dims)}):")
for d in dims:
    print(f"\n  {d['name']}  [SCD{d['scd_type']}]")
    print(f"    Source      : {d['source_table']}")
    print(f"    Natural key : {d['natural_key']}")
    print(f"    SCD reason  : {d['scd_reasoning']}")
    if d["scd_type"] == 2:
        print(f"    Tracked     : {d['tracked_columns']}")

# COMMAND ----------

table_summary = [
    {"table": t["table_name"], "rows": t["row_count"]}
    for t in profile["tables"]
]

stage2_prompt = f"""
You are a senior data engineer. Given this warehouse design, produce the
complete pipeline spec and data products.

WAREHOUSE DESIGN:
{json.dumps(stage1, indent=2)}

SOURCE TABLES:
{json.dumps(table_summary, indent=2)}

RULES:
- Silver: one entry per source table. Max 5 transforms per table.
  Focus on: cast date/timestamp columns, drop nulls on key cols, fill nulls on measures.
- Dimensional: one entry per fact and dim. Source = mazda_silver table.
- Gold: max 5 tables. Pre-joined aggregations on top of dimensional model.
- Data products: exactly 4, built ON TOP of gold (separate layer above gold).
  Each product needs exactly 5 genie_questions relevant to business users.
- All table prefixes: mazda_bronze, mazda_silver, mazda_dimensional, mazda_gold

Output ONLY valid JSON. No markdown. No text outside JSON:
{{
  "pipeline_spec": {{
    "silver": [
      {{
        "target_table": "mazda_silver.table_name",
        "source_table": "mazda_bronze.table_name",
        "transforms": [
          {{"column": "col", "action": "cast", "detail": "date"}},
          {{"column": "col", "action": "drop", "detail": "reason"}},
          {{"column": "col", "action": "fill_null", "detail": "0"}},
          {{"column": "col", "action": "rename", "detail": "new_name"}}
        ]
      }}
    ],
    "dimensional": [
      {{
        "target_table": "mazda_dimensional.table_name",
        "layer": "dimensional",
        "source_tables": ["mazda_silver.table_name"],
        "join_logic": "",
        "column_map": {{}},
        "surrogate_key": "col_key"
      }}
    ],
    "gold": [
      {{
        "target_table": "mazda_gold.table_name",
        "source_tables": ["mazda_dimensional.fact_name"],
        "group_by": ["col1", "col2"],
        "metrics": [
          {{"name": "metric_name", "expr": "sum(col)"}}
        ],
        "window_functions": []
      }}
    ]
  }},
  "data_products": [
    {{
      "name": "dp_...",
      "display_name": "...",
      "domain": "consumer|operations|executive|ai",
      "owner": "team_name",
      "description": "1-2 sentences describing this product for business users",
      "source_gold_tables": ["mazda_gold.table_name"],
      "refresh_cadence": "daily",
      "sla_completeness_pct": 99,
      "column_descriptions": {{
        "col_name": "plain English description"
      }},
      "genie_questions": [
        "business question 1",
        "business question 2",
        "business question 3",
        "business question 4",
        "business question 5"
      ]
    }}
  ]
}}
"""

tokens_est = len(stage2_prompt) // 4
print(f"✓ Stage 2 prompt built")
print(f"  Chars  : {len(stage2_prompt):,}")
print(f"  Tokens : ~{tokens_est:,}")

print(f"\nCalling Groq — Stage 2: pipeline spec + data products...")
print("-" * 55)

raw_stage2 = call_llm(stage2_prompt, max_tokens=4000)

print(f"✓ Response received ({len(raw_stage2):,} chars)")
print(f"\nFirst 400 chars:")
print(raw_stage2[:400])

# COMMAND ----------

stage2 = parse_llm_json(raw_stage2)

# Merge both stages into full spec
spec = {
    "warehouse_design": {
        "reasoning":        stage1["reasoning"],
        "fact_tables":      stage1["fact_tables"],
        "dimension_tables": stage1["dimension_tables"]
    },
    "pipeline_spec": stage2["pipeline_spec"],
    "data_products":  stage2["data_products"]
}

silver   = spec["pipeline_spec"]["silver"]
dim_spec = spec["pipeline_spec"]["dimensional"]
gold     = spec["pipeline_spec"]["gold"]
products = spec["data_products"]

# Print summary
print("✓ Full spec assembled")
print(f"\n  Silver tables : {len(silver)}")
print(f"  Dimensional   : {len(dim_spec)}")
print(f"  Gold tables   : {len(gold)}")
print(f"  Data products : {len(products)}")

print(f"\nSILVER TRANSFORMS:")
for s in silver:
    print(f"  {s['target_table']}")
    for t in s["transforms"]:
        print(f"    {t['action']:<12} {t['column']} → {t['detail']}")

print(f"\nDIMENSIONAL TABLES:")
for d in dim_spec:
    print(f"  {d['target_table']}")
    print(f"    Sources : {d['source_tables']}")
    print(f"    SK      : {d['surrogate_key']}")

print(f"\nGOLD TABLES:")
for g in gold:
    print(f"  {g['target_table']}")
    print(f"    Sources : {g['source_tables']}")
    print(f"    Group by: {g['group_by']}")
    print(f"    Metrics : {[m['name'] for m in g['metrics']]}")

print(f"\nDATA PRODUCTS:")
for p in products:
    print(f"\n  {p['name']}  [{p['domain']}]")
    print(f"    Display : {p['display_name']}")
    print(f"    Owner   : {p['owner']}")
    print(f"    Desc    : {p['description']}")
    print(f"    Sources : {p['source_gold_tables']}")
    for i, q in enumerate(p['genie_questions'], 1):
        print(f"    Q{i}      : {q}")

# Save to volume
spec_json_str = json.dumps(spec, indent=2)
with open(SPEC_PATH, "w") as f:
    f.write(spec_json_str)
size = os.path.getsize(SPEC_PATH)
print(f"\n✓ pipeline_spec.json saved ({size/1024:.1f} KB)")

# Save to Delta
spark.sql(f"""
    UPDATE {SPEC_TABLE}
    SET is_current = false
    WHERE is_current = true
""")

now = datetime.datetime.now(datetime.timezone.utc).isoformat()
spec_df = spark.createDataFrame(
    [("1.0", now, spec_json_str, True)],
    ["spec_version", "created_at", "spec_json", "is_current"]
)
spec_df.write.format("delta").mode("append").saveAsTable(SPEC_TABLE)
print(f"✓ Spec saved to Delta as version 1.0")

log_agent_decision(
    decision_type  = "warehouse_design",
    reasoning      = stage1["reasoning"][:2000],
    input_summary  = (f"{profile['source_count']} tables, "
                      f"{profile['total_rows']:,} rows"),
    output_summary = (f"{len(facts)} facts, {len(dims)} dims, "
                      f"{len(gold)} gold, {len(products)} products")
)
print(f"✓ Agent decision logged")

print(f"\n{'=' * 55}")
print("NOTEBOOK 2 COMPLETE")
print(f"  Facts    : {[f['name'] for f in facts]}")
print(f"  Dims     : {[d['name'] for d in dims]}")
print(f"  Gold     : {[g['target_table'].split('.')[-1] for g in gold]}")
print(f"  Products : {[p['name'] for p in products]}")
print(f"{'=' * 55}")