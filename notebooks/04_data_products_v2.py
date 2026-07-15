# Databricks notebook source
import json, datetime
from pyspark.sql import functions as F

SPEC_PATH   = "/Volumes/workspace/default/mazda_specs/pipeline_spec.json"
DQ_TABLE    = "workspace.mazda_metadata.data_quality_scores"
CATALOG_TBL = "workspace.mazda_products.dp_catalog"

with open(SPEC_PATH, "r") as f:
    spec = json.load(f)

run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

def score_dq(df, table_name):
    row_count = df.count()
    if row_count == 0:
        return 0.0
    null_rates = [
        df.filter(F.col(c).isNull()).count() / row_count
        for c in df.columns
    ]
    completeness = round((1 - sum(null_rates)/len(null_rates)) * 100, 2)
    distinct     = df.select(df.columns[0]).distinct().count()
    uniqueness   = round(distinct / row_count * 100, 2)
    dq_score     = round((completeness + uniqueness) / 2, 2)
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    spark.createDataFrame(
        [(run_id, table_name, completeness, uniqueness, dq_score, now)],
        ["run_id","table_name","completeness_pct","uniqueness_pct","dq_score","scored_at"]
    ).write.format("delta").mode("append").saveAsTable(DQ_TABLE)
    return dq_score

print(f"✓ Setup complete  run_id={run_id}")

# COMMAND ----------

# Clean slate — drop old product tables from first run
old_products = spark.sql("SHOW TABLES IN workspace.mazda_products").collect()
for t in old_products:
    spark.sql(f"DROP TABLE IF EXISTS workspace.mazda_products.{t['tableName']}")
    print(f"✓ Dropped workspace.mazda_products.{t['tableName']}")

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.mazda_products")
print("✓ Schema ready")

# COMMAND ----------

print("Building dp_vehicle_sales_intelligence...")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_products.dp_vehicle_sales_intelligence
    USING DELTA AS
    SELECT
        s.sale_year,
        s.sale_month,
        s.sale_quarter,
        s.model_name,
        s.segment,
        s.powertrain_type,
        s.dealer_region,
        s.dealer_state,
        s.total_sales,
        s.total_revenue,
        s.avg_sale_price,
        s.total_discounts,
        s.avg_discount_pct,
        s.total_fi_income,
        s.trade_in_count,
        s.avg_days_in_inventory,
        s.avg_csi_score,
        s.returning_owner_count,
        -- Enriched metrics
        ROUND(s.total_discounts / NULLIF(s.total_revenue, 0) * 100, 2)
                                            AS discount_revenue_ratio_pct,
        ROUND(s.trade_in_count / NULLIF(s.total_sales, 0) * 100, 2)
                                            AS trade_in_rate_pct,
        ROUND(s.returning_owner_count / NULLIF(s.total_sales, 0) * 100, 2)
                                            AS loyalty_rate_pct,
        -- Product contract columns
        'dp_vehicle_sales_intelligence'     AS _product_name,
        '1.0'                               AS _product_version,
        'operations'                        AS _domain,
        'sales_analytics'                   AS _owner,
        CURRENT_TIMESTAMP()                 AS _refreshed_at
    FROM workspace.mazda_gold.sales_summary s
""")

df    = spark.table("workspace.mazda_products.dp_vehicle_sales_intelligence")
count = df.count()
dq    = score_dq(df, "dp_vehicle_sales_intelligence")

spark.sql("""
    COMMENT ON TABLE workspace.mazda_products.dp_vehicle_sales_intelligence IS
    'Vehicle sales performance by model, region, and time period.
     Includes revenue, discounts, trade-ins, and loyalty metrics.
     Owner: sales_analytics | SLA: 99% | Refresh: daily'
""")

print(f"  ✓ dp_vehicle_sales_intelligence   {count:>8,} rows  DQ={dq}%")

# COMMAND ----------

print("Building dp_service_operations_intelligence...")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_products.dp_service_operations_intelligence
    USING DELTA AS
    SELECT
        s.repair_year,
        s.repair_month,
        s.service_type,
        s.service_category,
        s.dealer_region,
        s.dealer_state,
        s.model_name,
        s.total_orders,
        s.total_revenue,
        s.avg_order_value,
        s.total_labor,
        s.total_parts,
        s.total_warranty_recovery,
        s.avg_labor_hours,
        s.avg_wait_minutes,
        s.avg_csi_score,
        s.recall_orders,
        s.warranty_orders,
        s.repeat_visit_count,
        -- Enriched metrics
        ROUND(s.recall_orders / NULLIF(s.total_orders, 0) * 100, 2)
                                            AS recall_rate_pct,
        ROUND(s.warranty_orders / NULLIF(s.total_orders, 0) * 100, 2)
                                            AS warranty_rate_pct,
        ROUND(s.repeat_visit_count / NULLIF(s.total_orders, 0) * 100, 2)
                                            AS repeat_visit_rate_pct,
        ROUND(s.total_labor / NULLIF(s.total_revenue, 0) * 100, 2)
                                            AS labor_revenue_pct,
        -- Product contract columns
        'dp_service_operations_intelligence' AS _product_name,
        '1.0'                               AS _product_version,
        'operations'                        AS _domain,
        'service_ops'                       AS _owner,
        CURRENT_TIMESTAMP()                 AS _refreshed_at
    FROM workspace.mazda_gold.service_summary s
""")

df    = spark.table("workspace.mazda_products.dp_service_operations_intelligence")
count = df.count()
dq    = score_dq(df, "dp_service_operations_intelligence")

spark.sql("""
    COMMENT ON TABLE workspace.mazda_products.dp_service_operations_intelligence IS
    'Service repair order performance by type, dealer, and model.
     Includes revenue, labor efficiency, recall rates, and repeat visit tracking.
     Owner: service_ops | SLA: 99% | Refresh: daily'
""")

print(f"  ✓ dp_service_operations_intelligence   {count:>8,} rows  DQ={dq}%")

# COMMAND ----------

print("Building dp_customer_360...")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_products.dp_customer_360
    USING DELTA AS
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone,
        c.city,
        c.state,
        c.age,
        c.gender,
        c.returning_mazda_owner,
        c.total_purchases,
        c.total_spend,
        c.avg_purchase_value,
        c.last_purchase_date,
        c.first_purchase_date,
        c.total_service_visits,
        c.total_service_spend,
        c.avg_service_csi,
        c.total_warranty_claims,
        c.total_warranty_cost,
        c.avg_sales_csi,
        -- Enriched metrics
        ROUND(c.total_spend + c.total_service_spend, 2)
                                            AS total_lifetime_spend,
        DATEDIFF(CURRENT_DATE(), c.first_purchase_date)
                                            AS customer_tenure_days,
        DATEDIFF(CURRENT_DATE(), c.last_purchase_date)
                                            AS days_since_last_purchase,
        CASE
            WHEN c.total_purchases >= 3 THEN 'High Value'
            WHEN c.total_purchases = 2  THEN 'Returning'
            WHEN c.total_purchases = 1  THEN 'Single Purchase'
            ELSE 'Prospect'
        END                                 AS customer_segment,
        CASE
            WHEN c.total_warranty_claims > 2 THEN 'High Risk'
            WHEN c.total_warranty_claims > 0 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END                                 AS warranty_risk_tier,
        -- Product contract columns
        'dp_customer_360'                   AS _product_name,
        '1.0'                               AS _product_version,
        'consumer'                          AS _domain,
        'customer_analytics'                AS _owner,
        CURRENT_TIMESTAMP()                 AS _refreshed_at
    FROM workspace.mazda_gold.customer_360 c
""")

df    = spark.table("workspace.mazda_products.dp_customer_360")
count = df.count()
dq    = score_dq(df, "dp_customer_360")

spark.sql("""
    COMMENT ON TABLE workspace.mazda_products.dp_customer_360 IS
    'Single view of every Mazda customer with purchase history,
     service visits, warranty claims, lifetime spend, and risk tier.
     Owner: customer_analytics | SLA: 99% | Refresh: daily'
""")

print(f"  ✓ dp_customer_360   {count:>8,} rows  DQ={dq}%")

# COMMAND ----------

print("Building dp_dealer_scorecard...")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_products.dp_dealer_scorecard
    USING DELTA AS
    SELECT
        d.dealer_code,
        d.dealer_name,
        d.dealer_region,
        d.dealer_state,
        d.total_sales,
        d.total_revenue,
        d.avg_sale_price,
        d.avg_discount_pct,
        d.avg_days_in_inventory,
        d.avg_sales_csi,
        d.total_service_orders,
        d.total_service_revenue,
        d.avg_service_csi,
        d.repeat_visit_count,
        d.total_warranty_claims,
        d.total_warranty_cost,
        -- Composite score (0-100)
        ROUND(
            (LEAST(d.avg_sales_csi / 5.0, 1.0)    * 30) +
            (LEAST(d.avg_service_csi / 5.0, 1.0)  * 30) +
            (GREATEST(1 - (d.avg_discount_pct / 20.0), 0) * 20) +
            (GREATEST(1 - (d.repeat_visit_count /
                NULLIF(d.total_service_orders, 0)), 0) * 20)
        , 1)                                AS dealer_health_score,
        -- Rank within region
        RANK() OVER (
            PARTITION BY d.dealer_region
            ORDER BY d.total_revenue DESC
        )                                   AS revenue_rank_in_region,
        RANK() OVER (
            ORDER BY d.total_revenue DESC
        )                                   AS revenue_rank_national,
        -- Product contract columns
        'dp_dealer_scorecard'               AS _product_name,
        '1.0'                               AS _product_version,
        'operations'                        AS _domain,
        'dealer_ops'                        AS _owner,
        CURRENT_TIMESTAMP()                 AS _refreshed_at
    FROM workspace.mazda_gold.dealer_scorecard d
""")

df    = spark.table("workspace.mazda_products.dp_dealer_scorecard")
count = df.count()
dq    = score_dq(df, "dp_dealer_scorecard")

spark.sql("""
    COMMENT ON TABLE workspace.mazda_products.dp_dealer_scorecard IS
    'Dealer performance scorecard combining sales, service, and warranty KPIs.
     Includes composite health score and national/regional revenue ranking.
     Owner: dealer_ops | SLA: 99% | Refresh: daily'
""")

print(f"  ✓ dp_dealer_scorecard   {count:>8,} rows  DQ={dq}%")

# COMMAND ----------

print("Building dp_crm_lead_intelligence...")

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_products.dp_crm_lead_intelligence
    USING DELTA AS
    SELECT
        l.lead_year,
        l.lead_month,
        l.lead_source,
        l.lead_channel,
        l.device_type,
        l.segment_of_interest,
        l.powertrain_preference,
        l.income_band,
        l.total_leads,
        l.converted_leads,
        l.conversion_rate_pct,
        l.test_drives,
        l.avg_lead_score,
        l.avg_days_to_convert,
        l.avg_sessions,
        l.avg_pages_viewed,
        l.avg_follow_up_attempts,
        -- Enriched metrics
        ROUND(l.test_drives / NULLIF(l.total_leads, 0) * 100, 2)
                                            AS test_drive_rate_pct,
        ROUND(l.converted_leads / NULLIF(l.test_drives, 0) * 100, 2)
                                            AS test_drive_to_sale_pct,
        -- Product contract columns
        'dp_crm_lead_intelligence'          AS _product_name,
        '1.0'                               AS _product_version,
        'consumer'                          AS _domain,
        'marketing_analytics'               AS _owner,
        CURRENT_TIMESTAMP()                 AS _refreshed_at
    FROM workspace.mazda_gold.crm_lead_summary l
""")

df    = spark.table("workspace.mazda_products.dp_crm_lead_intelligence")
count = df.count()
dq    = score_dq(df, "dp_crm_lead_intelligence")

spark.sql("""
    COMMENT ON TABLE workspace.mazda_products.dp_crm_lead_intelligence IS
    'CRM lead funnel analysis by source, channel, and customer segment.
     Includes conversion rates, test drive rates, and engagement metrics.
     Owner: marketing_analytics | SLA: 98% | Refresh: daily'
""")

print(f"  ✓ dp_crm_lead_intelligence   {count:>8,} rows  DQ={dq}%")

# COMMAND ----------

print("Building dp_catalog...")

catalog_rows = [
    ("dp_vehicle_sales_intelligence",   "Vehicle Sales Intelligence",
     "operations",  "sales_analytics",
     "Sales performance by model, region, and time. Revenue, discounts, trade-ins, loyalty.",
     "daily", "99",
     '["mazda_gold.sales_summary"]',
     '["What are the top selling models this year?","Which region has the highest revenue?","What is the average discount by model?","How has sales trended month over month?","Which powertrain type drives the most revenue?"]'),

    ("dp_service_operations_intelligence", "Service Operations Intelligence",
     "operations",  "service_ops",
     "Service repair performance by type, dealer, and model. Labor efficiency and recall tracking.",
     "daily", "99",
     '["mazda_gold.service_summary"]',
     '["Which service category generates the most revenue?","What is the recall rate by model?","Which dealers have the highest repeat visit rates?","What is the average wait time by region?","How does warranty recovery compare to total service revenue?"]'),

    ("dp_warranty_risk_intelligence",   "Warranty Risk Intelligence",
     "operations",  "warranty_team",
     "Warranty claim risk by defect, severity, and supplier. Recall and NHTSA tracking.",
     "daily", "99",
     '["mazda_gold.warranty_summary"]',
     '["Which defect category has the highest claim volume?","What is the average cost per warranty claim by model?","Which suppliers have the highest recovery rates?","How many claims are NHTSA reportable this year?","Which severity level drives the most Mazda cost?"]'),

    ("dp_customer_360",                 "Customer 360",
     "consumer",    "customer_analytics",
     "Single customer view with purchases, service, warranty, lifetime spend, and risk tier.",
     "daily", "99",
     '["mazda_gold.customer_360"]',
     '["How many high value customers do we have?","What is the average lifetime spend per customer?","Which customers have not purchased in over 2 years?","What percentage of customers have warranty claims?","What is the average tenure of returning Mazda owners?"]'),

    ("dp_dealer_scorecard",             "Dealer Scorecard",
     "operations",  "dealer_ops",
     "Dealer health scorecard with composite score, revenue rank, and KPIs across sales and service.",
     "daily", "99",
     '["mazda_gold.dealer_scorecard"]',
     '["Which dealers have the highest health score nationally?","Who are the top 10 dealers by revenue in each region?","Which dealers have the highest repeat service visit rate?","What is the average CSI score by dealer region?","Which dealers have the most warranty claims relative to sales?"]'),

    ("dp_crm_lead_intelligence",        "CRM Lead Intelligence",
     "consumer",    "marketing_analytics",
     "Lead funnel analysis by source, channel, and segment. Conversion and test drive rates.",
     "daily", "98",
     '["mazda_gold.crm_lead_summary"]',
     '["Which lead source has the highest conversion rate?","What is the test drive to sale conversion rate by channel?","Which income band converts fastest?","How many days on average does it take to convert a lead?","Which powertrain preference drives the most leads?"]'),
]

catalog_df = spark.createDataFrame(catalog_rows, [
    "product_name", "display_name", "domain", "owner",
    "description", "refresh_cadence", "sla_pct",
    "source_tables", "genie_questions"
])

catalog_df.write.format("delta").mode("overwrite")\
          .option("overwriteSchema","true")\
          .saveAsTable(CATALOG_TBL)

print(f"  ✓ dp_catalog   {catalog_df.count()} products registered")

# COMMAND ----------

print("=" * 60)
print("DATA PRODUCTS — VERIFICATION")
print("=" * 60)

products = spark.sql("SHOW TABLES IN workspace.mazda_products").collect()
for t in products:
    tbl   = f"workspace.mazda_products.{t['tableName']}"
    count = spark.table(tbl).count()
    cols  = len(spark.table(tbl).columns)
    print(f"  ✓ {t['tableName']:<45} {count:>8,} rows  {cols:>3} cols")

print(f"\n{'=' * 60}")
print("COMPLETE PIPELINE SUMMARY")
print("=" * 60)

all_layers = {
    "Bronze":        "workspace.mazda_bronze",
    "Silver":        "workspace.mazda_silver",
    "Dimensional":   "workspace.mazda_dimensional",
    "Gold":          "workspace.mazda_gold",
    "Data Products": "workspace.mazda_products"
}

for layer, schema in all_layers.items():
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    rows   = sum(
        spark.table(f"{schema}.{t['tableName']}").count()
        for t in tables
    )
    print(f"  {layer:<20} {len(tables):>3} tables  {rows:>12,} rows")

print(f"\n✓ All 5 layers complete")
print(f"✓ {len(products)} data products registered in dp_catalog")
print(f"✓ Ready for Genie setup")