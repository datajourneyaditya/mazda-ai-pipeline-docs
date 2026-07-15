# Databricks notebook source
import datetime, json
from pyspark.sql import functions as F

RUN_TABLE = "workspace.mazda_metadata.pipeline_runs"
DQ_TABLE  = "workspace.mazda_metadata.data_quality_scores"

run_id   = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
run_log  = []

def log_table(layer, table, rows):
    run_log.append({"layer": layer, "table": table, "rows": rows})
    print(f"  ✓ {table.split('.')[-1]:<50} {rows:>8,} rows")

def score_dq(df, table_name):
    row_count = df.count()
    if row_count == 0:
        return 0
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

import os

VOLUME = "/Volumes/workspace/default/mazda_raw"

print("=" * 60)
print("BRONZE — raw ingest")
print("=" * 60)

for f in sorted(os.listdir(VOLUME)):
    if not f.endswith(".csv"):
        continue
    tbl = f"workspace.mazda_bronze.{f.replace('.csv','')}"
    df  = spark.read.option("header",True).option("inferSchema",True)\
               .csv(f"{VOLUME}/{f}")
    df.write.format("delta").mode("overwrite")\
      .option("overwriteSchema","true").saveAsTable(tbl)
    log_table("bronze", tbl, df.count())

# COMMAND ----------

print("=" * 60)
print("SILVER — typed, cleaned, deduped")
print("=" * 60)

# ── vehicle_sales ─────────────────────────────────────────
df = spark.sql("""
    SELECT
        transaction_id,
        CAST(sale_datetime AS TIMESTAMP)  AS sale_datetime,
        CAST(sale_date     AS DATE)        AS sale_date,
        CAST(sale_year     AS INT)         AS sale_year,
        CAST(sale_month    AS INT)         AS sale_month,
        CAST(REGEXP_REPLACE(sale_quarter, '[^0-9]', '') AS INT) AS sale_quarter,
        vin,
        model_name,
        CAST(model_year    AS INT)         AS model_year,
        body_style,
        segment,
        powertrain_type,
        engine,
        trim_level,
        drivetrain,
        exterior_color,
        interior_color,
        CAST(msrp                  AS DOUBLE) AS msrp,
        CAST(discount_pct          AS DOUBLE) AS discount_pct,
        CAST(mazda_incentive_usd   AS DOUBLE) AS mazda_incentive_usd,
        CAST(dealer_fee_usd        AS DOUBLE) AS dealer_fee_usd,
        CAST(documentation_fee_usd AS DOUBLE) AS documentation_fee_usd,
        CAST(final_sale_price_usd  AS DOUBLE) AS final_sale_price_usd,
        CAST(fi_income_usd         AS DOUBLE) AS fi_income_usd,
        CAST(trade_in_yn           AS BOOLEAN) AS trade_in_yn,
        trade_in_brand,
        CAST(trade_in_year         AS INT)    AS trade_in_year,
        CAST(trade_in_value_usd    AS DOUBLE) AS trade_in_value_usd,
        finance_type,
        CAST(loan_term_months      AS INT)    AS loan_term_months,
        CAST(lease_term_months     AS INT)    AS lease_term_months,
        CAST(apr_rate_pct          AS DOUBLE) AS apr_rate_pct,
        CAST(down_payment_usd      AS DOUBLE) AS down_payment_usd,
        CAST(days_in_inventory     AS INT)    AS days_in_inventory,
        sale_channel,
        lead_source,
        campaign_id,
        salesperson_id,
        salesperson_name,
        dealer_code,
        dealer_name,
        dealer_city,
        dealer_state,
        dealer_region,
        dealer_tier,
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        customer_city,
        customer_state,
        customer_zip,
        CAST(customer_age          AS INT)    AS customer_age,
        customer_gender,
        CAST(returning_mazda_owner AS BOOLEAN) AS returning_mazda_owner,
        CAST(csi_score             AS DOUBLE) AS csi_score,
        CAST(csi_response_date     AS DATE)   AS csi_response_date
    FROM workspace.mazda_bronze.vehicle_sales
    WHERE transaction_id IS NOT NULL
      AND vin IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.vehicle_sales")
log_table("silver","workspace.mazda_silver.vehicle_sales", df.count())

# ── service_repair_orders ─────────────────────────────────
df = spark.sql("""
    SELECT
        repair_order_number,
        CAST(repair_order_date AS DATE)      AS repair_order_date,
        CAST(open_datetime     AS TIMESTAMP) AS open_datetime,
        CAST(close_datetime    AS TIMESTAMP) AS close_datetime,
        dealer_code,
        dealer_name,
        dealer_state,
        dealer_region,
        customer_id,
        customer_name,
        customer_phone,
        vin,
        model_name,
        CAST(model_year            AS INT)    AS model_year,
        CAST(mileage_in            AS INT)    AS mileage_in,
        CAST(mileage_out           AS INT)    AS mileage_out,
        CAST(vehicle_age_months    AS INT)    AS vehicle_age_months,
        service_type,
        service_category,
        technician_id,
        technician_name,
        technician_level,
        appointment_type,
        CAST(labor_hours_standard  AS DOUBLE) AS labor_hours_standard,
        CAST(labor_hours_actual    AS DOUBLE) AS labor_hours_actual,
        CAST(labor_rate_usd        AS DOUBLE) AS labor_rate_usd,
        CAST(labor_amount_usd      AS DOUBLE) AS labor_amount_usd,
        CAST(parts_amount_usd      AS DOUBLE) AS parts_amount_usd,
        CAST(misc_charges_usd      AS DOUBLE) AS misc_charges_usd,
        CAST(total_ro_amount_usd   AS DOUBLE) AS total_ro_amount_usd,
        CAST(customer_pay_usd      AS DOUBLE) AS customer_pay_usd,
        CAST(warranty_claim_usd    AS DOUBLE) AS warranty_claim_usd,
        CAST(is_recall    AS BOOLEAN) AS is_recall,
        CAST(is_warranty  AS BOOLEAN) AS is_warranty,
        CAST(wait_time_minutes AS INT)    AS wait_time_minutes,
        CAST(loaner_provided   AS BOOLEAN) AS loaner_provided,
        CAST(return_visit_same_issue AS BOOLEAN) AS return_visit_same_issue,
        CAST(csi_score AS DOUBLE) AS csi_score
    FROM workspace.mazda_bronze.service_repair_orders
    WHERE repair_order_number IS NOT NULL
      AND vin IS NOT NULL
      AND dealer_code IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.service_repair_orders")
log_table("silver","workspace.mazda_silver.service_repair_orders", df.count())

# ── warranty_claims ───────────────────────────────────────
df = spark.sql("""
    SELECT
        claim_id,
        CAST(claim_date   AS DATE)       AS claim_date,
        CAST(claim_year   AS INT)        AS claim_year,
        CAST(claim_month  AS INT)        AS claim_month,
        CAST(open_datetime AS TIMESTAMP) AS open_datetime,
        CAST(close_date   AS DATE)       AS close_date,
        CAST(days_to_resolve AS INT)     AS days_to_resolve,
        status,
        vin,
        model_name,
        CAST(model_year         AS INT)    AS model_year,
        powertrain_type,
        CAST(mileage_at_claim   AS INT)    AS mileage_at_claim,
        CAST(vehicle_age_months AS INT)    AS vehicle_age_months,
        customer_id,
        customer_name,
        customer_state,
        dealer_code,
        dealer_name,
        dealer_state,
        technician_id,
        defect_description,
        defect_category,
        severity_level,
        CAST(is_recall_related AS BOOLEAN) AS is_recall_related,
        recall_campaign_id,
        CAST(is_repeat_claim   AS BOOLEAN) AS is_repeat_claim,
        part_number,
        part_name,
        part_category,
        supplier_name,
        CAST(labor_hours_claimed    AS DOUBLE) AS labor_hours_claimed,
        CAST(labor_rate_usd         AS DOUBLE) AS labor_rate_usd,
        CAST(parts_cost_usd         AS DOUBLE) AS parts_cost_usd,
        CAST(total_repair_cost_usd  AS DOUBLE) AS total_repair_cost_usd,
        CAST(mazda_liability_pct    AS DOUBLE) AS mazda_liability_pct,
        CAST(mazda_cost_usd         AS DOUBLE) AS mazda_cost_usd,
        CAST(supplier_recovery_usd  AS DOUBLE) AS supplier_recovery_usd,
        root_cause,
        corrective_action,
        CAST(nhtsa_reportable AS BOOLEAN) AS nhtsa_reportable,
        CASE customer_satisfaction
            WHEN 'Satisfied'    THEN 4.0
            WHEN 'Neutral'      THEN 3.0
            WHEN 'Dissatisfied' THEN 2.0
            WHEN 'Very Satisfied'    THEN 5.0
            WHEN 'Very Dissatisfied' THEN 1.0
            ELSE TRY_CAST(customer_satisfaction AS DOUBLE)
        END                                    AS customer_satisfaction
    FROM workspace.mazda_bronze.warranty_claims
    WHERE claim_id IS NOT NULL
      AND vin IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.warranty_claims")
log_table("silver","workspace.mazda_silver.warranty_claims", df.count())

# ── crm_leads ─────────────────────────────────────────────
df = spark.sql("""
    SELECT
        lead_id,
        CAST(lead_created_datetime AS TIMESTAMP) AS lead_created_datetime,
        CAST(lead_date  AS DATE)  AS lead_date,
        CAST(lead_year  AS INT)   AS lead_year,
        CAST(lead_month AS INT)   AS lead_month,
        campaign_id,
        lead_source,
        lead_channel,
        device_type,
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        city,
        state,
        zip_code,
        CAST(age AS INT)           AS age,
        gender,
        income_band,
        model_of_interest,
        segment_of_interest,
        powertrain_preference,
        CAST(lead_score AS DOUBLE) AS lead_score,
        lead_status,
        CAST(is_converted         AS BOOLEAN) AS is_converted,
        CAST(conversion_date      AS DATE)    AS conversion_date,
        CAST(days_to_convert      AS INT)     AS days_to_convert,
        CAST(test_drive_completed AS BOOLEAN) AS test_drive_completed,
        CAST(test_drive_date      AS DATE)    AS test_drive_date,
        assigned_dealer_code,
        assigned_dealer_name,
        assigned_dealer_state,
        lost_reason,
        competitor_considered,
        CAST(website_sessions    AS INT) AS website_sessions,
        CAST(pages_viewed        AS INT) AS pages_viewed,
        CAST(follow_up_attempts  AS INT) AS follow_up_attempts
    FROM workspace.mazda_bronze.crm_leads
    WHERE lead_id IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.crm_leads")
log_table("silver","workspace.mazda_silver.crm_leads", df.count())

# ── dealer_inventory_snapshots ────────────────────────────
df = spark.sql("""
    SELECT
        CAST(snapshot_date AS DATE)  AS snapshot_date,
        CAST(snapshot_week AS INT)   AS snapshot_week,
        CAST(snapshot_year AS INT)   AS snapshot_year,
        dealer_code,
        dealer_name,
        dealer_city,
        dealer_state,
        dealer_region,
        vin,
        model_name,
        CAST(model_year AS INT)            AS model_year,
        trim_level,
        body_style,
        segment,
        powertrain_type,
        drivetrain,
        exterior_color,
        interior_color,
        CAST(msrp AS DOUBLE)               AS msrp,
        CAST(units_on_hand    AS INT)      AS units_on_hand,
        CAST(units_in_transit AS INT)      AS units_in_transit,
        CAST(units_on_order   AS INT)      AS units_on_order,
        CAST(days_supply      AS DOUBLE)   AS days_supply,
        CAST(days_on_lot_this_unit AS INT) AS days_on_lot_this_unit,
        CAST(is_aged_60_plus  AS BOOLEAN)  AS is_aged_60_plus,
        CAST(is_aged_90_plus  AS BOOLEAN)  AS is_aged_90_plus,
        CAST(sticker_adjustment AS DOUBLE) AS sticker_adjustment,
        allocation_source,
        certification_status
    FROM workspace.mazda_bronze.dealer_inventory_snapshots
    WHERE dealer_code IS NOT NULL
      AND vin IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.dealer_inventory_snapshots")
log_table("silver","workspace.mazda_silver.dealer_inventory_snapshots", df.count())

# ── employee_records ──────────────────────────────────────
df = spark.sql("""
    SELECT
        employee_id,
        badge_number,
        first_name,
        last_name,
        preferred_name,
        email_work,
        phone_work,
        gender,
        CAST(date_of_birth      AS DATE)   AS date_of_birth,
        nationality,
        department,
        division,
        job_title,
        job_family,
        grade_level,
        employment_status,
        employment_type,
        CAST(hire_date          AS DATE)   AS hire_date,
        CAST(original_hire_date AS DATE)   AS original_hire_date,
        CAST(termination_date   AS DATE)   AS termination_date,
        termination_reason,
        reports_to_emp_id,
        office_location_city,
        office_location_state,
        dealer_code,
        CAST(base_salary_usd    AS DOUBLE) AS base_salary_usd,
        CAST(target_bonus_pct   AS DOUBLE) AS target_bonus_pct,
        CAST(years_of_service   AS DOUBLE) AS years_of_service,
        education_level,
        performance_band_last,
        business_unit,
        cost_center
    FROM workspace.mazda_bronze.employee_records
    WHERE employee_id IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.employee_records")
log_table("silver","workspace.mazda_silver.employee_records", df.count())

# ── manufacturing_production_log ──────────────────────────
df = spark.sql("""
    SELECT
        production_log_id,
        CAST(production_date  AS DATE) AS production_date,
        CAST(production_year  AS INT)  AS production_year,
        CAST(production_month AS INT)  AS production_month,
        CAST(production_week  AS INT)  AS production_week,
        shift,
        plant_name,
        plant_location,
        country,
        model_name,
        CAST(model_year AS INT)               AS model_year,
        powertrain_type,
        CAST(planned_units         AS INT)    AS planned_units,
        CAST(actual_units_produced AS INT)    AS actual_units_produced,
        CAST(utilization_pct       AS DOUBLE) AS utilization_pct,
        CAST(defect_units          AS INT)    AS defect_units,
        CAST(defect_rate_pct       AS DOUBLE) AS defect_rate_pct,
        CAST(scraped_units         AS INT)    AS scraped_units,
        CAST(first_pass_yield_pct  AS DOUBLE) AS first_pass_yield_pct,
        CAST(rework_units          AS INT)    AS rework_units,
        CAST(downtime_hours        AS DOUBLE) AS downtime_hours,
        CAST(overtime_hours        AS DOUBLE) AS overtime_hours,
        CAST(oee_pct               AS DOUBLE) AS oee_pct,
        CAST(energy_consumed_kwh   AS DOUBLE) AS energy_consumed_kwh,
        CAST(workforce_headcount   AS INT)    AS workforce_headcount,
        CAST(safety_incidents      AS INT)    AS safety_incidents
    FROM workspace.mazda_bronze.manufacturing_production_log
    WHERE production_log_id IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.manufacturing_production_log")
log_table("silver","workspace.mazda_silver.manufacturing_production_log", df.count())

# ── parts_supply_transactions ─────────────────────────────
df = spark.sql("""
    SELECT
        transaction_id,
        CAST(transaction_datetime AS TIMESTAMP) AS transaction_datetime,
        CAST(transaction_date     AS DATE)      AS transaction_date,
        CAST(transaction_year     AS INT)       AS transaction_year,
        CAST(transaction_month    AS INT)       AS transaction_month,
        transaction_type,
        direction,
        part_number,
        part_name,
        part_category,
        supplier_name,
        supplier_location,
        supplier_tier,
        warehouse_location,
        dealer_code,
        CAST(quantity              AS INT)     AS quantity,
        unit_of_measure,
        CAST(unit_cost_usd         AS DOUBLE)  AS unit_cost_usd,
        CAST(total_cost_usd        AS DOUBLE)  AS total_cost_usd,
        CAST(list_price_usd        AS DOUBLE)  AS list_price_usd,
        purchase_order_number,
        CAST(lead_time_actual_days  AS INT)    AS lead_time_actual_days,
        CAST(lead_time_planned_days AS INT)    AS lead_time_planned_days,
        CAST(on_time_delivery       AS BOOLEAN) AS on_time_delivery,
        quality_status,
        CAST(stock_before          AS INT)     AS stock_before,
        CAST(stock_after           AS INT)     AS stock_after,
        CAST(reorder_triggered     AS BOOLEAN) AS reorder_triggered,
        CAST(freight_cost_usd      AS DOUBLE)  AS freight_cost_usd
    FROM workspace.mazda_bronze.parts_supply_transactions
    WHERE transaction_id IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.parts_supply_transactions")
log_table("silver","workspace.mazda_silver.parts_supply_transactions", df.count())

# ── customer_survey_responses ─────────────────────────────
df = spark.sql("""
    SELECT
        response_id,
        CAST(response_datetime AS TIMESTAMP) AS response_datetime,
        CAST(survey_date       AS DATE)      AS survey_date,
        CAST(survey_year       AS INT)       AS survey_year,
        CAST(survey_month      AS INT)       AS survey_month,
        survey_type,
        survey_timing,
        channel,
        customer_id,
        customer_name,
        CAST(customer_age AS INT)                AS customer_age,
        customer_gender,
        customer_state,
        dealer_code,
        dealer_name,
        dealer_state,
        dealer_region,
        model_purchased_serviced,
        CAST(model_year AS INT)                  AS model_year,
        vin,
        CAST(overall_satisfaction_1_5 AS DOUBLE) AS overall_satisfaction_1_5,
        CAST(nps_score_0_10           AS DOUBLE) AS nps_score_0_10,
        nps_category,
        (would_recommend )        AS would_recommend,
        (would_buy_again )        AS would_buy_again,
        verbatim_positive,
        verbatim_negative,
        (follow_up_required )      AS follow_up_required,
        (case_escalated    )      AS case_escalated
    FROM workspace.mazda_bronze.customer_survey_responses
    WHERE response_id IS NOT NULL
""")
df.write.format("delta").mode("overwrite")\
  .option("overwriteSchema","true")\
  .saveAsTable("workspace.mazda_silver.customer_survey_responses")
log_table("silver","workspace.mazda_silver.customer_survey_responses", df.count())

print(f"\n✓ Silver complete — "
      f"{len([l for l in run_log if l['layer']=='silver'])} tables")

# COMMAND ----------

print("=" * 60)
print("DIMENSIONAL — dimensions")
print("=" * 60)

# ── dim_customer ──────────────────────────────────────────
# Built from vehicle_sales — richest customer attributes
# SCD2: track gender, state, age band changes
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_customer
    USING DELTA AS
    SELECT
        UUID()                  AS customer_sk,
        customer_id,
        customer_first_name     AS first_name,
        customer_last_name      AS last_name,
        customer_email          AS email,
        customer_phone          AS phone,
        customer_city           AS city,
        customer_state          AS state,
        customer_zip            AS zip_code,
        CAST(customer_age AS INT) AS age,
        customer_gender         AS gender,
        CAST(returning_mazda_owner AS BOOLEAN) AS returning_mazda_owner,
        CURRENT_DATE()          AS effective_start,
        CAST(NULL AS DATE)      AS effective_end,
        TRUE                    AS is_current
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY sale_datetime DESC
               ) AS rn
        FROM workspace.mazda_silver.vehicle_sales
        WHERE customer_id IS NOT NULL
    )
    WHERE rn = 1
""")
count = spark.table("workspace.mazda_dimensional.dim_customer").count()
log_table("dimensional","workspace.mazda_dimensional.dim_customer", count)

# ── dim_vehicle ───────────────────────────────────────────
# Built from dealer_inventory_snapshots — most complete vehicle attributes
# SCD1: vehicle specs don't change
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_vehicle
    USING DELTA AS
    SELECT
        UUID()          AS vehicle_sk,
        vin,
        model_name,
        CAST(model_year AS INT) AS model_year,
        trim_level,
        body_style,
        segment,
        powertrain_type,
        drivetrain,
        exterior_color,
        interior_color,
        CAST(msrp AS DOUBLE) AS msrp,
        certification_status
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY vin
                   ORDER BY snapshot_date DESC
               ) AS rn
        FROM workspace.mazda_silver.dealer_inventory_snapshots
        WHERE vin IS NOT NULL
    )
    WHERE rn = 1
""")
count = spark.table("workspace.mazda_dimensional.dim_vehicle").count()
log_table("dimensional","workspace.mazda_dimensional.dim_vehicle", count)

# ── dim_dealer ────────────────────────────────────────────
# SCD2: dealer name, region, state can change
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_dealer
    USING DELTA AS
    SELECT
        UUID()          AS dealer_sk,
        dealer_code,
        dealer_name,
        dealer_city,
        dealer_state,
        dealer_region,
        CURRENT_DATE()     AS effective_start,
        CAST(NULL AS DATE) AS effective_end,
        TRUE               AS is_current
    FROM (
        SELECT
            dealer_code,
            dealer_name,
            dealer_city,
            dealer_state,
            dealer_region,
            ROW_NUMBER() OVER (
                PARTITION BY dealer_code
                ORDER BY snapshot_date DESC
            ) AS rn
        FROM workspace.mazda_silver.dealer_inventory_snapshots
        WHERE dealer_code IS NOT NULL
    )
    WHERE rn = 1
""")
count = spark.table("workspace.mazda_dimensional.dim_dealer").count()
log_table("dimensional","workspace.mazda_dimensional.dim_dealer", count)

# ── dim_model ─────────────────────────────────────────────
# SCD1: model specs are stable reference data
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_model
    USING DELTA AS
    SELECT
        UUID()          AS model_sk,
        model_name,
        CAST(model_year AS INT) AS model_year,
        body_style,
        segment,
        powertrain_type,
        CAST(msrp AS DOUBLE) AS base_msrp
    FROM (
        SELECT
            model_name,
            model_year,
            body_style,
            segment,
            powertrain_type,
            msrp,
            ROW_NUMBER() OVER (
                PARTITION BY model_name, model_year
                ORDER BY snapshot_date DESC
            ) AS rn
        FROM workspace.mazda_silver.dealer_inventory_snapshots
        WHERE model_name IS NOT NULL
    )
    WHERE rn = 1
""")
count = spark.table("workspace.mazda_dimensional.dim_model").count()
log_table("dimensional","workspace.mazda_dimensional.dim_model", count)

# ── dim_employee ──────────────────────────────────────────
# SCD2: job title, department, status can change
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_employee
    USING DELTA AS
    SELECT
        UUID()              AS employee_sk,
        employee_id,
        first_name,
        last_name,
        email_work,
        department,
        division,
        job_title,
        job_family,
        grade_level,
        employment_status,
        employment_type,
        CAST(hire_date AS DATE)  AS hire_date,
        dealer_code,
        business_unit,
        performance_band_last,
        CAST(base_salary_usd AS DOUBLE) AS base_salary_usd,
        CURRENT_DATE()      AS effective_start,
        CAST(NULL AS DATE)  AS effective_end,
        TRUE                AS is_current
    FROM workspace.mazda_silver.employee_records
    WHERE employee_id IS NOT NULL
""")
count = spark.table("workspace.mazda_dimensional.dim_employee").count()
log_table("dimensional","workspace.mazda_dimensional.dim_employee", count)

# ── dim_date ──────────────────────────────────────────────
# Generated date spine from min to max sale date
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.dim_date
    USING DELTA AS
    SELECT
        CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) AS date_key,
        d                                        AS full_date,
        YEAR(d)                                  AS year,
        QUARTER(d)                               AS quarter,
        MONTH(d)                                 AS month,
        DAY(d)                                   AS day,
        DAYOFWEEK(d)                             AS day_of_week,
        DATE_FORMAT(d, 'EEEE')                   AS day_name,
        DATE_FORMAT(d, 'MMMM')                   AS month_name,
        WEEKOFYEAR(d)                            AS week_of_year,
        CASE WHEN DAYOFWEEK(d) IN (1,7)
             THEN TRUE ELSE FALSE END            AS is_weekend
    FROM (
        SELECT EXPLODE(SEQUENCE(
            MIN(sale_date),
            MAX(sale_date),
            INTERVAL 1 DAY
        )) AS d
        FROM workspace.mazda_silver.vehicle_sales
    )
""")
count = spark.table("workspace.mazda_dimensional.dim_date").count()
log_table("dimensional","workspace.mazda_dimensional.dim_date", count)

print(f"\n✓ Dimensions complete")

# COMMAND ----------

print("=" * 60)
print("DIMENSIONAL — fact tables")
print("=" * 60)

# ── fact_vehicle_sales ────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.fact_vehicle_sales
    USING DELTA AS
    SELECT
        vs.transaction_id                           AS sale_id,
        vs.sale_datetime,
        vs.sale_date,
        vs.sale_year,
        vs.sale_month,
        vs.sale_quarter,
        dd.date_key,
        -- Foreign keys to dimensions
        dc.customer_sk,
        vs.customer_id,
        dv.vehicle_sk,
        vs.vin,
        ddl.dealer_sk,
        vs.dealer_code,
        dm.model_sk,
        vs.model_name,
        vs.model_year,
        -- Measures
        CAST(vs.msrp                AS DOUBLE) AS msrp,
        CAST(vs.final_sale_price_usd AS DOUBLE) AS final_sale_price_usd,
        CAST(vs.discount_pct        AS DOUBLE) AS discount_pct,
        CAST(vs.mazda_incentive_usd AS DOUBLE) AS mazda_incentive_usd,
        CAST(vs.fi_income_usd       AS DOUBLE) AS fi_income_usd,
        CAST(vs.trade_in_value_usd  AS DOUBLE) AS trade_in_value_usd,
        CAST(vs.days_in_inventory   AS INT)    AS days_in_inventory,
        vs.trade_in_yn,
        vs.returning_mazda_owner,
        vs.finance_type,
        vs.sale_channel,
        vs.lead_source,
        CAST(vs.csi_score AS DOUBLE) AS csi_score,
        -- Derived measures
        CAST(vs.msrp - vs.final_sale_price_usd AS DOUBLE) AS total_discount_usd
    FROM workspace.mazda_silver.vehicle_sales vs
    LEFT JOIN workspace.mazda_dimensional.dim_customer dc
           ON vs.customer_id = dc.customer_id
          AND dc.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_vehicle dv
           ON vs.vin = dv.vin
    LEFT JOIN workspace.mazda_dimensional.dim_dealer ddl
           ON vs.dealer_code = ddl.dealer_code
          AND ddl.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON vs.model_name = dm.model_name
          AND vs.model_year  = dm.model_year
    LEFT JOIN workspace.mazda_dimensional.dim_date dd
           ON vs.sale_date = dd.full_date
""")
count = spark.table("workspace.mazda_dimensional.fact_vehicle_sales").count()
log_table("dimensional","workspace.mazda_dimensional.fact_vehicle_sales", count)

# ── fact_service_repair_orders ────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.fact_service_repair_orders
    USING DELTA AS
    SELECT
        sro.repair_order_number                     AS repair_order_id,
        sro.repair_order_date,
        sro.open_datetime,
        sro.close_datetime,
        dd.date_key,
        -- Foreign keys
        dc.customer_sk,
        sro.customer_id,
        dv.vehicle_sk,
        sro.vin,
        ddl.dealer_sk,
        sro.dealer_code,
        dm.model_sk,
        sro.model_name,
        sro.model_year,
        -- Service attributes
        sro.service_type,
        sro.service_category,
        sro.technician_id,
        sro.appointment_type,
        -- Measures
        CAST(sro.labor_hours_standard AS DOUBLE) AS labor_hours_standard,
        CAST(sro.labor_hours_actual   AS DOUBLE) AS labor_hours_actual,
        CAST(sro.labor_amount_usd     AS DOUBLE) AS labor_amount_usd,
        CAST(sro.parts_amount_usd     AS DOUBLE) AS parts_amount_usd,
        CAST(sro.total_ro_amount_usd  AS DOUBLE) AS total_ro_amount_usd,
        CAST(sro.customer_pay_usd     AS DOUBLE) AS customer_pay_usd,
        CAST(sro.warranty_claim_usd   AS DOUBLE) AS warranty_claim_usd,
        CAST(sro.wait_time_minutes    AS INT)    AS wait_time_minutes,
        CAST(sro.mileage_in           AS INT)    AS mileage_in,
        CAST(sro.vehicle_age_months   AS INT)    AS vehicle_age_months,
        sro.is_recall,
        sro.is_warranty,
        sro.loaner_provided,
        sro.return_visit_same_issue,
        CAST(sro.csi_score AS DOUBLE) AS csi_score
    FROM workspace.mazda_silver.service_repair_orders sro
    LEFT JOIN workspace.mazda_dimensional.dim_customer dc
           ON sro.customer_id = dc.customer_id
          AND dc.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_vehicle dv
           ON sro.vin = dv.vin
    LEFT JOIN workspace.mazda_dimensional.dim_dealer ddl
           ON sro.dealer_code = ddl.dealer_code
          AND ddl.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON sro.model_name = dm.model_name
          AND sro.model_year  = dm.model_year
    LEFT JOIN workspace.mazda_dimensional.dim_date dd
           ON sro.repair_order_date = dd.full_date
""")
count = spark.table("workspace.mazda_dimensional.fact_service_repair_orders").count()
log_table("dimensional","workspace.mazda_dimensional.fact_service_repair_orders", count)

# ── fact_warranty_claims ──────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.fact_warranty_claims
    USING DELTA AS
    SELECT
        wc.claim_id,
        wc.claim_date,
        wc.claim_year,
        wc.claim_month,
        wc.open_datetime,
        wc.close_date,
        dd.date_key,
        -- Foreign keys
        dc.customer_sk,
        wc.customer_id,
        dv.vehicle_sk,
        wc.vin,
        ddl.dealer_sk,
        wc.dealer_code,
        dm.model_sk,
        wc.model_name,
        wc.model_year,
        -- Claim attributes
        wc.status,
        wc.defect_category,
        wc.severity_level,
        wc.part_number,
        wc.part_name,
        wc.part_category,
        wc.supplier_name,
        wc.root_cause,
        -- Measures
        CAST(wc.days_to_resolve       AS INT)    AS days_to_resolve,
        CAST(wc.mileage_at_claim      AS INT)    AS mileage_at_claim,
        CAST(wc.vehicle_age_months    AS INT)    AS vehicle_age_months,
        CAST(wc.labor_hours_claimed   AS DOUBLE) AS labor_hours_claimed,
        CAST(wc.parts_cost_usd        AS DOUBLE) AS parts_cost_usd,
        CAST(wc.total_repair_cost_usd AS DOUBLE) AS total_repair_cost_usd,
        CAST(wc.mazda_cost_usd        AS DOUBLE) AS mazda_cost_usd,
        CAST(wc.supplier_recovery_usd AS DOUBLE) AS supplier_recovery_usd,
        wc.is_recall_related,
        wc.is_repeat_claim,
        wc.nhtsa_reportable,
        CAST(wc.customer_satisfaction AS DOUBLE) AS customer_satisfaction
    FROM workspace.mazda_silver.warranty_claims wc
    LEFT JOIN workspace.mazda_dimensional.dim_customer dc
           ON wc.customer_id = dc.customer_id
          AND dc.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_vehicle dv
           ON wc.vin = dv.vin
    LEFT JOIN workspace.mazda_dimensional.dim_dealer ddl
           ON wc.dealer_code = ddl.dealer_code
          AND ddl.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON wc.model_name = dm.model_name
          AND wc.model_year  = dm.model_year
    LEFT JOIN workspace.mazda_dimensional.dim_date dd
           ON wc.claim_date = dd.full_date
""")
count = spark.table("workspace.mazda_dimensional.fact_warranty_claims").count()
log_table("dimensional","workspace.mazda_dimensional.fact_warranty_claims", count)

# ── fact_crm_leads ────────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.fact_crm_leads
    USING DELTA AS
    SELECT
        cl.lead_id,
        cl.lead_created_datetime,
        cl.lead_date,
        cl.lead_year,
        cl.lead_month,
        dd.date_key,
        -- Foreign keys
        dc.customer_sk,
        cl.customer_id,
        cl.assigned_dealer_code     AS dealer_code,
        dm.model_sk,
        cl.model_of_interest        AS model_name,
        -- Lead attributes
        cl.lead_source,
        cl.lead_channel,
        cl.device_type,
        cl.campaign_id,
        cl.lead_status,
        cl.income_band,
        cl.powertrain_preference,
        cl.segment_of_interest,
        -- Measures
        CAST(cl.lead_score          AS DOUBLE) AS lead_score,
        CAST(cl.days_to_convert     AS INT)    AS days_to_convert,
        CAST(cl.website_sessions    AS INT)    AS website_sessions,
        CAST(cl.pages_viewed        AS INT)    AS pages_viewed,
        CAST(cl.follow_up_attempts  AS INT)    AS follow_up_attempts,
        cl.is_converted,
        cl.test_drive_completed,
        cl.lost_reason,
        cl.competitor_considered
    FROM workspace.mazda_silver.crm_leads cl
    LEFT JOIN workspace.mazda_dimensional.dim_customer dc
           ON cl.customer_id = dc.customer_id
          AND dc.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON cl.model_of_interest = dm.model_name
    LEFT JOIN workspace.mazda_dimensional.dim_date dd
           ON cl.lead_date = dd.full_date
""")
count = spark.table("workspace.mazda_dimensional.fact_crm_leads").count()
log_table("dimensional","workspace.mazda_dimensional.fact_crm_leads", count)

print(f"\n✓ Facts complete")

# COMMAND ----------

print("=" * 60)
print("GOLD — aggregations")
print("=" * 60)

# ── gold_sales_summary ────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.sales_summary
    USING DELTA AS
    SELECT
        f.sale_year,
        f.sale_month,
        f.sale_quarter,
        d.model_name,
        d.segment,
        d.powertrain_type,
        dl.dealer_region,
        dl.dealer_state,
        COUNT(*)                            AS total_sales,
        SUM(f.final_sale_price_usd)         AS total_revenue,
        AVG(f.final_sale_price_usd)         AS avg_sale_price,
        SUM(f.total_discount_usd)           AS total_discounts,
        AVG(f.discount_pct)                 AS avg_discount_pct,
        SUM(f.fi_income_usd)                AS total_fi_income,
        SUM(CASE WHEN f.trade_in_yn = TRUE
                 THEN 1 ELSE 0 END)         AS trade_in_count,
        AVG(f.days_in_inventory)            AS avg_days_in_inventory,
        AVG(f.csi_score)                    AS avg_csi_score,
        SUM(CASE WHEN f.returning_mazda_owner = TRUE
                 THEN 1 ELSE 0 END)         AS returning_owner_count
    FROM workspace.mazda_dimensional.fact_vehicle_sales f
    LEFT JOIN workspace.mazda_dimensional.dim_vehicle d
           ON f.vin = d.vin
    LEFT JOIN workspace.mazda_dimensional.dim_dealer dl
           ON f.dealer_code = dl.dealer_code
          AND dl.is_current = TRUE
    GROUP BY
        f.sale_year, f.sale_month, f.sale_quarter,
        d.model_name, d.segment, d.powertrain_type,
        dl.dealer_region, dl.dealer_state
""")
count = spark.table("workspace.mazda_gold.sales_summary").count()
log_table("gold","workspace.mazda_gold.sales_summary", count)

# ── gold_service_summary ──────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.service_summary
    USING DELTA AS
    SELECT
        YEAR(f.repair_order_date)           AS repair_year,
        MONTH(f.repair_order_date)          AS repair_month,
        f.service_type,
        f.service_category,
        dl.dealer_region,
        dl.dealer_state,
        dm.model_name,
        COUNT(*)                            AS total_orders,
        SUM(f.total_ro_amount_usd)          AS total_revenue,
        AVG(f.total_ro_amount_usd)          AS avg_order_value,
        SUM(f.labor_amount_usd)             AS total_labor,
        SUM(f.parts_amount_usd)             AS total_parts,
        SUM(f.warranty_claim_usd)           AS total_warranty_recovery,
        AVG(f.labor_hours_actual)           AS avg_labor_hours,
        AVG(f.wait_time_minutes)            AS avg_wait_minutes,
        AVG(f.csi_score)                    AS avg_csi_score,
        SUM(CASE WHEN f.is_recall = TRUE
                 THEN 1 ELSE 0 END)         AS recall_orders,
        SUM(CASE WHEN f.is_warranty = TRUE
                 THEN 1 ELSE 0 END)         AS warranty_orders,
        SUM(CASE WHEN f.return_visit_same_issue = TRUE
                 THEN 1 ELSE 0 END)         AS repeat_visit_count
    FROM workspace.mazda_dimensional.fact_service_repair_orders f
    LEFT JOIN workspace.mazda_dimensional.dim_dealer dl
           ON f.dealer_code = dl.dealer_code
          AND dl.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON f.model_name = dm.model_name
          AND f.model_year  = dm.model_year
    GROUP BY
        YEAR(f.repair_order_date),
        MONTH(f.repair_order_date),
        f.service_type,
        f.service_category,
        dl.dealer_region,
        dl.dealer_state,
        dm.model_name
""")
count = spark.table("workspace.mazda_gold.service_summary").count()
log_table("gold","workspace.mazda_gold.service_summary", count)

# ── gold_warranty_summary ─────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.warranty_summary
    USING DELTA AS
    SELECT
        f.claim_year,
        f.claim_month,
        f.defect_category,
        f.severity_level,
        f.part_category,
        f.supplier_name,
        dm.model_name,
        dm.powertrain_type,
        dl.dealer_region,
        COUNT(*)                            AS total_claims,
        SUM(f.total_repair_cost_usd)        AS total_repair_cost,
        AVG(f.total_repair_cost_usd)        AS avg_repair_cost,
        SUM(f.mazda_cost_usd)               AS total_mazda_cost,
        SUM(f.supplier_recovery_usd)        AS total_supplier_recovery,
        AVG(f.days_to_resolve)              AS avg_days_to_resolve,
        SUM(CASE WHEN f.is_recall_related = TRUE
                 THEN 1 ELSE 0 END)         AS recall_claims,
        SUM(CASE WHEN f.is_repeat_claim = TRUE
                 THEN 1 ELSE 0 END)         AS repeat_claims,
        SUM(CASE WHEN f.nhtsa_reportable = TRUE
                 THEN 1 ELSE 0 END)         AS nhtsa_reportable_count,
        AVG(f.customer_satisfaction)        AS avg_customer_satisfaction
    FROM workspace.mazda_dimensional.fact_warranty_claims f
    LEFT JOIN workspace.mazda_dimensional.dim_model dm
           ON f.model_name = dm.model_name
          AND f.model_year  = dm.model_year
    LEFT JOIN workspace.mazda_dimensional.dim_dealer dl
           ON f.dealer_code = dl.dealer_code
          AND dl.is_current = TRUE
    GROUP BY
        f.claim_year, f.claim_month,
        f.defect_category, f.severity_level,
        f.part_category, f.supplier_name,
        dm.model_name, dm.powertrain_type,
        dl.dealer_region
""")
count = spark.table("workspace.mazda_gold.warranty_summary").count()
log_table("gold","workspace.mazda_gold.warranty_summary", count)

# ── gold_crm_lead_summary ─────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.crm_lead_summary
    USING DELTA AS
    SELECT
        f.lead_year,
        f.lead_month,
        f.lead_source,
        f.lead_channel,
        f.device_type,
        f.segment_of_interest,
        f.powertrain_preference,
        f.income_band,
        COUNT(*)                                AS total_leads,
        SUM(CASE WHEN f.is_converted = TRUE
                 THEN 1 ELSE 0 END)             AS converted_leads,
        ROUND(
            SUM(CASE WHEN f.is_converted = TRUE
                     THEN 1 ELSE 0 END)
            / COUNT(*) * 100, 2)                AS conversion_rate_pct,
        SUM(CASE WHEN f.test_drive_completed = TRUE
                 THEN 1 ELSE 0 END)             AS test_drives,
        AVG(f.lead_score)                       AS avg_lead_score,
        AVG(f.days_to_convert)                  AS avg_days_to_convert,
        AVG(f.website_sessions)                 AS avg_sessions,
        AVG(f.pages_viewed)                     AS avg_pages_viewed,
        AVG(f.follow_up_attempts)               AS avg_follow_up_attempts
    FROM workspace.mazda_dimensional.fact_crm_leads f
    GROUP BY
        f.lead_year, f.lead_month,
        f.lead_source, f.lead_channel,
        f.device_type, f.segment_of_interest,
        f.powertrain_preference, f.income_band
""")
count = spark.table("workspace.mazda_gold.crm_lead_summary").count()
log_table("gold","workspace.mazda_gold.crm_lead_summary", count)

# ── gold_customer_360 ─────────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.customer_360
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
        -- Sales metrics
        COUNT(DISTINCT s.sale_id)               AS total_purchases,
        SUM(s.final_sale_price_usd)             AS total_spend,
        AVG(s.final_sale_price_usd)             AS avg_purchase_value,
        MAX(s.sale_date)                        AS last_purchase_date,
        MIN(s.sale_date)                        AS first_purchase_date,
        -- Service metrics
        COUNT(DISTINCT sro.repair_order_id)     AS total_service_visits,
        SUM(sro.total_ro_amount_usd)            AS total_service_spend,
        AVG(sro.csi_score)                      AS avg_service_csi,
        -- Warranty metrics
        COUNT(DISTINCT wc.claim_id)             AS total_warranty_claims,
        SUM(wc.total_repair_cost_usd)           AS total_warranty_cost,
        -- Satisfaction
        AVG(s.csi_score)                        AS avg_sales_csi
    FROM workspace.mazda_dimensional.dim_customer c
    LEFT JOIN workspace.mazda_dimensional.fact_vehicle_sales s
           ON c.customer_id = s.customer_id
    LEFT JOIN workspace.mazda_dimensional.fact_service_repair_orders sro
           ON c.customer_id = sro.customer_id
    LEFT JOIN workspace.mazda_dimensional.fact_warranty_claims wc
           ON c.customer_id = wc.customer_id
    WHERE c.is_current = TRUE
    GROUP BY
        c.customer_id, c.first_name, c.last_name,
        c.email, c.phone, c.city, c.state,
        c.age, c.gender, c.returning_mazda_owner
""")
count = spark.table("workspace.mazda_gold.customer_360").count()
log_table("gold","workspace.mazda_gold.customer_360", count)

# ── gold_dealer_scorecard ─────────────────────────────────
spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_gold.dealer_scorecard
    USING DELTA AS
    WITH sales_agg AS (
        SELECT
            dealer_code,
            COUNT(DISTINCT sale_id)          AS total_sales,
            SUM(final_sale_price_usd)        AS total_revenue,
            AVG(final_sale_price_usd)        AS avg_sale_price,
            AVG(discount_pct)                AS avg_discount_pct,
            AVG(days_in_inventory)           AS avg_days_in_inventory,
            AVG(csi_score)                   AS avg_sales_csi
        FROM workspace.mazda_dimensional.fact_vehicle_sales
        GROUP BY dealer_code
    ),
    service_agg AS (
        SELECT
            dealer_code,
            COUNT(DISTINCT repair_order_id)  AS total_service_orders,
            SUM(total_ro_amount_usd)         AS total_service_revenue,
            AVG(csi_score)                   AS avg_service_csi,
            SUM(CASE WHEN return_visit_same_issue = TRUE
                     THEN 1 ELSE 0 END)      AS repeat_visit_count
        FROM workspace.mazda_dimensional.fact_service_repair_orders
        GROUP BY dealer_code
    ),
    warranty_agg AS (
        SELECT
            dealer_code,
            COUNT(DISTINCT claim_id)         AS total_warranty_claims,
            SUM(total_repair_cost_usd)       AS total_warranty_cost
        FROM workspace.mazda_dimensional.fact_warranty_claims
        GROUP BY dealer_code
    )
    SELECT
        d.dealer_code,
        d.dealer_name,
        d.dealer_region,
        d.dealer_state,
        COALESCE(s.total_sales, 0)           AS total_sales,
        COALESCE(s.total_revenue, 0)         AS total_revenue,
        COALESCE(s.avg_sale_price, 0)        AS avg_sale_price,
        COALESCE(s.avg_discount_pct, 0)      AS avg_discount_pct,
        COALESCE(s.avg_days_in_inventory, 0) AS avg_days_in_inventory,
        COALESCE(s.avg_sales_csi, 0)         AS avg_sales_csi,
        COALESCE(sro.total_service_orders, 0)  AS total_service_orders,
        COALESCE(sro.total_service_revenue, 0) AS total_service_revenue,
        COALESCE(sro.avg_service_csi, 0)       AS avg_service_csi,
        COALESCE(sro.repeat_visit_count, 0)    AS repeat_visit_count,
        COALESCE(w.total_warranty_claims, 0)   AS total_warranty_claims,
        COALESCE(w.total_warranty_cost, 0)     AS total_warranty_cost
    FROM workspace.mazda_dimensional.dim_dealer d
    LEFT JOIN sales_agg   s   ON d.dealer_code = s.dealer_code
    LEFT JOIN service_agg sro ON d.dealer_code = sro.dealer_code
    LEFT JOIN warranty_agg w  ON d.dealer_code = w.dealer_code
    WHERE d.is_current = TRUE
""")
count = spark.table("workspace.mazda_gold.dealer_scorecard").count()
log_table("gold", "workspace.mazda_gold.dealer_scorecard", count)

print(f"\n✓ Gold complete")

# COMMAND ----------

print("=" * 60)
print("FULL PIPELINE VERIFICATION")
print("=" * 60)

layers = {
    "Bronze":      "workspace.mazda_bronze",
    "Silver":      "workspace.mazda_silver",
    "Dimensional": "workspace.mazda_dimensional",
    "Gold":        "workspace.mazda_gold"
}

grand_total = 0
for layer_name, schema in layers.items():
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()
    layer_rows = 0
    print(f"\n{layer_name} ({len(tables)} tables):")
    for t in tables:
        tbl   = f"{schema}.{t['tableName']}"
        count = spark.table(tbl).count()
        cols  = len(spark.table(tbl).columns)
        layer_rows  += count
        grand_total += count
        print(f"  ✓ {t['tableName']:<45} {count:>8,} rows  {cols:>3} cols")
    print(f"  {'— layer total —':<45} {layer_rows:>8,}")

print(f"\n{'=' * 60}")
print(f"  Grand total rows : {grand_total:,}")
print(f"{'=' * 60}")

# Save run log
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
spark.createDataFrame([{
    "run_id":         run_id,
    "started_at":     now,
    "completed_at":   now,
    "tables_written": len(run_log),
    "layer_detail":   json.dumps(run_log),
    "status":         "success"
}]).write.format("delta").mode("append").saveAsTable(RUN_TABLE)

print(f"\n✓ Run log saved  run_id={run_id}")
print(f"✓ Pipeline v2 complete")

# COMMAND ----------

# Save run log — overwrite to avoid schema conflicts
now = datetime.datetime.now(datetime.timezone.utc).isoformat()

run_df = spark.createDataFrame([{
    "run_id":         run_id,
    "started_at":     now,
    "completed_at":   now,
    "tables_written": len(run_log),
    "layer_detail":   json.dumps(run_log),
    "status":         "success"
}])

run_df.write.format("delta")\
      .mode("overwrite")\
      .option("overwriteSchema", "true")\
      .saveAsTable(RUN_TABLE)

print(f"✓ Run log saved  run_id={run_id}")
print(f"✓ Pipeline v2 complete")

# COMMAND ----------

old_tables = [
    "workspace.mazda_gold.crm_leads_summary",
    "workspace.mazda_gold.customer_summary",
    "workspace.mazda_gold.service_repair_orders_summary",
    "workspace.mazda_gold.vehicle_sales_summary",
    "workspace.mazda_gold.warranty_claims_summary"
]

for tbl in old_tables:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
    print(f"✓ Dropped {tbl}")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE workspace.mazda_dimensional.fact_crm_leads
    USING DELTA AS
    SELECT
        cl.lead_id,
        cl.lead_created_datetime,
        cl.lead_date,
        cl.lead_year,
        cl.lead_month,
        dd.date_key,
        dc.customer_sk,
        cl.customer_id,
        cl.assigned_dealer_code     AS dealer_code,
        cl.model_of_interest        AS model_name,
        cl.lead_source,
        cl.lead_channel,
        cl.device_type,
        cl.campaign_id,
        cl.lead_status,
        cl.income_band,
        cl.powertrain_preference,
        cl.segment_of_interest,
        CAST(cl.lead_score         AS DOUBLE) AS lead_score,
        CAST(cl.days_to_convert    AS INT)    AS days_to_convert,
        CAST(cl.website_sessions   AS INT)    AS website_sessions,
        CAST(cl.pages_viewed       AS INT)    AS pages_viewed,
        CAST(cl.follow_up_attempts AS INT)    AS follow_up_attempts,
        cl.is_converted,
        cl.test_drive_completed,
        cl.lost_reason,
        cl.competitor_considered
    FROM workspace.mazda_silver.crm_leads cl
    LEFT JOIN workspace.mazda_dimensional.dim_customer dc
           ON cl.customer_id = dc.customer_id
          AND dc.is_current = TRUE
    LEFT JOIN workspace.mazda_dimensional.dim_date dd
           ON cl.lead_date = dd.full_date
""")
count = spark.table("workspace.mazda_dimensional.fact_crm_leads").count()
print(f"✓ fact_crm_leads fixed: {count:,} rows")

# COMMAND ----------

# Verify fixes
print("fact_crm_leads:")
print(f"  {spark.table('workspace.mazda_dimensional.fact_crm_leads').count():,} rows")

print("\nGold tables remaining:")
for t in spark.sql("SHOW TABLES IN workspace.mazda_gold").collect():
    count = spark.table(f"workspace.mazda_gold.{t['tableName']}").count()
    print(f"  {t['tableName']:<45} {count:>8,} rows")