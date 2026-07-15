# Source Data

The Mazda AI Pipeline uses 9 synthetic CSV files representing operational data from a fictional automotive company. The data is synthetic — generated to reflect realistic patterns found in automotive industry datasets.

## Files

Place all 9 CSV files in the Unity Catalog Volume at:
/Volumes/workspace/default/mazda_raw/

| File | Rows | Columns | Primary Key | Description |
|---|---|---|---|---|
| vehicle_sales.csv | 489,848 | 60 | transaction_id | Vehicle sale transactions with pricing, finance details, dealer, customer, and CSI data |
| service_repair_orders.csv | 863,966 | 39 | repair_order_number | Service and repair orders with labor, parts, technician, and warranty recovery |
| crm_leads.csv | 500,000 | 49 | lead_id | CRM lead pipeline with source attribution, engagement metrics, and conversion status |
| parts_supply_transactions.csv | 300,000 | 38 | transaction_id | Parts supply chain transactions with supplier, inventory, and cost data |
| dealer_inventory_snapshots.csv | 377,135 | 29 | vin + snapshot_date | Weekly dealer inventory snapshots with ageing and allocation data |
| customer_survey_responses.csv | 250,000 | 44 | response_id | NPS and satisfaction surveys covering sales and service experiences |
| warranty_claims.csv | 95,000 | 45 | claim_id | Warranty claims with defect classification, costs, supplier recovery, and NHTSA flags |
| manufacturing_production_log.csv | 23,188 | 33 | production_log_id | Plant production records with OEE, defect rates, and downtime |
| employee_records.csv | 14,200 | 42 | employee_id | HR records with salary, performance, tenure, and role data |

Total: 2,913,337 rows

## Known data quality issues (handled by silver layer)

These are intentional data quality issues in the source data that the silver layer cleans:

| Table | Column | Issue | Fix applied |
|---|---|---|---|
| vehicle_sales | sale_quarter | Values like 'Q1', 'Q2' instead of integers | REGEXP_REPLACE strips 'Q' prefix |
| warranty_claims | customer_satisfaction | Text values: 'Satisfied', 'Neutral', 'Dissatisfied' | CASE statement maps to 1.0–5.0 numeric scale |

All boolean columns across all files may contain text variants ('Probably Yes', 'true', 'True', '1') — these are handled by CAST to BOOLEAN in the silver transforms.

## FK relationships (detected by profiler)

The following columns appear in 3 or more tables — these are the primary join keys across the warehouse:

| Column | Tables |
|---|---|
| vin | dealer_inventory_snapshots, vehicle_sales, service_repair_orders, customer_survey_responses, warranty_claims |
| customer_id | vehicle_sales, service_repair_orders, crm_leads, customer_survey_responses, warranty_claims |
| dealer_code | vehicle_sales, dealer_inventory_snapshots, service_repair_orders, warranty_claims, employee_records, parts_supply_transactions, customer_survey_responses |
| model_name | vehicle_sales, dealer_inventory_snapshots, service_repair_orders, manufacturing_production_log, warranty_claims |
| model_year | vehicle_sales, dealer_inventory_snapshots, service_repair_orders, manufacturing_production_log, warranty_claims, customer_survey_responses |
