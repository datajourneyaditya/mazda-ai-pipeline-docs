# Genie Setup Guide

Step-by-step instructions to configure Databricks Genie over the Mazda data products.

---

## Step 1 — Open Genie

Left sidebar > Genie (or AI/BI > Genie) > New Genie Space

---

## Step 2 — Name the space

    Name        : Mazda Intelligence Hub
    Description : AI-powered analytics across Mazda sales, service,
                  warranty, customers, dealers, and CRM leads

---

## Step 3 — Add tables

Click Add tables and add all 7:

    workspace.mazda_products.dp_catalog
    workspace.mazda_products.dp_vehicle_sales_intelligence
    workspace.mazda_products.dp_service_operations_intelligence
    workspace.mazda_products.dp_warranty_risk_intelligence
    workspace.mazda_products.dp_customer_360
    workspace.mazda_products.dp_dealer_scorecard
    workspace.mazda_products.dp_crm_lead_intelligence

Do not add gold, silver, bronze, or dimensional tables — data products only.

---

## Step 4 — Add instructions

Paste this into the Instructions field:

    You are an AI analyst for Mazda's data intelligence platform.
    Answer questions using only the data product tables provided.

    DATA PRODUCTS:
    - dp_catalog: registry of all 6 data products with owners and SLAs
    - dp_vehicle_sales_intelligence: sales by model, region, time period.
      Key metrics: total_sales, total_revenue, avg_discount_pct, loyalty_rate_pct, discount_revenue_ratio_pct
    - dp_service_operations_intelligence: service repair orders by type and dealer.
      Key metrics: total_orders, total_revenue, recall_rate_pct, repeat_visit_rate_pct, labor_revenue_pct
    - dp_warranty_risk_intelligence: warranty claims by defect, severity, supplier.
      Key metrics: total_claims, total_repair_cost, recall_claim_rate_pct, avg_days_to_resolve, supplier_recovery_rate_pct
    - dp_customer_360: one row per customer. Full purchase, service, warranty history.
      Key metrics: total_purchases, total_lifetime_spend, customer_segment, warranty_risk_tier, customer_tenure_days
    - dp_dealer_scorecard: one row per dealer. Sales + service + warranty KPIs.
      Key metrics: dealer_health_score (0-100), revenue_rank_national, revenue_rank_in_region, total_revenue
    - dp_crm_lead_intelligence: lead funnel by source, channel, segment.
      Key metrics: total_leads, conversion_rate_pct, test_drive_rate_pct, test_drive_to_sale_pct

    KEY DIMENSION VALUES:
    - customer_segment: 'High Value', 'Returning', 'Single Purchase', 'Prospect'
    - warranty_risk_tier: 'High Risk', 'Medium Risk', 'Low Risk'
    - dealer_health_score: 0 to 100, higher is better
    - _domain values: 'consumer', 'operations', 'executive', 'ai'

    TIME COLUMNS:
    - sale_year, sale_month, sale_quarter (vehicle sales)
    - repair_year, repair_month (service orders)
    - claim_year, claim_month (warranty claims)
    - lead_year, lead_month (CRM leads)

---

## Step 5 — Add certified questions

Add these in the Certified questions section. These are pre-tested and return accurate results.

### dp_catalog queries

    Show me all available data products and their owners
    Which data products are in the operations domain?
    What is the SLA for each data product?

### dp_dealer_scorecard queries

    Which dealers are ranked top 5 nationally by revenue?
    Which dealer has the highest health score in each region?
    Which dealers have the highest repeat service visit rate?
    What is the average sales CSI score by dealer region?

### dp_customer_360 queries

    How many customers are in the High Value segment?
    What is the average lifetime spend per customer?
    Which customers have the highest total lifetime spend?
    How many customers have a High Risk warranty tier?

### dp_vehicle_sales_intelligence queries

    Which model has the highest total sales?
    Which region generates the most revenue?
    What is the average discount rate by model?
    What is the loyalty rate by powertrain type?

### dp_crm_lead_intelligence queries

    Which lead source has the highest conversion rate?
    What is the test drive to sale conversion rate by channel?
    Which income band converts the fastest?
    How many total leads were generated this year?

### dp_warranty_risk_intelligence queries

    Which defect category has the highest claim volume?
    What is the average Mazda cost per warranty claim by model?
    Which suppliers have the highest recovery rates?
    How many claims are NHTSA reportable?

### dp_service_operations_intelligence queries

    Which service category generates the most revenue?
    What is the recall rate by model?
    Which dealers have the highest repeat visit rates?
    What is the average wait time by region?

---

## Step 6 — Save and test

Click Save. Then test with these queries in the Genie chat:

    "Show me all available data products"

Expected: table with 6 rows showing product names, domains, owners, descriptions.

    "Which dealers are ranked top 5 nationally by revenue?"

Expected: table with dealer names, codes, states, revenue, health scores.

    "How many customers are in the High Value segment?"

Expected: single number (customers with total_purchases >= 3).

---

## Tips for best results

Use full column names when asking about specific metrics:
- "What is the dealer_health_score for top dealers?" works better than "What is the score?"

Use the correct domain terminology:
- "repair_year" not "year" for service queries
- "claim_year" not "year" for warranty queries

For customer segment queries, use exact values:
- 'High Value', 'Returning', 'Single Purchase', 'Prospect'

For time filtering, be specific:
- "in 2024" or "where sale_year = 2024"
