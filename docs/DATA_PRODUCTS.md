# Data Products Reference

Specifications for all 6 data products in the Mazda Intelligence Hub.

---

## What is a data product?

A data product is a named, versioned, self-describing data asset built on top of the gold layer. Each product carries:
- A business-readable name and description
- An owner (team responsible)
- A domain (consumer, operations, executive, ai)
- An SLA completeness percentage
- A refresh cadence
- Column descriptions
- 5 Genie-certified natural language questions
- Metadata columns: _product_name, _product_version, _domain, _owner, _refreshed_at

Data products are the only layer exposed to Genie. Business users query products — they never see bronze, silver, dimensional, or gold tables directly.

---

## dp_vehicle_sales_intelligence

**Table:** workspace.mazda_products.dp_vehicle_sales_intelligence
**Domain:** operations
**Owner:** sales_analytics
**SLA:** 99%
**Refresh:** daily
**Rows:** ~2,727 (one row per year/month/quarter/model/segment/region combination)
**Source:** mazda_gold.sales_summary

**Description:** Sales performance by model, region, and time period. Includes revenue, discounts, trade-ins, and loyalty metrics.

**Key columns:**
| Column | Description |
|---|---|
| sale_year, sale_month, sale_quarter | Time dimensions |
| model_name, segment, powertrain_type | Vehicle dimensions |
| dealer_region, dealer_state | Geography dimensions |
| total_sales | Count of sale transactions |
| total_revenue | Sum of final_sale_price_usd |
| avg_sale_price | Average final sale price |
| total_discounts | Sum of discount amounts |
| avg_discount_pct | Average discount percentage off MSRP |
| trade_in_count | Count of trade-in transactions |
| avg_days_in_inventory | Average days vehicle sat before selling |
| avg_csi_score | Average customer satisfaction score |
| returning_owner_count | Count of returning Mazda owners |
| discount_revenue_ratio_pct | Total discounts as % of revenue (enriched) |
| trade_in_rate_pct | Trade-in count as % of total sales (enriched) |
| loyalty_rate_pct | Returning owners as % of total sales (enriched) |

**Certified Genie questions:**
1. Which model has the highest total sales?
2. Which region generates the most revenue?
3. What is the average discount rate by model?
4. How has monthly sales volume trended?
5. What is the loyalty rate by powertrain type?

---

## dp_service_operations_intelligence

**Table:** workspace.mazda_products.dp_service_operations_intelligence
**Domain:** operations
**Owner:** service_ops
**SLA:** 99%
**Refresh:** daily
**Rows:** ~201,670
**Source:** mazda_gold.service_summary

**Description:** Service repair order performance by type, dealer, and model. Includes revenue, labor efficiency, recall rates, and repeat visit tracking.

**Key columns:**
| Column | Description |
|---|---|
| repair_year, repair_month | Time dimensions |
| service_type, service_category | Service classification |
| dealer_region, dealer_state | Geography dimensions |
| model_name | Vehicle model |
| total_orders | Count of repair orders |
| total_revenue | Sum of total_ro_amount_usd |
| avg_order_value | Average repair order value |
| total_labor, total_parts | Labor and parts revenue breakdown |
| total_warranty_recovery | Warranty claim amounts recovered |
| avg_labor_hours | Average actual labor hours |
| avg_wait_minutes | Average customer wait time |
| avg_csi_score | Average service satisfaction score |
| recall_orders | Count of recall-related orders |
| warranty_orders | Count of warranty-covered orders |
| repeat_visit_count | Count of return visits for same issue |
| recall_rate_pct | Recall orders as % of total (enriched) |
| warranty_rate_pct | Warranty orders as % of total (enriched) |
| repeat_visit_rate_pct | Repeat visits as % of total (enriched) |
| labor_revenue_pct | Labor as % of total revenue (enriched) |

---

## dp_warranty_risk_intelligence

**Table:** workspace.mazda_products.dp_warranty_risk_intelligence
**Domain:** operations
**Owner:** warranty_team
**SLA:** 99%
**Refresh:** daily
**Rows:** ~94,233
**Source:** mazda_gold.warranty_summary

**Description:** Warranty claim risk analysis by defect category, severity, supplier, and model. Includes recall rates, supplier recovery, NHTSA tracking.

**Key columns:**
| Column | Description |
|---|---|
| claim_year, claim_month | Time dimensions |
| defect_category, severity_level | Defect classification |
| part_category, supplier_name | Parts and supplier |
| model_name, powertrain_type | Vehicle dimensions |
| dealer_region | Geography |
| total_claims | Count of warranty claims |
| total_repair_cost | Sum of total_repair_cost_usd |
| avg_repair_cost | Average cost per claim |
| total_mazda_cost | Total Mazda liability |
| total_supplier_recovery | Amount recovered from suppliers |
| avg_days_to_resolve | Average claim resolution time |
| recall_claims | Count of recall-related claims |
| repeat_claims | Count of repeat claims for same issue |
| nhtsa_reportable_count | Count of NHTSA reportable incidents |
| avg_customer_satisfaction | Average satisfaction score post-claim |
| recall_claim_rate_pct | Recall claims as % of total (enriched) |
| repeat_claim_rate_pct | Repeat claims as % of total (enriched) |
| supplier_recovery_rate_pct | Recovery as % of total repair cost (enriched) |
| avg_mazda_cost_per_claim | Average Mazda cost per claim (enriched) |

---

## dp_customer_360

**Table:** workspace.mazda_products.dp_customer_360
**Domain:** consumer
**Owner:** customer_analytics
**SLA:** 99%
**Refresh:** daily
**Rows:** ~214,862 (one row per unique customer)
**Source:** mazda_gold.customer_360

**Description:** Single view of every Mazda customer with purchase history, service visits, warranty claims, lifetime spend, and risk tier.

**Key columns:**
| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| first_name, last_name, email, phone | Customer identity |
| city, state, age, gender | Demographics |
| returning_mazda_owner | Was customer a previous Mazda owner |
| total_purchases | Count of vehicle purchases |
| total_spend | Sum of all purchase amounts |
| avg_purchase_value | Average vehicle purchase price |
| last_purchase_date, first_purchase_date | Purchase date range |
| total_service_visits | Count of service repair orders |
| total_service_spend | Total service spend |
| avg_service_csi | Average service satisfaction |
| total_warranty_claims | Count of warranty claims filed |
| total_warranty_cost | Total warranty repair cost |
| avg_sales_csi | Average sales satisfaction |
| total_lifetime_spend | total_spend + total_service_spend (enriched) |
| customer_tenure_days | Days since first purchase (enriched) |
| days_since_last_purchase | Recency metric (enriched) |
| customer_segment | High Value / Returning / Single Purchase / Prospect (enriched) |
| warranty_risk_tier | High Risk / Medium Risk / Low Risk (enriched) |

**Segment logic:**
- High Value: total_purchases >= 3
- Returning: total_purchases = 2
- Single Purchase: total_purchases = 1
- Prospect: no purchases

**Risk tier logic:**
- High Risk: total_warranty_claims > 2
- Medium Risk: total_warranty_claims > 0
- Low Risk: no warranty claims

---

## dp_dealer_scorecard

**Table:** workspace.mazda_products.dp_dealer_scorecard
**Domain:** operations
**Owner:** dealer_ops
**SLA:** 99%
**Refresh:** daily
**Rows:** 678 (one row per active dealer)
**Source:** mazda_gold.dealer_scorecard

**Description:** Dealer performance scorecard combining sales, service, and warranty KPIs with composite health score and national revenue ranking.

**Key columns:**
| Column | Description |
|---|---|
| dealer_code, dealer_name | Dealer identity |
| dealer_region, dealer_state | Geography |
| total_sales | Count of vehicle sales |
| total_revenue | Sum of vehicle sale revenue |
| avg_sale_price | Average sale price |
| avg_discount_pct | Average discount off MSRP |
| avg_days_in_inventory | Average days vehicles sat before selling |
| avg_sales_csi | Average sales satisfaction score |
| total_service_orders | Count of service repair orders |
| total_service_revenue | Sum of service revenue |
| avg_service_csi | Average service satisfaction score |
| repeat_visit_count | Count of return visits for same issue |
| total_warranty_claims | Count of warranty claims |
| total_warranty_cost | Total warranty cost |
| dealer_health_score | Composite 0–100 score (enriched) |
| revenue_rank_in_region | Revenue rank within dealer's region (enriched) |
| revenue_rank_national | Revenue rank nationally (enriched) |

**Health score formula (0–100):**
- Sales CSI score (0–5 scale) weighted 30%
- Service CSI score (0–5 scale) weighted 30%
- Pricing discipline (inverse of discount %) weighted 20%
- First-visit resolution rate (inverse of repeat visits) weighted 20%

---

## dp_crm_lead_intelligence

**Table:** workspace.mazda_products.dp_crm_lead_intelligence
**Domain:** consumer
**Owner:** marketing_analytics
**SLA:** 98%
**Refresh:** daily
**Rows:** ~342,116
**Source:** mazda_gold.crm_lead_summary

**Description:** CRM lead funnel analysis by source, channel, and customer segment. Includes conversion rates, test drive rates, and engagement metrics.

**Key columns:**
| Column | Description |
|---|---|
| lead_year, lead_month | Time dimensions |
| lead_source, lead_channel | Attribution dimensions |
| device_type | Device used to submit lead |
| segment_of_interest | Vehicle segment the customer was interested in |
| powertrain_preference | Preferred powertrain (EV, hybrid, ICE) |
| income_band | Customer income classification |
| total_leads | Count of leads |
| converted_leads | Count of leads that resulted in a purchase |
| conversion_rate_pct | Converted / total * 100 |
| test_drives | Count of test drives completed |
| avg_lead_score | Average engagement/quality score |
| avg_days_to_convert | Average days from lead to purchase |
| avg_sessions | Average website sessions per lead |
| avg_pages_viewed | Average pages viewed per lead |
| avg_follow_up_attempts | Average follow-up contact attempts |
| test_drive_rate_pct | Test drives as % of leads (enriched) |
| test_drive_to_sale_pct | Conversions as % of test drives (enriched) |

---

## dp_catalog

**Table:** workspace.mazda_products.dp_catalog
**Rows:** 6 (one per data product)

Registry of all data products. Queryable by Genie for discovery queries.

**Columns:** product_name, display_name, domain, owner, description, refresh_cadence, sla_pct, source_tables, genie_questions
