### Define Data Consumers

This document identifies key consumer groups of the data platform, their primary use cases, the datasets they rely on, access methods, and service expectations. It aligns with the implemented stack: Postgres → Airflow → S3 → Great Expectations → Redshift → dbt (Bronze/Silver/Gold) → Power BI, plus Kafka/S3 and Streamlit for monitoring.

## Consumer Overview
- **Business Intelligence (BI) Analysts**: build dashboards and reports in Power BI using Redshift Gold models.
- **Operations & Support**: monitor service health and data pipeline errors via Streamlit (Kafka/S3 sources).
- **Product Management & Growth**: analyze funnels, retention, and feature adoption in Power BI; request curated datasets.
- **Finance & Accounting**: revenue recognition, sales performance, margin analysis from Redshift curated marts.
- **Data Engineering**: pipeline observability, data quality metrics, SLA monitoring (Streamlit, Airflow, logs).
- **Data Science (future)**: experimentation metrics, candidate feature tables from Silver/Gold; offline training from Redshift/S3.

## Key Datasets (Gold layer unless noted)
- `fact_orders`: order-level facts (amounts, status, dates, channels).
- `fact_order_line_items`: product-level breakdown of orders.
- `dim_customers`: conformed customer attributes, SCD.
- `dim_products`: product catalog attributes, SCD.
- `dq_findings` (Silver): data quality failures summary (from Great Expectations).
- `pipeline_runs` (Ops): orchestration and freshness metrics (from Airflow/logs).

## Access Methods
- **Power BI → Redshift**: DirectQuery or import on the Gold schema; shared semantic models where applicable.
- **Streamlit → Kafka/S3**: near real-time error and health monitoring; links back to Airflow tasks.
- **Ad-hoc SQL → Redshift**: governed schemas with read roles; query templates for common analyses.

## Refresh Cadence and SLAs
- **Batch (OLTP-derived)**: hourly Airflow extractions; Redshift load following validation. Target freshness: ≤ 75 minutes end-to-end.
- **Streaming (Logs)**: Spark Streaming micro-batches to S3 Raw; validations to Staging near real-time. Target latency: ≤ 5 minutes to Silver; ≤ next warehouse load to Gold.
- **Power BI Dashboards**: refresh aligned to warehouse loads (hourly) or business-specified schedules.
- **Monitoring (Streamlit)**: near real-time, updates within 1–2 minutes.

## Data Contracts and Governance
- **Schema expectations**: enforced via Great Expectations; contract changes require versioned change requests.
- **PII handling**: restricted columns in `dim_customers`; role-based access in Redshift and masked views where needed.
- **Lineage**: models tracked in dbt; source-to-report lineage documented for BI consumers.

## Consumer Needs → Dataset Mapping
- **BI Analysts**: `fact_orders`, `fact_order_line_items`, `dim_customers`, `dim_products`, `fact_sessions`.
- **Ops & Support**: `dq_findings`, `pipeline_runs`, Streamlit views from Kafka/S3 Quarantine.
- **Product & Growth**: `fact_sessions`, `fact_cart_events`, joined with orders and customers.
- **Finance**: `fact_orders` with fiscal calendars, `dim_products`, customer segments from `dim_customers`.
- **Executives**: curated KPI views/materializations built on top of Gold models.


## Backlog and Enhancements
- Publish semantic models for Power BI (consistent KPI definitions).
- Add feature store exports for Data Science (planned).
- Expand `dq_findings` with per-rule severity and owner routing.
- Add email/Slack alerts for SLA breaches and data quality failures.
