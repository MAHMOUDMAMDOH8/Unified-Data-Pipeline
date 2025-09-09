Building the Data Platform

Objective

Build a modern analytics platform that consolidates sales and operational data from the OLTP system and online store  into Amazon Redshift, enabling analytical reporting, near real-time monitoring, and informed decision-making.

Specifications

- Data Sources: Integrate data from two sources:
  - OLTP (Postgres) extracted via Airflow on an hourly schedule
  - Online Store Logs ingested via Kafka and processed by Spark Streaming
- Data Quality: Validate schemas and apply data quality checks (Great Expectations). Route invalid records to an S3 Quarantine area and log errors for observability.
- Integration: Land data in S3 (Raw → Staging), load into Redshift, and model a single, user-friendly analytical schema using dbt.
- Scope: Focus on latest incremental loads from OLTP and near real-time logs. Full historization is optional and can be added later via dbt snapshots.
- Documentation & Lineage: Document the data model for business and analytics teams; track lineage from sources to reports.

BI: Analysis & Reporting

Objective

Deliver dashboards and monitoring that provide actionable insights into:

- Customer Behavior (from logs joined with OLTP)
- Product Performance (sales and item-level analytics)
- Sales Trends (daily/hourly as appropriate)
- Operational Health (pipeline and data quality signals)

Deliverables

- Power BI dashboards built on Redshift Gold models (facts and dimensions)
- Streamlit monitoring app visualizing ingestion errors, data quality outcomes, and service health
- dbt documentation site (or markdown docs) describing models, sources, and transformations