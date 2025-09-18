### S3 Data Layout

Bucket: `s3://storepipeline`

## Landing-layer
- Path: `s3://storepipeline/Landing-layer/`
- Purpose: Raw ingress from sources (as-landed files, no transformation).

Subfolders:
- `batch-jop/`
  - `Store-db/` — hourly OLTP extracts (Postgres via Airflow)
    - Example: `s3://storepipeline/Landing-layer/batch-jop/Store-db/orders/ingestion_date=YYYY-MM-DD/HH=hh/*.csv`
- `Stream-jop/`
  - `Online-Store/` — streaming logs from Kafka/Spark
    - Example: `s3://storepipeline/Landing-layer/Stream-jop/Online-Store/events/ingestion_date=YYYY-MM-DD/HH=hh/*.json`

## Standarized-layer
- Path: `s3://storepipeline/Standarized-layer/`
- Purpose: Validated and standardized datasets after schema and quality checks.

Subfolders:
- `Processing-date/`
  - `Store-db/` — processed batches by processing date
    - Example: `s3://storepipeline/Standarized-layer/Processing-date/Store-db/orders/processing_date=YYYY-MM-DD/HH=hh/*.parquet`
  - `Online-Store/` — processed streams by processing date
    - Example: `s3://storepipeline/Standarized-layer/Processing-date/Online-Store/events/processing_date=YYYY-MM-DD/HH=hh/*.parquet`
- `Rrjected-data/` (Rejected-data)
  - `Store-db/` — rows failing validation from batch jobs
    - Example: `s3://storepipeline/Standarized-layer/Rrjected-data/Store-db/orders/processing_date=YYYY-MM-DD/*.json`
  - `Online-Store/` — events failing validation from streams
    - Example: `s3://storepipeline/Standarized-layer/Rrjected-data/Online-Store/events/processing_date=YYYY-MM-DD/*.json`
- `Valid-data/`
  - `Store-db/` — validated, conformed batch data
    - Example: `s3://storepipeline/Standarized-layer/Valid-data/Store-db/orders/processing_date=YYYY-MM-DD/part-*.parquet`
  - `Online-Store/` — validated, conformed streaming data
    - Example: `s3://storepipeline/Standarized-layer/Valid-data/Online-Store/events/processing_date=YYYY-MM-DD/part-*.parquet`

## Conventions
- Partitioning:
  - `ingestion_date=YYYY-MM-DD/HH=hh` in Landing-layer
  - `processing_date=YYYY-MM-DD/HH=hh` in Standarized-layer (if hourly)
- File formats:
  - Landing: CSV for batch, JSON for streaming
  - Standardized: Parquet (columnar)
- Naming:
  - Entities as folders (e.g., `orders`, `order_details`, `events`)
  - Hyphenated layer names kept as specified

## Notes
- Typo preserved per current structure: `Rrjected-data` (can be renamed to `Rejected-data` later if desired).
- Both `Online-Store` and `Store-db` exist under each Standardized category to keep sources separate while aligning schemas for joins downstream.
