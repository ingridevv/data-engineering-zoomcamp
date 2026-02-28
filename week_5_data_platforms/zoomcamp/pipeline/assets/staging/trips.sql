/* @bruin
name: staging.trips
type: duckdb.sql

materialization:
  type: table

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: vendor_id
    type: integer
    description: Original vendor code from TLC
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip start time
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip end time
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: float
  - name: rate_code_id
    type: integer
  - name: payment_type_id
    type: integer
  - name: payment_type_name
    type: string
  - name: fare_amount
    type: float
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: improvement_surcharge
    type: float
  - name: tip_amount
    type: float
  - name: tolls_amount
    type: float
  - name: total_amount
    type: float
  - name: extracted_at
    type: timestamp

custom_checks:
  - name: row_count_positive
    description: Staging should never be empty
    query:
      SELECT COUNT(*) > 0
      FROM staging.trips
    value: 1

@bruin */

-- staging query implements normalization, deduplication, and lookup enrichment
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.rate_code_id,
    t.payment_type              AS payment_type_id,
    p.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.improvement_surcharge,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.extracted_at
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
