/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: pickup_date
    type: date
    description: Trip date
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment method
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips
  - name: total_fare
    type: float
    description: Sum of all fares
  - name: total_tip
    type: float
    description: Sum of all tips
  - name: total_revenue
    type: float
    description: Total revenue (fares + tips + fees)
  - name: avg_trip_distance
    type: float
    description: Average trip distance
  - name: avg_passenger_count
    type: float
    description: Average passengers per trip

@bruin */

SELECT
    CAST(pickup_datetime AS DATE)        AS pickup_date,
    COALESCE(payment_type_name, 'Unknown') AS payment_type_name,
    COUNT(*)                             AS trip_count,
    SUM(fare_amount)                     AS total_fare,
    SUM(tip_amount)                      AS total_tip,
    SUM(total_amount)                    AS total_revenue,
    AVG(trip_distance)                   AS avg_trip_distance,
    AVG(passenger_count)                 AS avg_passenger_count
FROM staging.trips
GROUP BY
    CAST(pickup_datetime AS DATE),
    COALESCE(payment_type_name, 'Unknown')