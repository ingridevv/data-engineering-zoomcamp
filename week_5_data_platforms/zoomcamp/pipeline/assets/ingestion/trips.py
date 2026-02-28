"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: int
  - name: pickup_datetime
    type: string
    primary_key: true
  - name: dropoff_datetime
    type: string
  - name: passenger_count
    type: int
  - name: trip_distance
    type: float
  - name: rate_code_id
    type: int
  - name: payment_type
    type: int
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
  - name: trip_type
    type: int
  - name: extracted_at
    type: string

@bruin"""

import json
import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    """
    # Parse environment variables
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    bruin_vars_str = os.getenv("BRUIN_VARS", "{}")

    # Parse variables
    bruin_vars = json.loads(bruin_vars_str)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # Parse dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Generate list of (year, month) tuples to fetch
    dates_to_fetch = []
    current = start_date
    while current <= end_date:
        dates_to_fetch.append((current.year, current.month))
        current += relativedelta(months=1)

    # Fetch data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    all_dataframes = []
    extracted_at = datetime.utcnow()

    for taxi_type in taxi_types:
        for year, month in dates_to_fetch:
            file_name = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = base_url + file_name

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                df = pd.read_parquet(BytesIO(response.content))
                df["extracted_at"] = extracted_at
                
                # Padronização de nomes de colunas (caso venha do dataset original)
                df.columns = [c.lower() for c in df.columns]
                # Mapeamento básico se necessário (ex: tpep_pickup_datetime -> pickup_datetime)
                df = df.rename(columns={
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'vendorid': 'vendor_id',
                    'ratecodeid': 'rate_code_id',
                })

                all_dataframes.append(df)
                print(f"Loaded {file_name} ({len(df)} rows)")

            except Exception as e:
                print(f"Failed to load {file_name}: {e}")
                continue

    if not all_dataframes:
        return pd.DataFrame()

    # Concatenar
    final_df = pd.concat(all_dataframes, ignore_index=True)

    # --- SOLUÇÃO PARA ERRO DE TIMEZONE NO WINDOWS ---
    # Converter todas as colunas de data para STRING antes de entregar ao Bruin/PyArrow
    cols_to_fix = ['pickup_datetime', 'dropoff_datetime', 'extracted_at']
    for col in cols_to_fix:
        if col in final_df.columns:
            # Remove fuso horário e converte para string
            if pd.api.types.is_datetime64_any_dtype(final_df[col]):
                final_df[col] = final_df[col].dt.tz_localize(None).astype(str)
            else:
                final_df[col] = final_df[col].astype(str)

    print(f"\nTotal rows fetched: {len(final_df)}")
    print("Timezones converted to string for Windows compatibility.")
    
    return final_df