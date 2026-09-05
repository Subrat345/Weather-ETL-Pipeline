Weather ETL Pipeline

An end-to-end ETL pipeline that pulls daily weather forecast data for four Indian cities, transforms it into a dimensional model, loads it into PostgreSQL with incremental upserts, and visualizes it through an interactive Streamlit dashboard.

Overview
Source: Open-Meteo API — free weather forecast API, no key required
Cities tracked: Delhi, Noida, Paradeep, Bhubaneswar
Orchestration: Apache Airflow
Storage: PostgreSQL
Visualization: Streamlit

Architecture
Open-Meteo API
      │
      ▼
  Extract (Airflow PythonOperator)
      │
      ▼
  Transform (pandas → star schema)
      │
      ▼
  Load (incremental upsert into PostgreSQL)
      │
      ▼
  Streamlit Dashboard
Data Model

The pipeline transforms the raw API response into a small star schema:

Fact_table — daily weather metrics (max/min temperature, rainfall, wind speed) per city per date
Dim_city — city names with latitude/longitude
Dim_Weather — WMO weather code lookup with human-readable descriptions
Key Engineering Decisions

Incremental loading. Open-Meteo returns a rolling 7-day forecast window on every call, meaning consecutive daily runs overlap by several dates. Naively appending each run's data would create duplicate rows for every overlapping date. This pipeline solves that with a delete-then-append upsert: on each run, any existing Fact_table rows falling within the incoming data's date range are deleted first, then the fresh data is appended. This guarantees each (city, date) combination always holds only the most recently fetched forecast — no duplicates, and historical dates outside the current window are preserved.

Dimension tables (Dim_city, Dim_Weather) are small and static, so they're fully replaced (if_exists='replace') on every run rather than upserted.

Dashboard

The Streamlit dashboard connects directly to PostgreSQL and provides:

City filter (multi-select)
KPI summary (average max temperature, total rainfall, average wind speed)
Trend charts for temperature, rainfall, and wind speed by city
Raw data table