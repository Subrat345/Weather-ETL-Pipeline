Weather ETL Pipeline

Automated weather data pipeline — Extract, Transform, Load, orchestrated with Airflow, visualized with Streamlit.

What it does?

This pipeline extracts daily weather forecast data for four Indian cities from the Open-Meteo API, transforms it into a star schema, and loads it into a PostgreSQL database with incremental upserts - this task is automated by Apache Airflow. A Streamlit dashboard then connects to the database to visualize trends.

Tech Stack
Python,
Pandas,
PostgreSQL,
Apache Airflow,
XCom for data exchange,
Star Schema (Data Warehouse),
Streamlit,
SQLAlchemy
Data Model
Fact_table — daily weather metrics (max/min temperature, rainfall, wind speed) per city per date
Dim_city — city names with latitude/longitude
Dim_Weather — WMO weather code lookup with descriptions
Key Engineering Decision: Incremental Loading

Open-Meteo returns a rolling 7-day forecast window on every call, so consecutive daily runs overlap by several dates. Naively appending each run's data would create duplicate rows. This pipeline solves that with a delete-then-append upsert: existing Fact_table rows in the incoming data's date range are deleted, then the fresh data is appended — so each (city, date) always holds only the latest forecast, with no duplicates, and older history preserved.

Dashboard
City filter (multi-select)
KPI summary — average max temperature, total rainfall, average wind speed
Trend charts for temperature, rainfall, and wind speed by city
Raw data table