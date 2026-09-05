import streamlit as st
import pandas as pd
from sqlalchemy import create_engine


# 1. DATABASE CONNECTION

# Replace with your actual Postgres username/password if different.
# This matches the weather_pipeline DB you've been using in psql.
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "weather_pipeline"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

st.set_page_config(page_title="Weather Pipeline Dashboard", layout="wide")
st.title("🌦️ Weather Pipeline Dashboard")


# 2. LOAD DATA

@st.cache_data(ttl=300)  # cache for 5 minutes so we're not hitting Postgres on every click
def load_data():
    query = """
        SELECT
            f.time,
            c."City",
            f.temperature_2m_max,
            f.temperature_2m_min,
            f.rain_sum,
            f.wind_speed_10m_max,
            w.weather_description
        FROM "Fact_table" f
        JOIN "Dim_city" c ON f."City_ID" = c."City_ID"
        LEFT JOIN "Dim_Weather" w ON f."Weather_ID" = w."Weather_ID"
        ORDER BY f.time
    """
    return pd.read_sql(query, engine)

df = load_data()


# 3. SIDEBAR FILTERS

st.sidebar.header("Filters")
cities = st.sidebar.multiselect(
    "Select cities",
    options=df["City"].unique(),
    default=df["City"].unique()
)

filtered_df = df[df["City"].isin(cities)]


# 4. KPI ROW

col1, col2, col3 = st.columns(3)
col1.metric("Avg Max Temp (°C)", round(filtered_df["temperature_2m_max"].mean(), 1))
col2.metric("Total Rain (mm)", round(filtered_df["rain_sum"].sum(), 1))
col3.metric("Avg Wind Speed (km/h)", round(filtered_df["wind_speed_10m_max"].mean(), 1))


# 5. CHARTS

st.subheader("Max Temperature Over Time by City")
temp_pivot = filtered_df.pivot_table(index="time", columns="City", values="temperature_2m_max")
st.line_chart(temp_pivot)

st.subheader("Rainfall Over Time by City")
rain_pivot = filtered_df.pivot_table(index="time", columns="City", values="rain_sum")
st.bar_chart(rain_pivot)

st.subheader("Wind Speed Over Time by City")
wind_pivot = filtered_df.pivot_table(index="time", columns="City", values="wind_speed_10m_max")
st.line_chart(wind_pivot)


# 6. RAW DATA TABLE

st.subheader("Raw Data")
st.dataframe(filtered_df.sort_values(["City", "time"]), use_container_width=True)