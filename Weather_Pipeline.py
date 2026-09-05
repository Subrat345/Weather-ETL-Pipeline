from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
from sqlalchemy import create_engine, text
import pandas as pd
import requests
import json
engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/weather_pipeline')

cities = {
    "Delhi": {"lat": 28.61, "lon": 77.21},
    "Noida": {"lat": 28.57, "lon": 77.32},
    "Paradeep": {"lat": 20.32, "lon": 86.61},
    "Bhubaneswar": {"lat": 20.30, "lon": 85.84}
}

#Extract data from the API for each city and store it in a list
def extract():
    all_data = []
    for city, coords in cities.items():
      url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&daily=temperature_2m_max,temperature_2m_min,rain_sum,wind_speed_10m_max,weather_code&timezone=Asia/Kolkata"
      extract = requests.get(url).json()
      all_data.append({"City": city, "Data": extract})
    return all_data


def transform(**kwargs):
    all_data = kwargs['ti'].xcom_pull(task_ids = 'Extract_from_API')
    #with open("weather_data.json", "w") as j:
        #json.dump(all_data, j)

    #Transform the data
    #with open("weather_data.json", "r") as r:
        #data = json.load(r)

    Append_data = []
    for city_data in all_data:
        city = city_data["City"]
        daily_data = city_data["Data"]["daily"]
        daily_df = pd.DataFrame(daily_data)
        daily_df["City"] = city
        Append_data.append(daily_df)

    #fact table
    combined_df = pd.concat(Append_data, ignore_index=True)

    #dim_city
    distinct_cities = combined_df["City"].unique()
    city_df = pd.DataFrame(distinct_cities, columns=["City"])
    city_df.insert(0, 'City_ID', range(1, len(city_df) + 1))
    lat_lon_df = pd.DataFrame(cities)
    lat_lon_df = lat_lon_df.transpose().reset_index()
    city_df = city_df.merge(lat_lon_df, left_on='City', right_on='index', how='left').drop(columns=['index'])
    #print(combined_df.head())

    #fact table
    fact_df = combined_df.merge(city_df, on='City', how='left').drop(columns=['lat', 'lon', 'City'])
    #print(fact_df)

    #Dim_weather
    weather_data = {
        95: 'Thunderstorm, slight or moderate',
        96: 'Thunderstorm with slight hail',
        51: 'Light drizzle',
        53: 'Moderate drizzle',
        3: 'Overcast',
    }

    weather_df= pd.DataFrame(weather_data.items(), columns=['weather_code', 'weather_description'])
    weather_df.insert(0, 'Weather_ID', range(1, len(weather_df) + 1))
    fact_df = fact_df.merge(weather_df, on='weather_code', how='left').drop(columns=['weather_code', 'weather_description'])
    return city_df, weather_df, fact_df
    #print(fact_df)

#Load the data to postgres
def load(**kwargs):
    city_df, weather_df, fact_df = kwargs['ti'].xcom_pull(task_ids = 'Transform_Data')

    #incremental load to postgres
    min_date = fact_df['time'].min()
    max_date = fact_df['time'].max()

    with engine.connect() as conn:
        conn.execute(
            text('DELETE FROM "Fact_table" WHERE "time" >= :min_date AND "time" <= :max_date'),
            {"min_date": min_date, "max_date": max_date}
        )
        conn.commit()
    city_df.to_sql('Dim_city', engine, if_exists='replace', index=False)
    weather_df.to_sql('Dim_Weather', engine, if_exists='replace', index=False)
    fact_df.to_sql('Fact_table', engine, if_exists='append', index=False)

#Defining DAG args
default_args = {
    "owner":"airflow",
    "start_date": pendulum.today('UTC'),
    "email":["sonusubrat34@gmail.com"],
    "email_on_failure":True,
    "email_on_retry":True,
    "retry_delay":timedelta(minutes=5),
    "retries":1,
}

dag = DAG(
    "Weather_ETL_Pipeline",
    schedule=timedelta(days=1),
    default_args=default_args
)

#Task-1 Extract
execute_extract = PythonOperator(
    task_id = "Extract_from_API",
    python_callable=extract,
    dag=dag
)

#Task-2 Transform
execute_transform = PythonOperator(
    task_id = "Transform_Data",
    python_callable=transform,
    dag = dag
)

#Task-3 Load
execute_load = PythonOperator(
    task_id = "Load_data",
    python_callable=load,
    dag = dag
)

#Task pipeline
execute_extract >> execute_transform >> execute_load
