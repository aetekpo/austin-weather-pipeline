from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, pandas as pd
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os

def ingest_weather(**context):
    """Austin weather ETL"""
    # OpenWeatherMap API
    api_key = os.getenv('OPENWEATHER_API_KEY')
    url = f"https://api.openweathermap.org/data/2.5/weather?q=Austin,US&appid={api_key}&units=imperial"
    
    data = requests.get(url).json()
    df = pd.DataFrame([{
        'load_timestamp': pd.Timestamp.now(),
        'temp_f': data['main']['temp'],
        'feels_like_f': data['main']['feels_like'],
        'humidity_pct': data['main']['humidity'],
        'weather_desc': data['weather'][0]['description'],
        'wind_speed_mph': data['wind']['speed']
    }])
    
    # Snowflake load
    conn = connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='WEATHER_DB',
        schema='PUBLIC'
    )
    
    success, nchunks, nrows, _ = write_pandas(conn, df, 'WEATHER_DATA')
    print(f"✅ Loaded {nrows} Austin weather rows")
    conn.close()

dag = DAG(
    'austin_weather_prod',
    start_date=datetime(2026, 1, 15),
    schedule='0 6 * * *',  # Daily 6AM CST
    catchup=False,
    tags=['weather', 'snowflake', 'prod']
)

ingest_task = PythonOperator(
    task_id='ingest_weather',
    python_callable=ingest_weather,
    dag=dag
)
