import json
import os
import csv
import time
from datetime import timedelta
import requests
from typing import Dict, Any, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago

# --- CONFIGURATION ---

CITY_NAME = "London"
LOAD_METHOD = "BOTH"  # "S3", "LOCAL", or "BOTH"
OPENWEATHERMAP_CONN_ID = "openweathermap_default"
AWS_CONN_ID = "aws_default"
S3_BUCKET_NAME = "your-unique-airflow-weather-bucket"  # Replace with your bucket name
S3_KEY_PREFIX = "weather_data"
LOCAL_DIR_PATH = "/tmp/weather_data"
API_VERSION = "2.5"
API_ENDPOINT = f"/data/{API_VERSION}/weather"
API_BASE_URL = "https://api.openweathermap.org"

CSV_COLUMNS = [
    "city", "country", "longitude", "latitude",
    "temperature_celsius", "feels_like_celsius", "temp_min_celsius", "temp_max_celsius",
    "pressure_hpa", "humidity_percent", "visibility_meters",
    "wind_speed_mps", "wind_direction_deg", "cloudiness_percent",
    "weather_main", "weather_description", "weather_icon",
    "sunrise_unix_utc", "sunset_unix_utc", "timezone_offset_sec",
    "datetime_unix_utc", "data_fetch_time_utc"
]

# --- HELPERS ---

def get_api_key() -> str:
    api_key = Variable.get("openweathermap_api_key", default_var=None)
    if not api_key:
        raise AirflowFailException("OpenWeatherMap API key not found in Airflow Variables.")
    return api_key

def get_formatted_date(execution_date, format_str='%Y%m%d'):
    return execution_date.strftime(format_str)

def get_formatted_time(execution_date, format_str='%Y%m%d%H%M%S'):
    return execution_date.strftime(format_str)

def dict_to_csv_row(data: Dict[str, Any], columns: List[str]) -> List[Any]:
    return [data.get(col, "") for col in columns]

# --- TASKS ---

def extract_weather_data(**context) -> bool:
    """Extract current weather data from OpenWeatherMap API."""
    api_key = get_api_key()
    ti = context['ti']
    params = {
        'q': CITY_NAME,
        'appid': api_key,
        'units': 'metric'
    }
    response = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not data.get('name') or not data.get('main'):
        raise AirflowFailException("API returned incomplete data structure")
    ti.xcom_push(key='weather_raw_data', value=data)
    return True

def transform_weather_data(**context) -> Dict[str, Any]:
    """Transform raw weather data into a flat, readable format."""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_weather', key='weather_raw_data')
    if not raw_data:
        raise AirflowFailException("No raw data found in XComs.")
    weather_item = raw_data.get("weather", [{}])[0] if raw_data.get("weather") else {}
    transformed_data = {
        "city": raw_data.get("name"),
        "country": raw_data.get("sys", {}).get("country"),
        "longitude": raw_data.get("coord", {}).get("lon"),
        "latitude": raw_data.get("coord", {}).get("lat"),
        "temperature_celsius": raw_data.get("main", {}).get("temp"),
        "feels_like_celsius": raw_data.get("main", {}).get("feels_like"),
        "temp_min_celsius": raw_data.get("main", {}).get("temp_min"),
        "temp_max_celsius": raw_data.get("main", {}).get("temp_max"),
        "pressure_hpa": raw_data.get("main", {}).get("pressure"),
        "humidity_percent": raw_data.get("main", {}).get("humidity"),
        "visibility_meters": raw_data.get("visibility"),
        "wind_speed_mps": raw_data.get("wind", {}).get("speed"),
        "wind_direction_deg": raw_data.get("wind", {}).get("deg"),
        "cloudiness_percent": raw_data.get("clouds", {}).get("all"),
        "weather_main": weather_item.get("main"),
        "weather_description": weather_item.get("description"),
        "weather_icon": weather_item.get("icon"),
        "sunrise_unix_utc": raw_data.get("sys", {}).get("sunrise"),
        "sunset_unix_utc": raw_data.get("sys", {}).get("sunset"),
        "timezone_offset_sec": raw_data.get("timezone"),
        "datetime_unix_utc": raw_data.get("dt"),
        "data_fetch_time_utc": context['ts']
    }
    ti.xcom_push(key='weather_transformed_data', value=transformed_data)
    return transformed_data

def data_quality_check(**context) -> bool:
    """Perform basic data quality checks on the transformed data."""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='transform_weather', key='weather_transformed_data')
    errors = []
    if not data['city']:
        errors.append("City is missing.")
    if not isinstance(data['temperature_celsius'], (float, int)):
        errors.append("Temperature is not a number.")
    if data['temperature_celsius'] is not None and (data['temperature_celsius'] < -100 or data['temperature_celsius'] > 70):
        errors.append("Temperature out of realistic bounds.")
    if not data['weather_main']:
        errors.append("Weather main description is missing.")
    if errors:
        raise AirflowFailException(f"Data quality check failed: {errors}")
    return True

def load_weather_data(**context) -> bool:
    """Load transformed data into both JSON and CSV files, locally and/or in S3."""
    ti = context['ti']
    data_to_load = ti.xcom_pull(task_ids='transform_weather', key='weather_transformed_data')
    execution_date = context['execution_date']
    if not data_to_load:
        raise AirflowFailException("No transformed data found in XComs.")

    date_str = get_formatted_date(execution_date)
    timestamp_str = get_formatted_time(execution_date)
    json_filename = f"weather_{CITY_NAME.lower()}_{timestamp_str}.json"
    csv_filename = f"weather_{CITY_NAME.lower()}_{timestamp_str}.csv"

    data_string = json.dumps(data_to_load, indent=4)
    csv_row = dict_to_csv_row(data_to_load, CSV_COLUMNS)

    # Save locally
    if LOAD_METHOD in ["LOCAL", "BOTH"]:
        daily_dir = os.path.join(LOCAL_DIR_PATH, date_str)
        os.makedirs(daily_dir, exist_ok=True)
        json_path = os.path.join(daily_dir, json_filename)
        csv_path = os.path.join(daily_dir, csv_filename)
        with open(json_path, 'w') as f:
            f.write(data_string)
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_COLUMNS)
            writer.writerow(csv_row)
        ti.xcom_push(key='local_json_path', value=json_path)
        ti.xcom_push(key='local_csv_path', value=csv_path)

    # Save to S3
    if LOAD_METHOD in ["S3", "BOTH"]:
        s3_json_key = f"{S3_KEY_PREFIX}/{date_str}/{json_filename}"
        s3_csv_key = f"{S3_KEY_PREFIX}/{date_str}/{csv_filename}"
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=data_string,
            key=s3_json_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        s3_hook.load_string(
            string_data=",".join(CSV_COLUMNS) + "\n" + ",".join(map(str, csv_row)),
            key=s3_csv_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True
        )
        ti.xcom_push(key='s3_json_key', value=s3_json_key)
        ti.xcom_push(key='s3_csv_key', value=s3_csv_key)
    return True

def api_health_response_check(response):
    if response.status_code != 200:
        return False
    try:
        data = response.json()
        return data.get('name') is not None and data.get('main') is not None
    except Exception:
        return False

def evaluate_pipeline(**context):
    """Evaluate performance and accuracy of the ETL pipeline."""
    ti = context['ti']
    start_time = context['dag_run'].start_date
    end_time = time.time()
    transformed_data = ti.xcom_pull(task_ids='transform_weather', key='weather_transformed_data')
    local_json_path = ti.xcom_pull(task_ids='load_weather_data', key='local_json_path')
    local_csv_path = ti.xcom_pull(task_ids='load_weather_data', key='local_csv_path')
    latency = end_time - start_time.timestamp()
    print(f"Pipeline latency (seconds): {latency:.2f}")
    if transformed_data:
        print(f"Evaluated city: {transformed_data['city']}")
        print(f"Temperature: {transformed_data['temperature_celsius']}°C")
        print(f"Weather: {transformed_data['weather_main']} - {transformed_data['weather_description']}")
    if local_json_path and os.path.exists(local_json_path):
        print(f"JSON file size: {os.path.getsize(local_json_path)/1024:.2f} KB")
    if local_csv_path and os.path.exists(local_csv_path):
        print(f"CSV file size: {os.path.getsize(local_csv_path)/1024:.2f} KB")
    print("Suggestions:")
    print("- Consider partitioning S3 data by date/city for efficient querying.")
    print("- Add schema validation and anomaly detection for production.")
    print("- Use Parquet for large-scale analytics workloads[3].")

def test_with_different_datasets(**context):
    """Test pipeline with different cities and document results."""
    test_cities = ["London", "New York", "Tokyo", "Sydney", "Cape Town"]
    api_key = get_api_key()
    results = []
    for city in test_cities:
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric'
        }
        try:
            response = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data.get('name') or not data.get('main'):
                results.append(f"{city}: Incomplete data")
            else:
                results.append(f"{city}: Success, Temp {data['main']['temp']}°C")
        except Exception as e:
            results.append(f"{city}: Failed - {str(e)}")
    print("Test results for multiple datasets:")
    for r in results:
        print(r)
    context['ti'].xcom_push(key='test_results', value=results)

# --- DAG DEFINITION ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='weather_etl_pipeline_V3',
    default_args=default_args,
    description='ETL pipeline for OpenWeatherMap data to S3/Local/CSV with advanced best practices.',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'etl', 'api', 'aws', 's3', 'csv', 'python', 'testing'],
    max_active_runs=1,
    doc_md="""
    ### Weather ETL Pipeline DAG

    - Extracts weather data from OpenWeatherMap API.
    - Transforms and loads to AWS S3 and local storage in both JSON and CSV formats.
    - Implements advanced sensors for API and file monitoring.
    - Evaluates pipeline performance and accuracy.
    - Tests pipeline with multiple datasets and documents results.
    """
)

check_api_availability = HttpSensor(
    task_id='check_openweathermap_api',
    http_conn_id=OPENWEATHERMAP_CONN_ID,
    endpoint=f'{API_ENDPOINT}?q={CITY_NAME}&appid={{{{ var.value.openweathermap_api_key }}}}&units=metric',
    method='GET',
    response_check=api_health_response_check,
    poke_interval=60,
    timeout=300,
    mode='poke',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_weather',
    python_callable=extract_weather_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_weather',
    python_callable=transform_weather_data,
    dag=dag,
)

data_quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag,
)

file_sensor = FileSensor(
    task_id='check_local_file',
    filepath="{{ ti.xcom_pull(task_ids='load_weather_data', key='local_csv_path') }}",
    poke_interval=30,
    timeout=300,
    mode='poke',
    dag=dag,
)

evaluate_task = PythonOperator(
    task_id='evaluate_pipeline',
    python_callable=evaluate_pipeline,
    dag=dag,
)

test_datasets_task = PythonOperator(
    task_id='test_with_different_datasets',
    python_callable=test_with_different_datasets,
    dag=dag,
)

# --- TASK DEPENDENCIES ---

check_api_availability >> extract_task >> transform_task >> data_quality_task >> load_task >> file_sensor >> evaluate_task >> test_datasets_task
