import json
import os
from datetime import timedelta
import requests
from typing import Dict, Any

# Airflow specific imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago

# --- Configuration Constants ---

# City for weather data
CITY_NAME = "London"

# Define where to load data: "S3" or "LOCAL"
LOAD_METHOD = "LOCAL"  # Change to "S3" for S3 storage

# Define connection IDs used in Airflow UI (Admin -> Connections)
OPENWEATHERMAP_CONN_ID = "openweathermap_default"
AWS_CONN_ID = "aws_default"

# S3 Configuration (only used if LOAD_METHOD is "S3")
S3_BUCKET_NAME = "your-unique-airflow-weather-bucket"  # Replace with your bucket name
S3_KEY_PREFIX = "weather_data"

# Local Filesystem Configuration (only used if LOAD_METHOD is "LOCAL")
LOCAL_DIR_PATH = "/tmp/weather_data"

# API Configurations
API_VERSION = "2.5"
API_ENDPOINT = f"/data/{API_VERSION}/weather"
API_BASE_URL = "https://api.openweathermap.org"

# --- Helper Functions ---

def get_api_key() -> str:
    """
    Retrieves the OpenWeatherMap API key securely from Airflow Variables.
    """
    try:
        api_key = Variable.get("openweathermap_api_key", default_var=None)
        if not api_key:
            raise AirflowFailException("OpenWeatherMap API key not found in Airflow Variables.")
        return api_key
    except Exception as e:
        raise AirflowFailException(f"Error retrieving API key: {str(e)}")

def get_formatted_date(execution_date, format_str='%Y%m%d'):
    """Format execution date for filenames and S3 paths"""
    return execution_date.strftime(format_str)

def get_formatted_time(execution_date, format_str='%Y%m%d%H%M%S'):
    """Format execution date and time for filenames and S3 paths"""
    return execution_date.strftime(format_str)

# --- Task Functions ---

def extract_weather_data(**context) -> bool:
    """Extract current weather data from OpenWeatherMap API."""
    print(f"Starting weather data extraction for {CITY_NAME}...")
    api_key = get_api_key()
    ti = context['ti']
    
    # Build API URL with query parameters
    params = {
        'q': CITY_NAME,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        response = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Validate essential data is present
        if not data.get('name') or not data.get('main'):
            raise AirflowFailException("API returned incomplete data structure")
            
        print(f"Successfully extracted weather data for {data.get('name', CITY_NAME)}")

        # Push data to XComs for the next task
        ti.xcom_push(key='weather_raw_data', value=data)
        return True
        
    except requests.exceptions.RequestException as e:
        raise AirflowFailException(f"API request failed: {str(e)}")
    except json.JSONDecodeError:
        raise AirflowFailException("Failed to parse API response as JSON")
    except Exception as e:
        raise AirflowFailException(f"Unexpected error during extraction: {str(e)}")


def transform_weather_data(**context) -> Dict[str, Any]:
    """Transform the raw weather data into a flattened format."""
    print("Starting data transformation...")
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_weather', key='weather_raw_data')

    if not raw_data:
        raise AirflowFailException("No raw data found in XComs. Extraction may have failed.")

    try:
        # Get the first weather item safely or default to empty dict
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
        
        # Log a preview of the transformed data without sensitive information
        print(f"Transformed data for city: {transformed_data['city']}, temp: {transformed_data['temperature_celsius']}°C")

        # Push transformed data to XComs for the load task
        ti.xcom_push(key='weather_transformed_data', value=transformed_data)
        return transformed_data
        
    except Exception as e:
        raise AirflowFailException(f"Error during transformation: {str(e)}")


def load_weather_data(**context) -> bool:
    """Load the transformed data into the target system (Local or S3)."""
    print(f"Starting data loading using method: {LOAD_METHOD}...")
    ti = context['ti']
    data_to_load = ti.xcom_pull(task_ids='transform_weather', key='weather_transformed_data')
    execution_date = context['execution_date']

    if not data_to_load:
        raise AirflowFailException("No transformed data found in XComs. Transform task may have failed.")

    # Generate filenames with date/time components
    date_str = get_formatted_date(execution_date)
    timestamp_str = get_formatted_time(execution_date)
    filename = f"weather_{CITY_NAME.lower()}_{timestamp_str}.json"

    try:
        # Convert to JSON string with pretty formatting
        data_string = json.dumps(data_to_load, indent=4)

        if LOAD_METHOD == "LOCAL":
            # Ensure directory exists
            daily_dir = os.path.join(LOCAL_DIR_PATH, date_str)
            os.makedirs(daily_dir, exist_ok=True)
            
            file_path = os.path.join(daily_dir, filename)
            with open(file_path, 'w') as f:
                f.write(data_string)
            print(f"Data loaded successfully to local file: {file_path}")

        elif LOAD_METHOD == "S3":
            s3_key = f"{S3_KEY_PREFIX}/{date_str}/{filename}"
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            s3_hook.load_string(
                string_data=data_string,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            print(f"Data loaded successfully to S3: s3://{S3_BUCKET_NAME}/{s3_key}")

        else:
            raise AirflowFailException(f"Invalid LOAD_METHOD configured: {LOAD_METHOD}. Must be 'S3' or 'LOCAL'.")

        print("Loading complete.")
        return True

    except Exception as e:
        raise AirflowFailException(f"Error during loading: {str(e)}")


def api_health_response_check(response):
    """Validates the API response for the health check."""
    # Check for HTTP 200 OK
    if response.status_code != 200:
        return False
        
    # Validate response contains required data
    try:
        data = response.json()
        # Check minimal required data structure
        return data.get('name') is not None and data.get('main') is not None
    except:
        return False

# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],  # Replace with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),  # Standard timedelta instead of pendulum.duration
}

# Define the DAG
dag = DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='ETL pipeline for OpenWeatherMap data to S3/Local',
    schedule_interval='@hourly',  # Standard cron expression instead of pendulum.duration
    start_date=days_ago(1),  # Using days_ago helper instead of pendulum.datetime
    catchup=False,
    tags=['weather', 'etl', 'api', 'aws', 's3', 'python'],
    max_active_runs=1,
    doc_md="""
    ### Weather ETL Pipeline DAG
    This DAG implements an ETL process for weather data:
    - **Sensor**: Checks OpenWeatherMap API availability.
    - **Extract**: Fetches current weather data for London.
    - **Transform**: Flattens and formats the JSON data.
    - **Load**: Saves the data to AWS S3 or Local filesystem based on configuration.
    
    **Requires:**
    - Airflow Variable: `openweathermap_api_key`
    - Airflow Connections: `openweathermap_default` (HTTP), `aws_default` (AWS)
    """
)

# Task 1: Sensor to check API availability
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

# Task 2: Extract data
extract_task = PythonOperator(
    task_id='extract_weather',
    python_callable=extract_weather_data,
    dag=dag,
)

# Task 3: Transform data
transform_task = PythonOperator(
    task_id='transform_weather',
    python_callable=transform_weather_data,
    dag=dag,
)

# Task 4: Load data
load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_weather_data,
    dag=dag,
)

# Define task dependencies (workflow order)
check_api_availability >> extract_task >> transform_task >> load_task