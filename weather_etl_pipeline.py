import json
import os
import csv
from datetime import timedelta, datetime
import requests
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd
import logging
import shutil
import time

# Airflow specific imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable, TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.email import EmailOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.sql import BranchSQLOperator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_etl')

# --- Configuration Constants ---
# Configuration can be moved to a separate config file or Airflow Variables for better modularity

# City for weather data (can be expanded to multiple cities)
CITIES = ["London", "New York", "Tokyo", "Colombo", "Sydney", "Paris"]

# Define storage methods: "S3", "LOCAL", or "CSV"
STORAGE_METHODS = ["CSV", "LOCAL", "S3"]  # Enable all storage methods

# Define connection IDs used in Airflow UI (Admin -> Connections)
OPENWEATHERMAP_CONN_ID = "openweathermap_default"
AWS_CONN_ID = "aws_default"

# S3 Configuration
S3_BUCKET_NAME = "your-unique-airflow-weather-bucket"  # Replace with your bucket name
S3_KEY_PREFIX = "weather_data"

# Local Filesystem Configuration
BASE_DIR = "/tmp/weather_etl"
LOCAL_DIR_PATH = f"{BASE_DIR}/json_data"
CSV_DIR_PATH = f"{BASE_DIR}/csv_data"
ARCHIVE_DIR_PATH = f"{BASE_DIR}/archive"
LOG_DIR_PATH = f"{BASE_DIR}/logs"

# Backfill settings
DEFAULT_BACKFILL_DAYS = 5

# API Configurations
API_VERSION = "2.5"
API_ENDPOINT = f"/data/{API_VERSION}/weather"
API_FORECAST_ENDPOINT = f"/data/{API_VERSION}/forecast"
API_BASE_URL = "https://api.openweathermap.org"

# Error thresholds
MAX_CONSECUTIVE_ERRORS = 3
ERROR_COUNT_RESET_HOURS = 24

# Performance monitoring
START_TIME_XCOM_KEY = "etl_start_time"

# Email notification settings
NOTIFICATION_EMAIL = "admin@example.com"  # Replace with your email
ALERT_EMAIL_SUBJECT = "Weather ETL Pipeline Alert"

# Sensor configurations
SENSOR_TIMEOUT = 60 * 5  # 5 minutes timeout for sensors
SENSOR_POKE_INTERVAL = 30  # Check every 30 seconds
S3_SENSOR_POKE_INTERVAL = 60  # Check S3 less frequently to reduce costs
LOCAL_SENSOR_POKE_INTERVAL = 15  # Check local files more frequently
SENSOR_MODE = "poke"  # Use "poke" or "reschedule" mode

# --- Helper Functions ---

def get_api_key() -> str:
    """
    Retrieves the OpenWeatherMap API key securely from Airflow Variables.
    
    Returns:
        str: The API key
        
    Raises:
        AirflowFailException: If API key is not found or there's an error
    """
    try:
        api_key = Variable.get("openweathermap_api_key", default_var=None)
        if not api_key:
            raise AirflowFailException("OpenWeatherMap API key not found in Airflow Variables.")
        return api_key
    except Exception as e:
        logger.error(f"Error retrieving API key: {str(e)}")
        raise AirflowFailException(f"Error retrieving API key: {str(e)}")

def get_formatted_date(execution_date, format_str='%Y%m%d'):
    """
    Format execution date for filenames and paths.
    
    Args:
        execution_date: The execution date from Airflow context
        format_str: String format pattern
        
    Returns:
        str: Formatted date string
    """
    return execution_date.strftime(format_str)

def get_formatted_time(execution_date, format_str='%Y%m%d%H%M%S'):
    """
    Format execution date and time for filenames and paths.
    
    Args:
        execution_date: The execution date from Airflow context
        format_str: String format pattern
        
    Returns:
        str: Formatted datetime string
    """
    return execution_date.strftime(format_str)

def ensure_directory_exists(directory_path):
    """
    Creates a directory if it doesn't exist.
    
    Args:
        directory_path: Path to the directory
    """
    os.makedirs(directory_path, exist_ok=True)

def initialize_directories():
    """Initialize all required directories for the ETL pipeline."""
    for directory in [LOCAL_DIR_PATH, CSV_DIR_PATH, ARCHIVE_DIR_PATH, LOG_DIR_PATH]:
        ensure_directory_exists(directory)
    
    # Create a log file if it doesn't exist
    log_file = os.path.join(LOG_DIR_PATH, "etl_execution.log")
    if not os.path.exists(log_file):
        with open(log_file, 'w') as f:
            f.write(f"# Weather ETL Log File - Created on {datetime.now().isoformat()}\n")
    
    logger.info("All required directories initialized")

def log_etl_event(message, city=None, status="INFO", **context):
    """Log ETL event to a file and Airflow logs."""
    timestamp = datetime.now().isoformat()
    city_prefix = f"[{city}] " if city else ""
    log_entry = f"{timestamp} - {status} - {city_prefix}{message}"
    
    # Log to Airflow
    if status == "ERROR":
        logger.error(log_entry)
    elif status == "WARNING":
        logger.warning(log_entry)
    else:
        logger.info(log_entry)
    
    # Log to file
    try:
        log_file = os.path.join(LOG_DIR_PATH, "etl_execution.log")
        with open(log_file, 'a') as f:
            f.write(f"{log_entry}\n")
    except Exception as e:
        logger.error(f"Failed to write to log file: {str(e)}")

def get_error_count(city_name, **context) -> int:
    """Get the count of consecutive errors for a city."""
    try:
        error_count = Variable.get(f"error_count_{city_name}", default_var="0")
        return int(error_count)
    except:
        return 0

def increment_error_count(city_name, **context):
    """Increment the error count for a city."""
    try:
        error_count = get_error_count(city_name, **context)
        error_count += 1
        Variable.set(f"error_count_{city_name}", error_count)
        log_etl_event(f"Error count incremented to {error_count}", city_name, "WARNING")
        return error_count
    except Exception as e:
        logger.error(f"Failed to increment error count for {city_name}: {str(e)}")
        return 0

def reset_error_count(city_name, **context):
    """Reset the error count for a city."""
    try:
        Variable.set(f"error_count_{city_name}", 0)
        log_etl_event(f"Error count reset to 0", city_name, "INFO")
    except Exception as e:
        logger.error(f"Failed to reset error count for {city_name}: {str(e)}")

def check_error_threshold(city_name, **context) -> bool:
    """Check if error threshold has been reached for a city."""
    error_count = get_error_count(city_name, **context)
    if error_count >= MAX_CONSECUTIVE_ERRORS:
        log_etl_event(
            f"Error threshold reached ({error_count}/{MAX_CONSECUTIVE_ERRORS}). Skipping this city until reset.",
            city_name, 
            "ERROR"
        )
        return True
    return False

def record_start_time(**context):
    """Record the start time of the pipeline for performance monitoring."""
    ti = context['ti']
    start_time = time.time()
    ti.xcom_push(key=START_TIME_XCOM_KEY, value=start_time)
    log_etl_event(f"ETL pipeline started at {datetime.fromtimestamp(start_time).isoformat()}")

def calculate_performance_metrics(**context):
    """Calculate and log performance metrics for the pipeline."""
    try:
        ti = context['ti']
        start_time = ti.xcom_pull(key=START_TIME_XCOM_KEY)
        
        if start_time:
            end_time = time.time()
            duration = end_time - start_time
            
            log_etl_event(f"ETL pipeline completed in {duration:.2f} seconds")
            
            # Save metrics for historical comparison
            try:
                metrics_file = os.path.join(LOG_DIR_PATH, "performance_metrics.csv")
                current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Create header if file doesn't exist
                if not os.path.exists(metrics_file):
                    with open(metrics_file, 'w', newline='') as f:
                        writer = csv.writer(f)
                        writer.writerow(['timestamp', 'duration_seconds', 'cities_processed'])
                
                # Append metrics
                with open(metrics_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([current_date, f"{duration:.2f}", len(CITIES)])
                    
            except Exception as e:
                logger.error(f"Failed to save performance metrics: {str(e)}")
        else:
            log_etl_event("Unable to calculate performance metrics - no start time recorded", status="WARNING")
    except Exception as e:
        logger.error(f"Error calculating performance metrics: {str(e)}")

def check_s3_connectivity(**context):
    """Check connectivity to S3 before beginning pipeline execution."""
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        # List buckets to check connectivity
        s3_hook.get_conn().list_buckets()
        
        # Check if our specific bucket exists
        buckets = s3_hook.list_buckets()
        if S3_BUCKET_NAME not in [bucket['Name'] for bucket in buckets]:
            log_etl_event(f"Warning: Bucket {S3_BUCKET_NAME} does not exist. Will attempt to create it.", status="WARNING")
            try:
                s3_hook.create_bucket(bucket_name=S3_BUCKET_NAME)
                log_etl_event(f"Successfully created bucket {S3_BUCKET_NAME}")
            except Exception as bucket_error:
                log_etl_event(f"Failed to create bucket: {str(bucket_error)}", status="ERROR")
                # Continue without failing, will try to use it later
        
        log_etl_event("S3 connectivity check passed")
        return True
    except Exception as e:
        log_etl_event(f"S3 connectivity check failed: {str(e)}", status="ERROR")
        # Don't fail the entire pipeline, just log the error
        # Tasks that depend on S3 will handle their own errors
        return False

def check_api_connectivity(**context):
    """Check connectivity to OpenWeatherMap API before beginning pipeline execution."""
    api_key = get_api_key()
    
    # Using a less resource-intensive endpoint for testing
    test_city = "London"  # Using a reliable city for test
    params = {
        'q': test_city,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        response = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", params=params, timeout=10)
        response.raise_for_status()
        
        # Validate response structure
        data = response.json()
        if not data.get('name') or not data.get('main'):
            log_etl_event("API connectivity check failed: Invalid response structure", status="ERROR")
            return False
            
        log_etl_event("API connectivity check passed")
        return True
    except requests.exceptions.RequestException as e:
        log_etl_event(f"API connectivity check failed: {str(e)}", status="ERROR")
        return False
    except json.JSONDecodeError as e:
        log_etl_event(f"API connectivity check failed: Invalid JSON response: {str(e)}", status="ERROR")
        return False
    except Exception as e:
        log_etl_event(f"API connectivity check failed: Unexpected error: {str(e)}", status="ERROR")
        return False

def send_error_notification(city_name=None, error_message=None, **context):
    """Send notification for pipeline failures."""
    execution_date = context.get('execution_date', datetime.now())
    date_str = get_formatted_date(execution_date)
    
    subject = f"{ALERT_EMAIL_SUBJECT} - {date_str}"
    if city_name:
        subject += f" - {city_name}"
    
    body = f"""
    Weather ETL Pipeline Alert
    
    Date: {date_str}
    Time: {datetime.now().strftime('%H:%M:%S')}
    
    """
    
    if city_name:
        body += f"City: {city_name}\n\n"
    
    if error_message:
        body += f"Error: {error_message}\n\n"
    
    body += "Please check the Airflow logs for more details."
    
    # Just log the notification since we're not actually sending emails in this example
    log_etl_event(f"Would send notification email: {subject}", city_name if city_name else None, "WARNING")
    log_etl_event(f"Email body: {body}", city_name if city_name else None, "WARNING")
    
    # In production, you would uncomment this to actually send the email
    # email_op = EmailOperator(
    #     task_id='send_error_email',
    #     to=NOTIFICATION_EMAIL,
    #     subject=subject,
    #     html_content=body,
    #     dag=context['dag']
    # )
    # email_op.execute(context=context)

# --- ETL Functions ---

def extract_weather_data(city_name, **context) -> Dict[str, Any]:
    """
    Extract current weather data from OpenWeatherMap API for a specific city.
    
    Args:
        city_name: Name of the city to get weather data for
        context: Airflow context dictionary
        
    Returns:
        Dict: Raw weather data
        
    Raises:
        AirflowFailException: If API request fails or returns invalid data
    """
    log_etl_event(f"Starting weather data extraction", city_name)
    api_key = get_api_key()
    ti = context['ti']
    
    # Check if error threshold has been reached
    if check_error_threshold(city_name, **context):
        raise AirflowSkipException(f"Skipping {city_name} due to too many consecutive errors")
    
    # Build API URL with query parameters
    params = {
        'q': city_name,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        response = requests.get(f"{API_BASE_URL}{API_ENDPOINT}", params=params, timeout=15)
        
        # Check if city was not found (404) and skip rather than fail
        if response.status_code == 404:
            log_etl_event(f"City not found in OpenWeatherMap API", city_name, "ERROR")
            increment_error_count(city_name, **context)
            return None
            
        response.raise_for_status()
        data = response.json()
        
        # Validate essential data is present
        if not data.get('name') or not data.get('main'):
            error_msg = f"API returned incomplete data structure"
            log_etl_event(error_msg, city_name, "ERROR")
            increment_error_count(city_name, **context)
            raise AirflowFailException(error_msg)
            
        log_etl_event(
            f"Successfully extracted weather data - temperature: {data.get('main', {}).get('temp')}°C", 
            city_name
        )

        # Reset error count on successful extraction
        reset_error_count(city_name, **context)

        # Push data to XComs for the next task with a city-specific key
        ti.xcom_push(key=f'weather_raw_data_{city_name}', value=data)
        return data
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        
        if "404" in str(e):
            raise AirflowSkipException(f"City '{city_name}' not found. Skipping this city.")
        raise AirflowFailException(error_msg)
        
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse API response as JSON: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg)
        
    except Exception as e:
        error_msg = f"Unexpected error during extraction: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg)


def transform_weather_data(city_name, **context) -> Optional[Dict[str, Any]]:
    """
    Transform the raw weather data into a flattened format.
    
    Args:
        city_name: Name of the city
        context: Airflow context dictionary
        
    Returns:
        Dict: Transformed weather data or None if no data
        
    Raises:
        AirflowSkipException: If no raw data found
        AirflowFailException: If transformation fails
    """
    log_etl_event(f"Starting data transformation", city_name)
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids=f'extract_weather_{city_name}', key=f'weather_raw_data_{city_name}')

    if not raw_data:
        log_etl_event(f"No raw data found. Skipping transformation.", city_name, "WARNING")
        return None

    try:
        # Get weather items safely with better error handling
        weather_items = raw_data.get("weather", [])
        weather_item = weather_items[0] if weather_items else {}
        
        # Extract timestamp for more accurate recording
        execution_timestamp = context.get('ts')
        
        # Get the timezone offset in hours for display
        timezone_offset_sec = raw_data.get("timezone", 0)
        timezone_offset_hrs = timezone_offset_sec / 3600
        
        transformed_data = {
            # Location information
            "city": raw_data.get("name"),
            "country": raw_data.get("sys", {}).get("country"),
            "longitude": raw_data.get("coord", {}).get("lon"),
            "latitude": raw_data.get("coord", {}).get("lat"),
            
            # Temperature data
            "temperature_celsius": raw_data.get("main", {}).get("temp"),
            "feels_like_celsius": raw_data.get("main", {}).get("feels_like"),
            "temp_min_celsius": raw_data.get("main", {}).get("temp_min"),
            "temp_max_celsius": raw_data.get("main", {}).get("temp_max"),
            
            # Atmospheric conditions
            "pressure_hpa": raw_data.get("main", {}).get("pressure"),
            "humidity_percent": raw_data.get("main", {}).get("humidity"),
            "visibility_meters": raw_data.get("visibility"),
            
            # Wind information
            "wind_speed_mps": raw_data.get("wind", {}).get("speed"),
            "wind_direction_deg": raw_data.get("wind", {}).get("deg"),
            "wind_gust_mps": raw_data.get("wind", {}).get("gust"),
            
            # Cloud coverage
            "cloudiness_percent": raw_data.get("clouds", {}).get("all"),
            
            # Precipitation (if available)
            "rain_1h_mm": raw_data.get("rain", {}).get("1h") if raw_data.get("rain") else None,
            "rain_3h_mm": raw_data.get("rain", {}).get("3h") if raw_data.get("rain") else None,
            "snow_1h_mm": raw_data.get("snow", {}).get("1h") if raw_data.get("snow") else None,
            "snow_3h_mm": raw_data.get("snow", {}).get("3h") if raw_data.get("snow") else None,
            
            # Weather description
            "weather_id": weather_item.get("id"),
            "weather_main": weather_item.get("main"),
            "weather_description": weather_item.get("description"),
            "weather_icon": weather_item.get("icon"),
            
            # Time information
            "sunrise_unix_utc": raw_data.get("sys", {}).get("sunrise"),
            "sunset_unix_utc": raw_data.get("sys", {}).get("sunset"),
            "timezone_offset_sec": timezone_offset_sec,
            "timezone_offset_hrs": timezone_offset_hrs,
            "datetime_unix_utc": raw_data.get("dt"),
            
            # Metadata
            "data_fetch_time_utc": execution_timestamp,
            "api_version": API_VERSION
        }
        
        # Add additional calculated fields
        if transformed_data["datetime_unix_utc"]:
            dt_obj = datetime.fromtimestamp(transformed_data["datetime_unix_utc"])
            transformed_data["date_iso"] = dt_obj.date().isoformat()
            transformed_data["time_iso"] = dt_obj.time().isoformat()
            
        # Log a preview of the transformed data
        log_etl_event(
            f"Transformed data - temp: {transformed_data['temperature_celsius']}°C, " +
            f"conditions: {transformed_data['weather_main']}", 
            city_name
        )

        # Push transformed data to XComs with city-specific key
        ti.xcom_push(key=f'weather_transformed_data_{city_name}', value=transformed_data)
        return transformed_data
        
    except Exception as e:
        error_msg = f"Error during transformation: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        raise AirflowFailException(error_msg)


def load_weather_data_to_json(city_name, storage_type, **context) -> bool:
    """
    Load the transformed data to JSON format in either S3 or local filesystem.
    
    Args:
        city_name: Name of the city
        storage_type: "S3" or "LOCAL"
        context: Airflow context dictionary
        
    Returns:
        bool: Success flag
        
    Raises:
        AirflowSkipException: If no transformed data found
        AirflowFailException: If loading fails
    """
    log_etl_event(f"Starting JSON data loading using {storage_type}", city_name)
    ti = context['ti']
    data_to_load = ti.xcom_pull(task_ids=f'transform_weather_{city_name}', key=f'weather_transformed_data_{city_name}')
    execution_date = context['execution_date']

    if not data_to_load:
        log_etl_event(f"No transformed data found. Skipping JSON load.", city_name, "WARNING")
        return False

    # Generate filenames with date/time components
    date_str = get_formatted_date(execution_date)
    timestamp_str = get_formatted_time(execution_date)
    filename = f"weather_{city_name.lower()}_{timestamp_str}.json"

    try:
        # Convert to JSON string with pretty formatting
        data_string = json.dumps(data_to_load, indent=4)

        if storage_type == "LOCAL":
            # Ensure directory exists
            daily_dir = os.path.join(LOCAL_DIR_PATH, date_str)
            ensure_directory_exists(daily_dir)
            
            file_path = os.path.join(daily_dir, filename)
            with open(file_path, 'w') as f:
                f.write(data_string)
            
            # Push file path to XCom for sensors to use
            ti.xcom_push(key=f'local_json_path_{city_name}', value=file_path)
            
            log_etl_event(f"Data loaded successfully to local file: {file_path}", city_name)
            return True

        elif storage_type == "S3":
            s3_key = f"{S3_KEY_PREFIX}/{date_str}/{filename}"
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            s3_hook.load_string(
                string_data=data_string,
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                replace=True
            )
            
            # Push S3 path to XCom for sensors to use
            s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
            ti.xcom_push(key=f's3_json_path_{city_name}', value=s3_path)
            ti.xcom_push(key=f's3_json_key_{city_name}', value=s3_key)
            
            log_etl_event(f"Data loaded successfully to S3: {s3_path}", city_name)
            return True
        else:
            log_etl_event(f"Skipping JSON storage for unsupported method: {storage_type}", city_name, "WARNING")
            return False

    except Exception as e:
        error_msg = f"Error during JSON loading: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        raise AirflowFailException(error_msg)


def load_weather_data_to_csv(city_name, **context) -> bool:
    """
    Load the transformed data to CSV, appending to existing CSV if it exists.
    This implements the requested functionality to have one CSV per city.
    
    Args:
        city_name: Name of the city
        context: Airflow context dictionary
        
    Returns:
        bool: Success flag
        
    Raises:
        AirflowSkipException: If no transformed data found
        AirflowFailException: If loading fails
    """
    log_etl_event(f"Starting CSV data loading", city_name)
    ti = context['ti']
    data_to_load = ti.xcom_pull(task_ids=f'transform_weather_{city_name}', key=f'weather_transformed_data_{city_name}')

    if not data_to_load:
        log_etl_event(f"No transformed data found. Skipping CSV load.", city_name, "WARNING")
        return False

    try:
        # Ensure CSV directory exists
        ensure_directory_exists(CSV_DIR_PATH)
        
        # Create a filename based on city name (lowercase for consistency)
        csv_filename = f"{city_name.lower()}_weather_data.csv"
        csv_path = os.path.join(CSV_DIR_PATH, csv_filename)
        
        # Convert the dictionary to a DataFrame with a single row
        df_new_data = pd.DataFrame([data_to_load])
        
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.isfile(csv_path)
        
        if file_exists:
            # Check if we need to verify schema consistency
            try:
                existing_df = pd.read_csv(csv_path, nrows=1)
                # Check for any columns that exist in new data but not in existing file
                missing_columns = set(df_new_data.columns) - set(existing_df.columns)
                
                if missing_columns:
                    # Create backup of existing file before modifying
                    backup_path = os.path.join(ARCHIVE_DIR_PATH, f"{csv_filename}.bak_{int(time.time())}")
                    shutil.copy2(csv_path, backup_path)
                    log_etl_event(f"Schema change detected. Backup created at {backup_path}", city_name, "WARNING")
                    
                    # Read the entire existing file, add new columns, and rewrite
                    full_existing_df = pd.read_csv(csv_path)
                    for col in missing_columns:
                        full_existing_df[col] = None
                    
                    # Write combined data
                    combined_df = pd.concat([full_existing_df, df_new_data], ignore_index=True)
                    combined_df.to_csv(csv_path, index=False)
                    log_etl_event(f"Updated CSV with new schema: added {len(missing_columns)} columns", city_name)
                else:
                    # Append without headers if schema matches
                    df_new_data.to_csv(csv_path, mode='a', header=False, index=False)
                    log_etl_event(f"Appended new data row to existing CSV", city_name)
            except Exception as e:
                # If there's an error checking schema, fall back to simple append
                log_etl_event(f"Error checking CSV schema: {str(e)}. Falling back to simple append.", city_name, "WARNING")
                df_new_data.to_csv(csv_path, mode='a', header=False, index=False)
        else:
            # Create new file with headers
            df_new_data.to_csv(csv_path, index=False)
            log_etl_event(f"Created new CSV file with headers", city_name)
        
        # Push CSV path to XCom for later tasks
        ti.xcom_push(key=f'csv_path_{city_name}', value=csv_path)
        
        log_etl_event(f"Data loaded successfully to CSV: {csv_path}", city_name)
        return True
        
    except Exception as e:
        error_msg = f"Error during CSV loading: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        raise AirflowFailException(error_msg)


def verify_data_load(city_name, **context) -> bool:
    """
    Verify that the data was successfully loaded by checking if the files exist.
    
    Args:
        city_name: Name of the city
        context: Airflow context dictionary
        
    Returns:
        bool: Verification result
    """
    log_etl_event(f"Verifying data load", city_name)
    ti = context['ti']
    
    verification_results = []
    error_messages = []
    
    # Check JSON file in local storage
    if "LOCAL" in STORAGE_METHODS:
        local_path = ti.xcom_pull(key=f'local_json_path_{city_name}')
        if local_path and os.path.exists(local_path):
            log_etl_event(f"Local JSON file verified at {local_path}", city_name)
            verification_results.append(True)
        else:
            error_msg = f"Local JSON file not found at expected path"
            log_etl_event(error_msg, city_name, "ERROR")
            error_messages.append(error_msg)
            verification_results.append(False)
    
    # Check CSV file
    if "CSV" in STORAGE_METHODS:
        csv_path = ti.xcom_pull(key=f'csv_path_{city_name}')
        if csv_path and os.path.exists(csv_path):
            # Also verify the file has valid content
            try:
                file_size = os.path.getsize(csv_path)
                if file_size > 0:
                    # Read the last line to verify it contains our new data
                    with open(csv_path, 'r') as f:
                        lines = f.readlines()
                        if len(lines) > 1:  # At least header + one data row
                            log_etl_event(f"CSV file verified at {csv_path}", city_name)
                            verification_results.append(True)
                        else:
                            error_msg = f"CSV file exists but does not contain expected data"
                            log_etl_event(error_msg, city_name, "WARNING")
                            error_messages.append(error_msg)
                            verification_results.append(False)
                else:
                    error_msg = f"CSV file exists but is empty"
                    log_etl_event(error_msg, city_name, "ERROR")
                    error_messages.append(error_msg)
                    verification_results.append(False)
            except Exception as e:
                error_msg = f"Error validating CSV file: {str(e)}"
                log_etl_event(error_msg, city_name, "ERROR")
                error_messages.append(error_msg)
                verification_results.append(False)
        else:
            error_msg = f"CSV file not found at expected path"
            log_etl_event(error_msg, city_name, "ERROR")
            error_messages.append(error_msg)
            verification_results.append(False)
    
    # Check S3 file (using the S3Hook to verify)
    if "S3" in STORAGE_METHODS:
        s3_key = ti.xcom_pull(key=f's3_json_key_{city_name}')
        if s3_key:
            try:
                s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                s3_object_exists = s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME)
                
                if s3_object_exists:
                    # Also verify object has content
                    obj = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET_NAME)
                    if obj.content_length > 0:
                        log_etl_event(f"S3 object verified at s3://{S3_BUCKET_NAME}/{s3_key}", city_name)
                        verification_results.append(True)
                    else:
                        error_msg = f"S3 object exists but is empty"
                        log_etl_event(error_msg, city_name, "ERROR")
                        error_messages.append(error_msg)
                        verification_results.append(False)
                else:
                    error_msg = f"S3 object not found at expected key"
                    log_etl_event(error_msg, city_name, "ERROR")
                    error_messages.append(error_msg)
                    verification_results.append(False)
            except Exception as e:
                error_msg = f"Error verifying S3 object: {str(e)}"
                log_etl_event(error_msg, city_name, "ERROR")
                error_messages.append(error_msg)
                verification_results.append(False)
        else:
            error_msg = f"No S3 key found for verification"
            log_etl_event(error_msg, city_name, "WARNING")
            error_messages.append(error_msg)
            verification_results.append(False)
    
    # Evaluate overall verification
    all_verified = all(verification_results) if verification_results else False
    
    if all_verified:
        log_etl_event(f"All data storage methods successfully verified", city_name)
        return True
    else:
        # Format all error messages
        error_summary = "; ".join(error_messages) if error_messages else "No storage methods verified"
        log_etl_event(f"Data verification failed: {error_summary}", city_name, "ERROR")
        
        # Don't fail the pipeline, but send notification
        send_error_notification(city_name, f"Data verification failed: {error_summary}", **context)
        return False


def cleanup_old_files(**context):
    """
    Clean up old data files to prevent disk space issues.
    Keeps a configurable number of days of data.
    
    Args:
        context: Airflow context dictionary
    """
    # Keep this many days of data (including today)
    days_to_keep = 7
    log_etl_event(f"Starting cleanup of files older than {days_to_keep} days")
    
    try:
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime('%Y%m%d')
        
        # Clean local JSON files
        if os.path.exists(LOCAL_DIR_PATH):
            for date_dir in os.listdir(LOCAL_DIR_PATH):
                # Check if the folder name is a date format and before cutoff
                if date_dir.isdigit() and date_dir < cutoff_str:
                    dir_path = os.path.join(LOCAL_DIR_PATH, date_dir)
                    if os.path.isdir(dir_path):
                        # Move to archive instead of delete
                        archive_path = os.path.join(ARCHIVE_DIR_PATH, date_dir)
                        ensure_directory_exists(archive_path)
                        
                        # Copy all files to archive
                        for file in os.listdir(dir_path):
                            src_file = os.path.join(dir_path, file)
                            dst_file = os.path.join(archive_path, file)
                            shutil.copy2(src_file, dst_file)
                        
                        # Remove original directory
                        shutil.rmtree(dir_path)
                        log_etl_event(f"Archived old data directory: {date_dir}")
        
        # For S3, use the S3 hook to list and delete old objects
        if "S3" in STORAGE_METHODS:
            try:
                s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                
                # List objects with the prefix
                keys = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=f"{S3_KEY_PREFIX}/")
                
                # Filter keys by date pattern
                keys_to_delete = []
                for key in keys:
                    # Extract date part from key, assuming format like "weather_data/20220101/"
                    parts = key.split('/')
                    if len(parts) >= 2 and parts[1].isdigit() and parts[1] < cutoff_str:
                        keys_to_delete.append(key)
                
                if keys_to_delete:
                    # Delete old objects
                    s3_hook.delete_objects(bucket=S3_BUCKET_NAME, keys=keys_to_delete)
                    log_etl_event(f"Deleted {len(keys_to_delete)} old S3 objects")
            except Exception as e:
                log_etl_event(f"Error cleaning up S3 objects: {str(e)}", status="ERROR")
        
        log_etl_event(f"Cleanup completed successfully")
        return True
        
    except Exception as e:
        log_etl_event(f"Error during cleanup: {str(e)}", status="ERROR")
        # Don't fail the pipeline for cleanup errors
        return False


def create_pipeline_summary(**context):
    """
    Create a summary of the pipeline execution to track performance and success rate.
    
    Args:
        context: Airflow context dictionary
    """
    log_etl_event("Creating pipeline execution summary")
    ti = context['ti']
    execution_date = context['execution_date']
    
    try:
        # Initialize counters
        city_count = len(CITIES)
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        # Check status for each city
        city_statuses = {}
        for city in CITIES:
            try:
                # Try to get verification result
                verified = ti.xcom_pull(task_ids=f'verify_data_{city}')
                if verified is True:
                    city_statuses[city] = "SUCCESS"
                    success_count += 1
                elif verified is False:
                    city_statuses[city] = "VERIFICATION_FAILED"
                    failed_count += 1
                else:
                    # If no verification result, check if transformation completed
                    transformed_data = ti.xcom_pull(task_ids=f'transform_weather_{city}', 
                                                key=f'weather_transformed_data_{city}')
                    if transformed_data:
                        city_statuses[city] = "PROCESSED_NOT_VERIFIED"
                        success_count += 1  # Count as success if transformed
                    else:
                        city_statuses[city] = "FAILED"
                        failed_count += 1
            except:
                # If we can't get status, assume it was skipped or failed
                city_statuses[city] = "UNKNOWN"
                skipped_count += 1
        
        # Calculate success rate
        success_rate = (success_count / city_count) * 100 if city_count > 0 else 0
        
        # Create summary
        summary = {
            "execution_date": execution_date.isoformat(),
            "total_cities": city_count,
            "successful_cities": success_count,
            "failed_cities": failed_count,
            "skipped_cities": skipped_count,
            "success_rate": f"{success_rate:.2f}%",
            "city_statuses": city_statuses,
            "storage_methods_used": STORAGE_METHODS
        }
        
        # Calculate execution time if available
        start_time = ti.xcom_pull(key=START_TIME_XCOM_KEY)
        if start_time:
            end_time = time.time()
            execution_time_sec = end_time - start_time
            summary["execution_time_seconds"] = f"{execution_time_sec:.2f}"
        
        # Log summary to both Airflow logs and summary file
        log_etl_event(f"Pipeline summary: {json.dumps(summary, indent=2)}")
        
        # Save summary to file
        summary_file = os.path.join(LOG_DIR_PATH, "pipeline_summaries.jsonl")
        with open(summary_file, 'a') as f:
            f.write(json.dumps(summary) + "\n")
        
        # Also create a CSV summary for easier analysis
        summary_csv = os.path.join(LOG_DIR_PATH, "pipeline_summaries.csv")
        csv_exists = os.path.exists(summary_csv)
        
        with open(summary_csv, 'a', newline='') as f:
            writer = csv.writer(f)
            
            # Write header if file doesn't exist
            if not csv_exists:
                writer.writerow([
                    "execution_date", "total_cities", "successful_cities", 
                    "failed_cities", "skipped_cities", "success_rate", 
                    "execution_time_seconds"
                ])
            
            # Write summary row
            writer.writerow([
                execution_date.isoformat(),
                city_count,
                success_count,
                failed_count,
                skipped_count,
                f"{success_rate:.2f}%",
                summary.get("execution_time_seconds", "N/A")
            ])
        
        return summary
        
    except Exception as e:
        log_etl_event(f"Error creating pipeline summary: {str(e)}", status="ERROR")
        return {
            "execution_date": execution_date.isoformat(),
            "status": "ERROR",
            "error": str(e)
        }


# --- Sensor Functions ---

def check_local_file_exists(city_name, **context):
    """
    Check if local JSON file exists for a city.
    This is a function that can be used by FileSensor.
    
    Args:
        city_name: Name of the city
        context: Airflow context
        
    Returns:
        str: File path if exists, None otherwise
    """
    ti = context['ti']
    file_path = ti.xcom_pull(key=f'local_json_path_{city_name}')
    
    if file_path and os.path.exists(file_path):
        log_etl_event(f"Local file detected at: {file_path}", city_name)
        return file_path
    
    return None


def check_csv_file_exists(city_name, **context):
    """
    Check if CSV file exists for a city.
    This is a function that can be used by FileSensor.
    
    Args:
        city_name: Name of the city
        context: Airflow context
        
    Returns:
        str: File path if exists, None otherwise
    """
    ti = context['ti']
    csv_path = ti.xcom_pull(key=f'csv_path_{city_name}')
    
    if csv_path and os.path.exists(csv_path):
        log_etl_event(f"CSV file detected at: {csv_path}", city_name)
        return csv_path
    
    return None


def check_s3_file_exists(city_name, **context):
    """
    Check if S3 file exists for a city.
    This is a function that can be used by S3KeySensor.
    
    Args:
        city_name: Name of the city
        context: Airflow context
        
    Returns:
        str: S3 key if exists, None otherwise
    """
    ti = context['ti']
    s3_key = ti.xcom_pull(key=f's3_json_key_{city_name}')
    
    if s3_key:
        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
                log_etl_event(f"S3 object detected at: s3://{S3_BUCKET_NAME}/{s3_key}", city_name)
                return s3_key
        except Exception as e:
            log_etl_event(f"Error checking S3 key: {str(e)}", city_name, "ERROR")
    
    return None


def check_api_connectivity_sensor(**context):
    """
    Sensor function to check API connectivity.
    
    Args:
        context: Airflow context
        
    Returns:
        bool: True if connected, False otherwise
    """
    return check_api_connectivity(**context)


# --- Branching Functions ---

def branch_based_on_storage_methods(**context):
    """
    Branch the DAG based on configured storage methods.
    
    Args:
        context: Airflow context
        
    Returns:
        list: List of task IDs to execute
    """
    branches = []
    
    # Add tasks for all enabled storage methods
    if "LOCAL" in STORAGE_METHODS:
        branches.append("store_json_local_group")
    
    if "S3" in STORAGE_METHODS:
        branches.append("store_json_s3_group")
    
    if "CSV" in STORAGE_METHODS:
        branches.append("store_csv_group")
    
    # If no storage methods are enabled, go to end
    if not branches:
        branches.append("end_etl")
    
    log_etl_event(f"Branching to storage methods: {branches}")
    return branches


def branch_based_on_errors(city_name, **context):
    """
    Branch the DAG based on error threshold for a city.
    
    Args:
        city_name: Name of the city
        context: Airflow context
        
    Returns:
        str: Task ID to execute
    """
    if check_error_threshold(city_name, **context):
        log_etl_event(f"Branching to skip city due to error threshold", city_name, "WARNING")
        return f"skip_{city_name}"
    else:
        log_etl_event(f"Proceeding with normal extraction flow", city_name)
        return f"extract_weather_{city_name}"


# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [NOTIFICATION_EMAIL],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'start_date': days_ago(1),
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval=timedelta(hours=1),  # Run hourly
    catchup=False,
    tags=['weather', 'etl'],
    max_active_runs=1,
)

# --- Task Groups ---

# Create a task group for each city to improve Airflow UI organization
def create_city_task_group(city):
    """Create tasks for a specific city within a task group."""
    with TaskGroup(group_id=f'city_{city.lower()}_tasks', dag=dag) as city_group:
        # Branch based on error threshold
        branch_task = BranchPythonOperator(
            task_id=f'check_errors_{city}',
            python_callable=branch_based_on_errors,
            provide_context=True,
            op_kwargs={'city_name': city},
            dag=dag,
        )
        
        # Skip task if error threshold reached
        skip_task = DummyOperator(
            task_id=f'skip_{city}',
            dag=dag,
        )
        
        # Extract task
        extract_task = PythonOperator(
            task_id=f'extract_weather_{city}',
            python_callable=extract_weather_data,
            op_kwargs={'city_name': city},
            provide_context=True,
            dag=dag,
        )
        
        # Transform task
        transform_task = PythonOperator(
            task_id=f'transform_weather_{city}',
            python_callable=transform_weather_data,
            op_kwargs={'city_name': city},
            provide_context=True,
            dag=dag,
        )
        
        # Local JSON storage task
        load_json_local_task = PythonOperator(
            task_id=f'load_json_local_{city}',
            python_callable=load_weather_data_to_json,
            op_kwargs={'city_name': city, 'storage_type': 'LOCAL'},
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.NONE_FAILED,  # Run even if some branches are skipped
        )
        
        # S3 JSON storage task
        load_json_s3_task = PythonOperator(
            task_id=f'load_json_s3_{city}',
            python_callable=load_weather_data_to_json,
            op_kwargs={'city_name': city, 'storage_type': 'S3'},
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.NONE_FAILED,  # Run even if some branches are skipped
        )
        
        # CSV storage task
        load_csv_task = PythonOperator(
            task_id=f'load_csv_{city}',
            python_callable=load_weather_data_to_csv,
            op_kwargs={'city_name': city},
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.NONE_FAILED,  # Run even if some branches are skipped
        )
        
        # Verification task
        verify_task = PythonOperator(
            task_id=f'verify_data_{city}',
            python_callable=verify_data_load,
            op_kwargs={'city_name': city},
            provide_context=True,
            dag=dag,
            trigger_rule=TriggerRule.NONE_FAILED,  # Run even if some branches are skipped
        )
        
        # Local file sensor
        local_file_sensor = FileSensor(
            task_id=f'sensor_local_file_{city}',
            filepath="{{ ti.xcom_pull(key='local_json_path_" + city + "') }}",
            fs_conn_id='fs_default',
            mode=SENSOR_MODE,
            poke_interval=LOCAL_SENSOR_POKE_INTERVAL,
            timeout=SENSOR_TIMEOUT,
            soft_fail=True,  # Don't fail the entire DAG if this sensor times out
            dag=dag,
        )
        
        # CSV file sensor
        csv_file_sensor = FileSensor(
            task_id=f'sensor_csv_file_{city}',
            filepath="{{ ti.xcom_pull(key='csv_path_" + city + "') }}",
            fs_conn_id='fs_default',
            mode=SENSOR_MODE,
            poke_interval=LOCAL_SENSOR_POKE_INTERVAL,
            timeout=SENSOR_TIMEOUT,
            soft_fail=True,  # Don't fail the entire DAG if this sensor times out
            dag=dag,
        )
        
        # S3 file sensor
        s3_file_sensor = S3KeySensor(
            task_id=f'sensor_s3_file_{city}',
            bucket_key="{{ ti.xcom_pull(key='s3_json_key_" + city + "') }}",
            bucket_name=S3_BUCKET_NAME,
            aws_conn_id=AWS_CONN_ID,
            mode=SENSOR_MODE,
            poke_interval=S3_SENSOR_POKE_INTERVAL,
            timeout=SENSOR_TIMEOUT,
            soft_fail=True,  # Don't fail the entire DAG if this sensor times out
            dag=dag,
        )
        
        # Define task dependencies
        branch_task >> [skip_task, extract_task]
        extract_task >> transform_task
        
        # Branch to different storage methods
        if "LOCAL" in STORAGE_METHODS:
            transform_task >> load_json_local_task >> local_file_sensor
        
        if "S3" in STORAGE_METHODS:
            transform_task >> load_json_s3_task >> s3_file_sensor
        
        if "CSV" in STORAGE_METHODS:
            transform_task >> load_csv_task >> csv_file_sensor
        
        # Connect all tasks to verification
        verification_dependencies = []
        if "LOCAL" in STORAGE_METHODS:
            verification_dependencies.append(local_file_sensor)
        if "S3" in STORAGE_METHODS:
            verification_dependencies.append(s3_file_sensor)
        if "CSV" in STORAGE_METHODS:
            verification_dependencies.append(csv_file_sensor)
        
        # Only run verification if any storage method is enabled
        if verification_dependencies:
            verification_dependencies >> verify_task
        
        return city_group

# --- Main DAG Tasks ---

# Start task
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Record start time for performance monitoring
record_start = PythonOperator(
    task_id='record_start_time',
    python_callable=record_start_time,
    provide_context=True,
    dag=dag,
)

# Initialize directories
init_dirs = PythonOperator(
    task_id='initialize_directories',
    python_callable=initialize_directories,
    dag=dag,
)

# Check API connectivity using a sensor
api_connectivity_sensor = HttpSensor(
    task_id='check_api_connectivity',
    http_conn_id=OPENWEATHERMAP_CONN_ID,
    endpoint=API_ENDPOINT,
    request_params={
        'q': 'London',  # Use a reliable city for testing
        'appid': "{{ var.value.openweathermap_api_key }}",
        'units': 'metric'
    },
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=300,
    mode=SENSOR_MODE,
    soft_fail=True,  # Don't fail the entire DAG if API is down
    dag=dag,
)

# Check S3 connectivity 
s3_connectivity_task = PythonOperator(
    task_id='check_s3_connectivity',
    python_callable=check_s3_connectivity,
    provide_context=True,
    dag=dag,
)

# Wait for all cities to complete
cities_completed = DummyOperator(
    task_id='all_cities_completed',
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tasks failed
    dag=dag,
)

# Clean up old files
cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    provide_context=True,
    dag=dag,
)

# Generate performance metrics
performance_metrics_task = PythonOperator(
    task_id='calculate_performance_metrics',
    python_callable=calculate_performance_metrics,
    provide_context=True,
    dag=dag,
)

# Create pipeline summary
summary_task = PythonOperator(
    task_id='create_pipeline_summary',
    python_callable=create_pipeline_summary,
    provide_context=True,
    dag=dag,
)

# End task
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ALL_DONE,  # Run even if some tasks failed
    dag=dag,
)

# --- Task Dependencies ---

# Set up the main task flow
start_pipeline >> record_start >> init_dirs >> [api_connectivity_sensor, s3_connectivity_task]

# Create task groups for each city
city_tasks = []
for city in CITIES:
    city_task_group = create_city_task_group(city)
    [api_connectivity_sensor, s3_connectivity_task] >> city_task_group
    city_task_group >> cities_completed

# Final tasks
cities_completed >> cleanup_task >> performance_metrics_task >> summary_task >> end_pipeline

# For local development testing when running this script directly
if __name__ == "__main__":
    dag.clear()
    dag.run()