# Standard Python Libraries
import json                     # For handling JSON data (API responses, saving files)
import os                       # For interacting with the operating system (paths, directories)
import csv                      # For handling CSV data (saving files, metrics)
from datetime import timedelta, datetime # For scheduling, timestamps, date calculations
import requests                 # For making HTTP requests to the OpenWeatherMap API
from typing import Dict, Any, Optional # For type hinting, improving code readability

# Third-Party Libraries
import pandas as pd             # For data manipulation, especially useful for CSV handling
import logging                  # For logging information and errors
import shutil                   # For high-level file operations (moving/copying files/dirs)
import time                     # For timing operations (performance metrics)

# Airflow Core and Operators/Hooks/Utilities
from airflow import DAG         # Core class for defining a DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator # To run Python functions and branch logic
from airflow.providers.http.sensors.http import HttpSensor # To sense HTTP endpoint availability (requires apache-airflow-providers-http)
from airflow.sensors.filesystem import FileSensor           # To sense file existence (requires apache-airflow-providers-filesystem)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # To sense S3 object existence (requires apache-airflow-providers-amazon)
from airflow.models import Variable                         # To securely retrieve variables from Airflow metastore (e.g., API keys)
# from airflow.models import TaskInstance # Optional import, context['ti'] provides the instance implicitly
from airflow.providers.amazon.aws.hooks.s3 import S3Hook   # To interact with AWS S3 (requires apache-airflow-providers-amazon)
from airflow.exceptions import AirflowFailException, AirflowSkipException # To control task states explicitly
from airflow.utils.dates import days_ago                    # Helper function for setting DAG start dates
from airflow.operators.dummy import DummyOperator          # Placeholder tasks (use EmptyOperator in newer Airflow versions if preferred)
from airflow.utils.trigger_rule import TriggerRule         # To control task execution based on upstream status
from airflow.utils.task_group import TaskGroup             # To logically group tasks in the UI
from airflow.operators.email import EmailOperator         # To send email notifications (requires email setup in Airflow)

# ----------------------------------------------------------------------------
# Logging Configuration
# ----------------------------------------------------------------------------
# Basic logging setup, messages will appear in Airflow task logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_etl') # Create a logger specific to this DAG

# ----------------------------------------------------------------------------
# Configuration Constants
# ----------------------------------------------------------------------------
# These parameters control the DAG's behavior. Consider moving sensitive items
# like API keys to Airflow Variables and Connections for better security and management.

# List of cities to fetch weather data for
CITIES = ["London", "New York", "Tokyo", "Colombo", "Sydney", "Paris"]

# Define which storage backends to use. Options: "S3", "LOCAL", "CSV"
# Include the methods you want to activate in this list.
STORAGE_METHODS = ["CSV", "LOCAL", "S3"] # Example: Using all three methods

# --- Airflow Connection IDs ---
# These MUST match the Connection IDs created in the Airflow UI (Admin -> Connections)
# HTTP Connection for OpenWeatherMap API
OPENWEATHERMAP_CONN_ID = "openweathermap_default" # Create HTTP connection with Host: https://api.openweathermap.org
# AWS Connection for S3 access
AWS_CONN_ID = "aws_default"                      # Create AWS connection with your credentials

# --- Airflow Variable Names ---
# This MUST match the Variable key created in the Airflow UI (Admin -> Variables)
OPENWEATHERMAP_API_KEY_VAR = "openweathermap_api_key" # Store your OpenWeatherMap API key here

# --- S3 Configuration ---
# *** IMPORTANT: Replace with your actual S3 bucket name! ***
# The bucket must exist, and the AWS connection needs write permissions.
# The DAG *tries* to create it if missing, but requires s3:CreateBucket permission.
S3_BUCKET_NAME = "your-unique-airflow-weather-bucket"
S3_KEY_PREFIX = "weather_data"                         # Root "folder" in the bucket for weather data

# --- Local Filesystem Configuration ---
# Define paths for storing data/logs locally on the Airflow worker execution environment.
# Ensure the user running the Airflow worker process has write permissions to BASE_DIR.
BASE_DIR = "/tmp/airflow_weather_etl"                  # Base directory for local storage
LOCAL_DIR_PATH = f"{BASE_DIR}/json_data"             # Where raw JSON files will be stored (organized by date)
CSV_DIR_PATH = f"{BASE_DIR}/csv_data"               # Where appended CSV history files will be stored
ARCHIVE_DIR_PATH = f"{BASE_DIR}/archive"           # Where old local files will be moved before deletion
LOG_DIR_PATH = f"{BASE_DIR}/logs"                 # Directory for custom log files (summary, performance)

# --- API Configuration ---
API_VERSION = "2.5"                                    # OpenWeatherMap API version
API_ENDPOINT = f"/data/{API_VERSION}/weather"        # API endpoint for current weather
# API_FORECAST_ENDPOINT = f"/data/{API_VERSION}/forecast" # Example for future use
# Base URL comes from the OPENWEATHERMAP_CONN_ID connection's Host field

# --- ETL Behavior Configuration ---
MAX_CONSECUTIVE_ERRORS = 3      # Number of consecutive errors for a city before skipping it for a run
ERROR_COUNT_RESET_HOURS = 24    # Not currently implemented: Hours after which error count could be reset (useful for transient issues)

# --- Performance & Monitoring ---
START_TIME_XCOM_KEY = "etl_start_time"                 # XCom key to store pipeline start time
NOTIFICATION_EMAIL = "naribana5@gmail.com"               # Recipient for email alerts (if EmailOperator is used)
ALERT_EMAIL_SUBJECT = "Weather ETL Pipeline Alert"     # Subject line for alert emails

# --- Sensor Configuration ---
SENSOR_TIMEOUT = 60 * 5                 # Max time (seconds) a sensor waits for condition before timing out
SENSOR_POKE_INTERVAL = 30               # How often (seconds) a sensor checks the condition
S3_SENSOR_POKE_INTERVAL = 60            # Check S3 less frequently (potentially reduce costs/API calls)
LOCAL_SENSOR_POKE_INTERVAL = 15         # Check local filesystem more frequently
SENSOR_MODE = "poke"                    # Sensor mode ('poke' or 'reschedule') - poke keeps worker slot busy

# ----------------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------------

def get_api_key() -> str:
    """
    Retrieves the OpenWeatherMap API key securely from Airflow Variables.
    Raises AirflowFailException if the variable is not found, failing the task.
    """
    try:
        # Attempt to get the variable value from Airflow metastore
        api_key = Variable.get(OPENWEATHERMAP_API_KEY_VAR)
        if not api_key:
            # Raise specific error if variable exists but is empty
            raise ValueError(f"Airflow Variable '{OPENWEATHERMAP_API_KEY_VAR}' found but is empty.")
        return api_key
    except KeyError:
        # Handle case where the variable doesn't exist at all
        error_msg = f"Airflow Variable '{OPENWEATHERMAP_API_KEY_VAR}' not found. Please create it in the Airflow UI (Admin -> Variables)."
        logger.error(error_msg)
        raise AirflowFailException(error_msg)
    except Exception as e:
        # Catch other potential errors during Variable retrieval
        error_msg = f"Error retrieving Airflow Variable '{OPENWEATHERMAP_API_KEY_VAR}': {str(e)}"
        logger.error(error_msg)
        raise AirflowFailException(error_msg)


def get_formatted_date(execution_date, format_str='%Y%m%d') -> str:
    """Formats the DAG execution date (logical date) into a string."""
    return execution_date.strftime(format_str)

def get_formatted_time(execution_date, format_str='%Y%m%d_%H%M%S') -> str:
    """Formats the DAG execution date and time into a string."""
    return execution_date.strftime(format_str)

def ensure_directory_exists(directory_path: str):
    """Creates a directory if it doesn't exist. Raises OSError on failure."""
    try:
        # exist_ok=True prevents error if directory already exists
        os.makedirs(directory_path, exist_ok=True)
        # logger.debug(f"Ensured directory exists: {directory_path}") # Optional debug log
    except OSError as e:
        logger.error(f"Failed to create directory {directory_path}: {e}")
        # Re-raise exception because directory creation is likely critical for subsequent tasks
        raise

def initialize_directories():
    """Initializes all required local directories at the start of the DAG run."""
    logger.info(f"Initializing local directories under base path: {BASE_DIR}")
    try:
        # Ensure all configured local directories exist
        for directory in [LOCAL_DIR_PATH, CSV_DIR_PATH, ARCHIVE_DIR_PATH, LOG_DIR_PATH]:
            ensure_directory_exists(directory)

        # Optionally, create a general execution log file if it doesn't exist
        log_file = os.path.join(LOG_DIR_PATH, "etl_execution_main.log")
        if not os.path.exists(log_file):
             with open(log_file, 'w') as f:
                 f.write(f"# Weather ETL Main Log File - Created on {datetime.now().isoformat()}\n")
             logger.info(f"Created main execution log file at: {log_file}")

        logger.info("Local directory initialization complete.")
    except OSError as e:
        # If directory creation fails (e.g., permissions), fail the task early
        logger.error(f"Directory initialization failed: {e}")
        raise AirflowFailException(f"Could not initialize local directories: {e}")


def log_etl_event(message: str, city: Optional[str] = None, status: str = "INFO", **context):
    """Custom logging function to add context and potentially write to a central file."""
    timestamp = datetime.now().isoformat() # Use ISO format for easy sorting/reading
    city_prefix = f"[{city}] " if city else "" # Add city context if provided
    log_entry = f"{timestamp} [{status}] {city_prefix}- {message}"

    # Log to the standard Airflow task logger
    if status == "ERROR":
        logger.error(log_entry)
    elif status == "WARNING":
        logger.warning(log_entry)
    else: # INFO or other custom statuses
        logger.info(log_entry)

    # --- Optional: Append to a central log file ---
    # This provides a single file view across all task runs, but can grow large
    # and requires careful file handling (permissions, locking if concurrent writes were possible).
    # Use with caution in distributed environments.
    # try:
    #     log_file = os.path.join(LOG_DIR_PATH, "etl_execution_main.log")
    #     # Basic check if directory exists (might have been removed externally)
    #     if os.path.exists(LOG_DIR_PATH):
    #          with open(log_file, 'a') as f:
    #              f.write(log_entry + "\n")
    #     # else: logger.warning("Log directory missing, skipping central file log.")
    # except Exception as e:
    #     logger.error(f"Failed writing to central log file {log_file}: {e}")
    # --- End Optional Central Log File Append ---


def get_error_count(city_name: str, **context) -> int:
    """Retrieves the consecutive error count for a city from an Airflow Variable."""
    # Variable key convention: prefix + city_name (lowercase)
    var_key = f"weather_etl_error_count_{city_name.lower().replace(' ', '_')}"
    try:
        # Get variable value, return '0' as string if not found
        error_count_str = Variable.get(var_key, default_var="0")
        return int(error_count_str)
    except ValueError:
        # Handle case where Variable exists but isn't a valid integer
        logger.warning(f"Invalid value found for error count Variable '{var_key}'. Resetting to 0.")
        Variable.set(var_key, "0") # Store as string "0"
        return 0
    except Exception as e:
        # Handle DB connection issues or other Variable access errors
        logger.error(f"Could not retrieve Variable '{var_key}': {str(e)}. Assuming 0 errors.")
        return 0 # Return 0 safely


def increment_error_count(city_name: str, **context) -> int:
    """Increments the error count Variable for a specific city."""
    var_key = f"weather_etl_error_count_{city_name.lower().replace(' ', '_')}"
    try:
        error_count = get_error_count(city_name, **context) # Get current count
        error_count += 1
        Variable.set(var_key, str(error_count)) # Store as string
        log_etl_event(f"Consecutive error count incremented to {error_count}", city_name, "WARNING")
        return error_count
    except Exception as e:
        logger.error(f"Failed to set/increment error Variable '{var_key}' for {city_name}: {str(e)}")
        # Return a high value to ensure threshold is likely met if setting fails
        return MAX_CONSECUTIVE_ERRORS + 1


def reset_error_count(city_name: str, **context):
    """Resets the error count Variable for a city to 0."""
    var_key = f"weather_etl_error_count_{city_name.lower().replace(' ', '_')}"
    try:
        # Optimization: Only set the variable if it's not already '0'
        current_count_str = Variable.get(var_key, default_var="0")
        if current_count_str != "0":
            Variable.set(var_key, "0") # Store as string "0"
            log_etl_event(f"Consecutive error count reset to 0", city_name, "INFO")
        # else: logger.debug(f"Error count for {city_name} already 0, no reset needed.")
    except Exception as e:
        logger.error(f"Failed to reset error Variable '{var_key}' for {city_name}: {str(e)}")


def check_error_threshold(city_name: str, **context) -> bool:
    """
    Checks if the consecutive error count for a city meets or exceeds the threshold.
    Returns True if threshold is met (should skip), False otherwise (should proceed).
    This function is primarily used by the `decide_branch_path` callable.
    """
    error_count = get_error_count(city_name, **context)
    threshold_reached = error_count >= MAX_CONSECUTIVE_ERRORS
    if threshold_reached:
        log_etl_event(
            f"Error threshold met ({error_count}/{MAX_CONSECUTIVE_ERRORS}). Will skip {city_name} for this run.",
            city_name,
            "WARNING"
        )
    return threshold_reached


def record_start_time(**context):
    """Pushes the pipeline start timestamp (epoch float) to XComs."""
    ti = context['ti']
    start_time = time.time()
    ti.xcom_push(key=START_TIME_XCOM_KEY, value=start_time)
    log_etl_event(f"Pipeline run start time recorded: {start_time}")


def calculate_performance_metrics(**context):
    """Calculates total DAG run duration and logs/saves it."""
    ti = context['ti']
    run_end_time = time.time() # Capture end time

    try:
        # Pull start time from XComs (pushed by 'record_start_time' task)
        start_time = ti.xcom_pull(key=START_TIME_XCOM_KEY, task_ids='record_start_time')

        if start_time:
            duration = run_end_time - start_time
            duration_str = f"{duration:.2f}" # Format to 2 decimal places
            log_etl_event(f"Pipeline Run Duration: {duration_str} seconds")

            # --- Save metrics to a CSV file for historical tracking ---
            metrics_file = os.path.join(LOG_DIR_PATH, "performance_metrics_history.csv")
            current_timestamp_iso = datetime.now().isoformat()
            dag_run_id = context.get('run_id', 'N/A') # Get DAG run ID from context

            try:
                # Retrieve summary info (if available) to add context
                summary_info = ti.xcom_pull(task_ids='create_pipeline_summary', key='pipeline_summary')
                cities_processed = summary_info.get('cities_processed_successfully', 'N/A') if summary_info else 'N/A'
                cities_verified = summary_info.get('cities_verified_successfully', 'N/A') if summary_info else 'N/A'

                # Define CSV header
                header = ['log_timestamp_iso', 'dag_run_id', 'duration_seconds', 'cities_processed_count', 'cities_verified_count']
                # Prepare data row as a dictionary for clarity
                data_row = {
                    'log_timestamp_iso': current_timestamp_iso,
                    'dag_run_id': dag_run_id,
                    'duration_seconds': duration_str,
                    'cities_processed_count': cities_processed,
                    'cities_verified_count': cities_verified
                }

                # Check if file needs header
                file_exists = os.path.exists(metrics_file)
                with open(metrics_file, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=header)
                    if not file_exists or os.path.getsize(metrics_file) == 0:
                        writer.writeheader() # Write header only if file is new/empty
                    writer.writerow(data_row) # Write the metrics data
                # logger.info(f"Performance metrics appended to {metrics_file}")

            except Exception as e:
                log_etl_event(f"Failed to save performance metrics to {metrics_file}: {str(e)}", status="WARNING")
            # --- End saving metrics to CSV ---

        else:
            # Handle case where start time wasn't found in XComs
            log_etl_event("Could not calculate duration: Start time XCom not found.", status="WARNING")

    except Exception as e:
        # Catch any other unexpected errors during calculation/logging
        log_etl_event(f"Error calculating/logging performance metrics: {str(e)}", status="ERROR")


def check_s3_connectivity(**context):
    """
    Checks connectivity to AWS S3 using the configured connection and verifies
    the target bucket exists, attempting creation if necessary and permitted.
    Logs errors but does not fail the task itself, allowing S3-dependent tasks
    to handle their own specific failures.
    """
    # Skip check entirely if S3 storage isn't enabled for this DAG run
    if "S3" not in STORAGE_METHODS:
        log_etl_event("S3 storage method disabled, skipping S3 connectivity check.", status="INFO")
        return True # Return True to indicate check is 'passed' (not applicable)

    log_etl_event(f"Checking S3 connectivity and bucket '{S3_BUCKET_NAME}' using AWS connection '{AWS_CONN_ID}'...")
    try:
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Use check_for_bucket for a direct existence check
        bucket_exists = s3_hook.check_for_bucket(bucket_name=S3_BUCKET_NAME)

        if bucket_exists:
            log_etl_event(f"S3 bucket '{S3_BUCKET_NAME}' confirmed to exist.", status="INFO")
            return True
        else:
            # Bucket not found, attempt to create it
            log_etl_event(f"S3 bucket '{S3_BUCKET_NAME}' not found. Attempting creation...", status="WARNING")
            try:
                # Note: Creation requires s3:CreateBucket permission in the AWS connection's policy
                # You might need to specify a region depending on your AWS defaults/setup
                # e.g., s3_hook.create_bucket(bucket_name=S3_BUCKET_NAME, region_name='us-east-1')
                s3_hook.create_bucket(bucket_name=S3_BUCKET_NAME)
                log_etl_event(f"Successfully created S3 bucket '{S3_BUCKET_NAME}'.", status="INFO")
                return True # Bucket now exists
            except Exception as creation_error:
                # Log the failure to create the bucket but don't fail the task
                log_etl_event(f"Failed to create S3 bucket '{S3_BUCKET_NAME}': {str(creation_error)}. "
                              "S3 loading tasks may fail if the bucket remains unavailable.", status="ERROR")
                # Return True here to allow DAG progression; downstream tasks will handle S3 failures.
                # Alternatively, return False or raise exception if bucket creation is mandatory.
                return True

    except Exception as e:
        # Catch connection errors (credentials, network) or other hook issues
        log_etl_event(f"S3 connectivity/bucket check failed: {str(e)}. Downstream S3 tasks may fail.", status="ERROR")
        # Return True to allow DAG progression, relying on specific S3 tasks to handle errors.
        return True


def send_error_notification(city_name=None, error_message=None, **context):
    """
    Formats and logs error details for notification purposes.
    Intended placeholder for actual notification logic (e.g., EmailOperator, Slack).
    """
    # Gather context information for the notification
    execution_date = context.get('execution_date') # The logical date/time of the run
    dag_run_id = context.get('run_id', 'N/A')      # The specific run instance ID
    task_id = context.get('task_instance_key_str', 'N/A') # The failed task's identifier
    log_url = context.get('ti', None).log_url if context.get('ti') else 'N/A' # Link to logs if available

    # Format subject and body
    date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S UTC') if execution_date else 'N/A'
    subject = f"{ALERT_EMAIL_SUBJECT} | DAG Run: {dag_run_id}"
    if city_name:
        subject += f" | City: {city_name}"

    body = f"""
    <h2>Weather ETL Pipeline Alert</h2>
    <p><strong>DAG Run ID:</strong> {dag_run_id}</p>
    <p><strong>Logical Execution Date:</strong> {date_str}</p>
    <p><strong>Failed Task:</strong> {task_id}</p>
    """
    if city_name:
        body += f"<p><strong>Affected City:</strong> {city_name}</p>"
    if error_message:
        # Basic escaping for HTML could be added: html.escape(error_message)
        body += f"<p><strong>Error Details:</strong><br><pre>{error_message}</pre></p>"
    body += f'<p>Check Airflow logs for details: <a href="{log_url}">Task Log Link</a></p>'

    # Log the notification content (instead of sending email in this example)
    log_etl_event(f"ERROR NOTIFICATION PREPARED - Subject: {subject}", city_name, "ERROR")
    log_etl_event(f"ERROR NOTIFICATION BODY (Preview): {body[:200]}...", city_name, "ERROR")

    # --- Placeholder for actual notification task ---
    # In a real scenario, you might trigger a downstream EmailOperator/SlackOperator etc.
    # based on the failure of the upstream task, rather than calling it directly here.
    # Example (if configured):
    # email_op = EmailOperator(task_id='...', to=NOTIFICATION_EMAIL, subject=subject, html_content=body)
    # You wouldn't typically run `email_op.execute()` here.


# ----------------------------------------------------------------------------
# ETL Core Functions (Extract, Transform, Load, Verify)
# ----------------------------------------------------------------------------

def extract_weather_data(city_name: str, **context) -> Optional[Dict[str, Any]]:
    """
    Extracts current weather data from OpenWeatherMap API for the given city.
    Handles API key retrieval, error threshold checking, API requests, response validation,
    and pushing raw data to XComs. Resets error count on success.
    """
    log_etl_event(f"Starting weather data extraction for {city_name}", city_name)
    ti = context['ti'] # TaskInstance object from context
    dag_run_id = context['run_id'] # Current DAG run ID for logging

    # --- Pre-checks ---
    # 1. Get API key (task fails if Variable is missing/invalid)
    api_key = get_api_key()

    # 2. Check error threshold - uses a separate check function for clarity
    # The actual branching logic is handled by the BranchPythonOperator based on this check.
    # However, this function raises AirflowSkipException if threshold is met,
    # preventing further execution *within this function*.
    if check_error_threshold(city_name, **context):
         # Note: BranchPythonOperator might have already directed flow elsewhere,
         # but this adds safety if called directly or if branching logic changed.
         raise AirflowSkipException(f"Skipping {city_name} extraction for run {dag_run_id} due to error threshold.")

    # --- API Request ---
    # Build request URL and parameters
    # Base URL could also be retrieved from the Airflow connection `host` field if using HttpHook/Operator
    request_url = f"{API_BASE_URL}{API_ENDPOINT}"
    params = {
        'q': city_name,    # City name
        'appid': api_key,  # API key
        'units': 'metric'  # Request units in Celsius/meters/sec
    }
    request_timeout = 20 # Seconds to wait for API response

    try:
        log_etl_event(f"Requesting weather: {request_url}?q={city_name}...", city_name)
        response = requests.get(request_url, params=params, timeout=request_timeout)

        # --- Response Handling ---
        # 1. Check for specific HTTP error codes
        if response.status_code == 404: # Not Found
            error_msg = f"City '{city_name}' not found by OpenWeatherMap API (404)."
            log_etl_event(error_msg, city_name, "ERROR")
            increment_error_count(city_name, **context) # Increment count for bad city
            # Fail the task - bad city name is likely a config issue needing fix.
            raise AirflowFailException(error_msg)
            # Alternative: Use AirflowSkipException if 404s should just skip silently.

        elif response.status_code == 401: # Unauthorized
            error_msg = f"API Key invalid or expired (401 Unauthorized). Check Variable '{OPENWEATHERMAP_API_KEY_VAR}'."
            log_etl_event(error_msg, city_name, "ERROR")
            # Don't increment city error count for global key issues
            raise AirflowFailException(error_msg) # Critical failure, likely affects all cities

        elif response.status_code == 429: # Too Many Requests (Rate Limit)
            error_msg = f"API rate limit exceeded (429). Check OpenWeatherMap plan. Task will retry."
            log_etl_event(error_msg, city_name, "WARNING")
            increment_error_count(city_name, **context) # Increment count, may be temporary
            raise AirflowFailException(error_msg) # Fail task to allow Airflow retry mechanism

        # 2. Raise exception for any other non-2xx status codes (e.g., 5xx server errors)
        response.raise_for_status()

        # --- Process Successful Response ---
        # 3. Parse JSON response
        data = response.json()

        # 4. Basic validation of expected structure
        if not isinstance(data, dict) or not all(k in data for k in ('name', 'main', 'weather', 'dt')):
            error_msg = f"API response lacks essential fields. Response sample: {str(data)[:200]}..."
            log_etl_event(error_msg, city_name, "ERROR")
            increment_error_count(city_name, **context)
            raise AirflowFailException(error_msg)

        # 5. Log success and reset error count for this city
        temp = data.get('main', {}).get('temp', 'N/A')
        condition = data.get('weather', [{}])[0].get('main', 'N/A')
        log_etl_event(f"Successfully extracted data. Temp: {temp}°C, Condition: {condition}", city_name)
        reset_error_count(city_name, **context) # Reset counter on successful fetch

        # --- Push data to XComs ---
        # Define a consistent key format for XComs related to this city's raw data
        xcom_key_raw = f'weather_raw_data_{city_name.lower().replace(" ", "_")}'
        ti.xcom_push(key=xcom_key_raw, value=data)
        log_etl_event(f"Pushed raw data to XCom key: {xcom_key_raw}", city_name)

        # Return the data dictionary (optional, as downstream tasks primarily use XComs)
        return data

    # --- Specific Exception Handling ---
    except requests.exceptions.Timeout as e:
        error_msg = f"API request timed out after {request_timeout} seconds: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg) # Fail task, allow retry

    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed (network/connection issue?): {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg) # Fail task, allow retry

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse API response as JSON. Response: {response.text[:200]}... Error: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg)

    # --- General Exception Handling ---
    # Ensure specific Airflow exceptions are re-raised correctly
    except AirflowSkipException:
         raise
    except AirflowFailException:
         raise
    # Catch any other unexpected Python errors
    except Exception as e:
        error_msg = f"Unexpected Python error during extraction: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        increment_error_count(city_name, **context)
        raise AirflowFailException(error_msg) # Fail task


def transform_weather_data(city_name: str, **context) -> Optional[Dict[str, Any]]:
    """
    Transforms raw weather data (from XComs) into a flattened, structured dictionary.
    Performs safe extraction of nested fields and basic data quality checks.
    Pushes the transformed data to XComs.
    """
    log_etl_event(f"Starting data transformation for {city_name}", city_name)
    ti = context['ti']

    # --- Retrieve Raw Data from XComs ---
    # Construct the key used by the corresponding extract task
    city_slug = city_name.lower().replace(" ", "_")
    xcom_key_raw = f'weather_raw_data_{city_slug}'
    # Define the task ID of the upstream extract task that pushed the data
    task_id_extract = f'process_{city_slug}.extract_weather_{city_slug}' # Include TaskGroup ID

    raw_data = ti.xcom_pull(task_ids=task_id_extract, key=xcom_key_raw)

    # --- Validate Input Data ---
    if not raw_data or not isinstance(raw_data, dict):
        # This task should only run if extract succeeded, so raw_data should exist.
        # Log a warning if it's missing/invalid for some reason.
        log_etl_event(f"No valid raw data found from XCom key '{xcom_key_raw}' (Task ID: {task_id_extract}). Skipping transformation.", city_name, "WARNING")
        # Return None to indicate no data processed; subsequent load tasks should handle this.
        return None

    # --- Perform Transformation ---
    try:
        # Safely access nested dictionary fields using .get() with default values (e.g., {} or None)
        main_data = raw_data.get("main", {})
        wind_data = raw_data.get("wind", {})
        coord_data = raw_data.get("coord", {})
        sys_data = raw_data.get("sys", {})
        clouds_data = raw_data.get("clouds", {})
        rain_data = raw_data.get("rain", {})   # Can be missing/None if no rain
        snow_data = raw_data.get("snow", {})   # Can be missing/None if no snow

        # Weather data is a list, typically with one item. Handle gracefully if list is empty.
        weather_list = raw_data.get("weather", [])
        weather_item = weather_list[0] if weather_list else {} # Use first item, or empty dict

        # Extract time information
        execution_dt_iso = context['ts'] # Airflow logical timestamp (iso format string)
        observation_dt_unix = raw_data.get("dt") # Observation time from API (unix timestamp)
        observation_dt_iso = datetime.utcfromtimestamp(observation_dt_unix).isoformat() if observation_dt_unix else None
        timezone_offset_sec = raw_data.get("timezone") # Location timezone offset from UTC (seconds)
        timezone_offset_hrs = timezone_offset_sec / 3600 if timezone_offset_sec is not None else None

        # Build the flattened, transformed dictionary
        transformed_data = {
            # Location Info
            "city_name": raw_data.get("name"),
            "country_code": sys_data.get("country"),
            "longitude": coord_data.get("lon"),
            "latitude": coord_data.get("lat"),
            "openweathermap_city_id": raw_data.get("id"), # OpenWeatherMap's internal city ID

            # Time Info (UTC)
            "observation_time_unix_utc": observation_dt_unix,
            "observation_time_iso_utc": observation_dt_iso,
            "system_sunrise_time_unix_utc": sys_data.get("sunrise"), # As reported by API
            "system_sunset_time_unix_utc": sys_data.get("sunset"),   # As reported by API
            "timezone_offset_seconds": timezone_offset_sec,
            "timezone_offset_hours": timezone_offset_hrs,
            "data_fetch_timestamp_iso_utc": execution_dt_iso,      # When the DAG ran

            # Weather Conditions
            "weather_condition_id": weather_item.get("id"),        # Weather condition code
            "weather_main": weather_item.get("main"),              # Main condition (e.g., Clouds, Rain)
            "weather_description": weather_item.get("description"),# Detailed condition
            "weather_icon_code": weather_item.get("icon"),         # Icon identifier

            # Temperature (Metric units requested: Celsius)
            "temp_celsius": main_data.get("temp"),
            "temp_feels_like_celsius": main_data.get("feels_like"),
            "temp_min_celsius": main_data.get("temp_min"),          # Min temp currently observed
            "temp_max_celsius": main_data.get("temp_max"),          # Max temp currently observed

            # Atmospheric Conditions
            "pressure_hpa": main_data.get("pressure"),             # Atmospheric pressure (hPa/millibars)
            "humidity_percent": main_data.get("humidity"),
            "sea_level_pressure_hpa": main_data.get("sea_level"),    # Pressure at sea level, if available
            "grnd_level_pressure_hpa": main_data.get("grnd_level"), # Pressure at ground level, if available
            "visibility_meters": raw_data.get("visibility"),       # Average visibility

            # Wind (Metric units requested: meters/sec)
            "wind_speed_mps": wind_data.get("speed"),
            "wind_direction_deg": wind_data.get("deg"),             # Meteorological degrees
            "wind_gust_mps": wind_data.get("gust"),                 # Wind gust speed, if available

            # Clouds
            "cloudiness_percent": clouds_data.get("all"),          # Cloud cover percentage

            # Precipitation (Metric: mm) - Volumes reported for the last 1 or 3 hours
            "rain_last_1h_mm": rain_data.get("1h"), # Will be None if no 'rain' field in raw_data
            "rain_last_3h_mm": rain_data.get("3h"),
            "snow_last_1h_mm": snow_data.get("1h"), # Will be None if no 'snow' field
            "snow_last_3h_mm": snow_data.get("3h"),

            # Metadata
            "api_version_used": API_VERSION                       # API version configured in DAG
        }

        # --- Basic Data Quality Check ---
        # Ensure some critical fields have values after transformation
        if transformed_data['temp_celsius'] is None or transformed_data['observation_time_unix_utc'] is None:
            raise ValueError(f"Essential transformed fields (temp_celsius, observation_time_unix_utc) are missing. Check raw data: {str(raw_data)[:200]}...")

        log_etl_event(f"Transformation successful. Obs Time: {observation_dt_iso}", city_name)

        # --- Push Transformed Data to XComs ---
        # Use a consistent key format for transformed data
        xcom_key_transformed = f'weather_transformed_data_{city_slug}'
        ti.xcom_push(key=xcom_key_transformed, value=transformed_data)
        log_etl_event(f"Pushed transformed data to XCom key: {xcom_key_transformed}", city_name)

        # Return transformed data (optional)
        return transformed_data

    # --- Exception Handling ---
    except Exception as e:
        error_msg = f"Error during data transformation: {str(e)}. Raw data sample: {str(raw_data)[:200]}..."
        log_etl_event(error_msg, city_name, "ERROR")
        # Fail the task if transformation encounters an error
        raise AirflowFailException(error_msg)


def load_weather_data_to_json(city_name: str, storage_type: str, **context) -> bool:
    """
    Loads transformed weather data (from XComs) into a JSON file.
    Supports 'LOCAL' and 'S3' storage types based on the `storage_type` argument.
    Pushes the output file path/key to XComs.
    """
    # Check if the specified storage method is enabled in the global configuration
    if storage_type not in STORAGE_METHODS:
        log_etl_event(f"Skipping JSON load ({storage_type}): Method not enabled in STORAGE_METHODS.", city_name, "INFO")
        return False # Signal skipped, not failed

    log_etl_event(f"Starting JSON data loading to {storage_type} for {city_name}", city_name)
    ti = context['ti']

    # --- Retrieve Transformed Data ---
    city_slug = city_name.lower().replace(" ", "_")
    xcom_key_transformed = f'weather_transformed_data_{city_slug}'
    # Task ID needs to include the TaskGroup ID
    task_id_transform = f'process_{city_slug}.transform_weather_{city_slug}'

    data_to_load = ti.xcom_pull(task_ids=task_id_transform, key=xcom_key_transformed)

    # --- Validate Input ---
    if not data_to_load:
        log_etl_event(f"No transformed data found from XCom key '{xcom_key_transformed}'. Skipping JSON load to {storage_type}.", city_name, "WARNING")
        # Do not fail, but return False as loading didn't happen
        return False

    # --- Prepare Filename/Key ---
    execution_date = context['execution_date'] # Logical date of the run
    date_str = get_formatted_date(execution_date, '%Y%m%d') # YYYYMMDD format
    time_str = get_formatted_time(execution_date, '%H%M%S') # HHMMSS format
    # Create a unique filename using city, date, and time
    filename = f"weather_{city_slug}_{date_str}_{time_str}.json"

    try:
        # --- Convert Data to JSON String ---
        # Using compact separators can save space, indent=None disables pretty-printing.
        data_string = json.dumps(data_to_load, indent=None, separators=(',', ':'), ensure_ascii=False) # Allow unicode chars

        # --- Load based on Storage Type ---
        if storage_type == "LOCAL":
            # Define the full local path, organized by date
            daily_dir = os.path.join(LOCAL_DIR_PATH, date_str) # e.g., /tmp/.../json_data/20231027/
            ensure_directory_exists(daily_dir) # Create date directory if it doesn't exist
            file_path = os.path.join(daily_dir, filename)

            log_etl_event(f"Writing JSON data to local file: {file_path}", city_name)
            with open(file_path, 'w', encoding='utf-8') as f: # Specify UTF-8 encoding
                f.write(data_string)

            # Push the local file path to XComs for sensor/verification tasks
            xcom_key_local_path = f'local_json_path_{city_slug}'
            ti.xcom_push(key=xcom_key_local_path, value=file_path)
            log_etl_event(f"Local JSON load successful. Path pushed to XCom key: {xcom_key_local_path}", city_name)
            return True

        elif storage_type == "S3":
            # Define the S3 object key, using date partitioning
            s3_key = f"{S3_KEY_PREFIX}/{date_str}/{filename}" # e.g., weather_data/20231027/weather_london_...json
            s3_full_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
            log_etl_event(f"Uploading JSON data to S3: {s3_full_path}", city_name)

            try:
                # Use S3Hook to interact with S3
                s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                s3_hook.load_string(
                    string_data=data_string, # The JSON data as a string
                    key=s3_key,             # The S3 object key
                    bucket_name=S3_BUCKET_NAME, # Target bucket name
                    replace=True,          # Overwrite if object key already exists (e.g., on task retry)
                    acl_policy=None,       # Use bucket's default ACL
                    # encoding='utf-8'     # load_string handles string data directly
                )
            except Exception as s3_error:
                # Catch specific S3 errors (credentials, bucket access, etc.)
                log_etl_event(f"S3 load failed for {s3_full_path}: {s3_error}", city_name, "ERROR")
                raise AirflowFailException(f"S3 load operation failed: {s3_error}") # Fail the task

            # Push S3 key and full path to XComs for sensor/verification
            xcom_key_s3_key = f's3_json_key_{city_slug}'
            xcom_key_s3_path = f's3_json_path_{city_slug}'
            ti.xcom_push(key=xcom_key_s3_key, value=s3_key)
            ti.xcom_push(key=xcom_key_s3_path, value=s3_full_path)
            log_etl_event(f"S3 JSON load successful. Key/Path pushed to XComs: {xcom_key_s3_key}, {xcom_key_s3_path}", city_name)
            return True

        else:
            # This case should not be reached if storage_type check at start is working
            log_etl_event(f"Internal Error: Unsupported storage_type '{storage_type}' encountered in load_json.", city_name, "ERROR")
            return False # Should technically be unreachable

    # --- Exception Handling ---
    except AirflowFailException:
        # Re-raise specific S3 load failures
        raise
    except Exception as e:
        # Catch other errors (e.g., JSON serialization, file write permissions)
        error_msg = f"Unexpected error during JSON loading ({storage_type}) for {city_name}: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        raise AirflowFailException(error_msg) # Fail the task


def load_weather_data_to_csv(city_name: str, **context) -> bool:
    """
    Loads transformed weather data (from XComs) by appending it as a new row
    to a city-specific CSV file. Creates the file and header if it doesn't exist.
    Attempts to handle schema changes by adding new columns if detected.
    Pushes the CSV file path to XComs.
    """
    # Check if CSV storage is enabled
    if "CSV" not in STORAGE_METHODS:
        log_etl_event("Skipping CSV load: CSV method not enabled in STORAGE_METHODS.", city_name, "INFO")
        return False # Skipped

    log_etl_event(f"Starting CSV data loading/appending for {city_name}", city_name)
    ti = context['ti']

    # --- Retrieve Transformed Data ---
    city_slug = city_name.lower().replace(" ", "_")
    xcom_key_transformed = f'weather_transformed_data_{city_slug}'
    task_id_transform = f'process_{city_slug}.transform_weather_{city_slug}'
    data_to_load = ti.xcom_pull(task_ids=task_id_transform, key=xcom_key_transformed)

    # --- Validate Input ---
    if not data_to_load:
        log_etl_event(f"No transformed data found from XCom key '{xcom_key_transformed}'. Skipping CSV load.", city_name, "WARNING")
        return False # No data to load

    try:
        # --- Prepare File Path and DataFrame ---
        # Ensure the base directory for CSVs exists
        ensure_directory_exists(CSV_DIR_PATH)
        # Define a consistent CSV filename for the city's history
        csv_filename = f"{city_slug}_weather_history.csv"
        csv_path = os.path.join(CSV_DIR_PATH, csv_filename)
        log_etl_event(f"Target CSV file for appending/creation: {csv_path}", city_name)

        # Convert the single record (dict) into a pandas DataFrame for easy CSV handling
        df_new_data = pd.DataFrame([data_to_load])

        # --- Handle File Existence and Schema ---
        file_exists = os.path.isfile(csv_path)
        file_is_empty = file_exists and os.path.getsize(csv_path) == 0

        if file_exists and not file_is_empty:
            log_etl_event(f"CSV file exists and is not empty. Checking schema before appending...", city_name)
            try:
                # Read only header to get existing columns (more efficient than reading whole file)
                df_existing_header = pd.read_csv(csv_path, nrows=0)
                existing_columns = df_existing_header.columns.tolist()
                new_columns = df_new_data.columns.tolist()

                # --- Schema Evolution Check ---
                # Check if the new data contains columns not present in the existing file
                added_columns = set(new_columns) - set(existing_columns)
                if added_columns:
                    log_etl_event(f"Schema change! New columns detected: {added_columns}. Realigning CSV file.", city_name, "WARNING")

                    # --- Backup existing file ---
                    backup_ts = int(time.time())
                    backup_path = os.path.join(ARCHIVE_DIR_PATH, f"{csv_filename}.backup_{backup_ts}")
                    try:
                        ensure_directory_exists(ARCHIVE_DIR_PATH) # Ensure archive dir exists
                        shutil.copy2(csv_path, backup_path)
                        log_etl_event(f"Created backup of existing CSV: {backup_path}", city_name)
                    except Exception as backup_err:
                        log_etl_event(f"Failed to create CSV backup {backup_path}: {backup_err}. Proceeding with overwrite cautiously.", city_name, "WARNING")
                    # --- End Backup ---

                    # --- Realign and Overwrite ---
                    # Read the *full* existing data
                    df_existing_full = pd.read_csv(csv_path)
                    # Determine the complete set of columns (preserving order mostly)
                    all_columns = list(dict.fromkeys(existing_columns + new_columns))
                    # Reindex both existing and new dataframes to include all columns, filling missing with NA
                    df_existing_realigned = df_existing_full.reindex(columns=all_columns, fill_value=pd.NA) # Use pandas NA
                    df_new_data_realigned = df_new_data.reindex(columns=all_columns, fill_value=pd.NA)
                    # Combine the realigned dataframes
                    df_combined = pd.concat([df_existing_realigned, df_new_data_realigned], ignore_index=True)
                    # Overwrite the original CSV file with the combined data and full header
                    df_combined.to_csv(csv_path, index=False, header=True, mode='w', encoding='utf-8') # Explicit 'w' mode
                    log_etl_event(f"CSV file {csv_path} rewritten with updated schema ({len(all_columns)} columns).", city_name)
                    # --- End Realign and Overwrite ---

                else:
                    # --- Append Data (Schema Compatible) ---
                    # If no new columns, append the new data without the header
                    log_etl_event(f"Schema compatible. Appending data to {csv_path}", city_name)
                    df_new_data.to_csv(csv_path, mode='a', header=False, index=False, encoding='utf-8')
                    # --- End Append Data ---

            except pd.errors.EmptyDataError:
                # Handle case where file exists but pandas reads it as empty (e.g., only header, corrupted)
                log_etl_event(f"Existing CSV file {csv_path} is empty or invalid. Overwriting with new data and header.", city_name, "WARNING")
                df_new_data.to_csv(csv_path, mode='w', header=True, index=False, encoding='utf-8')

            except Exception as schema_check_err:
                # Catch other errors during schema check/realignment (e.g., file read errors)
                log_etl_event(f"Error during CSV schema check/append for {csv_path}: {schema_check_err}. Check file integrity.", city_name, "ERROR")
                # Fail the task, as proceeding could corrupt data further
                raise AirflowFailException(f"CSV schema check/append failed: {schema_check_err}")

        else:
            # --- Create New File ---
            # File doesn't exist or is empty, create it with the header
            log_etl_event(f"CSV file {csv_path} not found or empty. Creating new file with header.", city_name)
            df_new_data.to_csv(csv_path, mode='w', header=True, index=False, encoding='utf-8')
            # --- End Create New File ---


        # --- Push Path to XComs ---
        xcom_key_csv_path = f'csv_path_{city_slug}'
        ti.xcom_push(key=xcom_key_csv_path, value=csv_path)
        log_etl_event(f"CSV load/append successful. Path pushed to XCom key: {xcom_key_csv_path}", city_name)
        return True # Signal success

    # --- Exception Handling ---
    except AirflowFailException:
        # Re-raise failures from schema checks/writes
        raise
    except Exception as e:
        # Catch other unexpected errors (e.g., permissions, DataFrame issues)
        error_msg = f"Unexpected error during CSV loading for {city_name}: {str(e)}"
        log_etl_event(error_msg, city_name, "ERROR")
        raise AirflowFailException(error_msg) # Fail the task


def verify_data_load(city_name: str, **context) -> bool:
    """
    Verifies data load success by checking for expected outputs (files/S3 keys)
    based on the enabled STORAGE_METHODS. Checks existence and basic non-emptiness.
    Logs detailed results and pushes an overall verification status (True/False) to XComs.
    This task itself does *not* fail on verification errors, allowing the summary task
    to report on verification status.
    """
    log_etl_event(f"Starting data load verification for {city_name}", city_name)
    ti = context['ti']
    city_slug = city_name.lower().replace(" ", "_")
    overall_verification_passed = True # Assume success initially
    verification_details = {} # Store results per method

    # --- Verify LOCAL JSON ---
    if "LOCAL" in STORAGE_METHODS:
        method = "LOCAL_JSON"
        verification_details[method] = {"status": "CHECKING", "path": None, "message": None}
        log_etl_event(f"Verifying {method} storage...", city_name)
        xcom_key = f'local_json_path_{city_slug}'
        # Task ID needs group prefix
        task_id_load = f'process_{city_slug}.load_json_local_{city_slug}'
        file_path = ti.xcom_pull(task_ids=task_id_load, key=xcom_key)
        verification_details[method]["path"] = file_path

        if file_path and isinstance(file_path, str):
            if os.path.exists(file_path):
                try:
                    if os.path.getsize(file_path) > 2: # Check if file has content beyond "{}"
                        verification_details[method]["status"] = "VERIFIED"
                        log_etl_event(f"{method} file verified: {file_path}", city_name)
                    else:
                        overall_verification_passed = False
                        verification_details[method]["status"] = "FAILED_EMPTY"
                        verification_details[method]["message"] = f"File exists but is empty/minimal."
                        log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
                except OSError as e:
                     overall_verification_passed = False
                     verification_details[method]["status"] = "FAILED_ACCESS"
                     verification_details[method]["message"] = f"Error accessing file: {e}"
                     log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
            else:
                overall_verification_passed = False
                verification_details[method]["status"] = "FAILED_NOT_FOUND"
                verification_details[method]["message"] = "File path retrieved but file does not exist."
                log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
        else:
            # Check if the load task failed or was skipped, explaining why XCom is missing
            load_task_state = ti.get_task_instance(task_id=task_id_load).current_state() if ti.task else 'unknown' # Need task instance check?
            if load_task_state not in ('success', 'skipped'):
                 overall_verification_passed = False # Missing output due to upstream failure
                 verification_details[method]["status"] = "FAILED_NO_XCOM"
                 verification_details[method]["message"] = f"File path not found in XComs (Load task state: {load_task_state})."
                 log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "WARNING")
            else:
                 # Load task skipped (or succeeded but somehow didn't push?), can't verify
                 verification_details[method]["status"] = "SKIPPED_NO_XCOM"
                 verification_details[method]["message"] = f"File path not found in XComs (Load task state: {load_task_state}). Assuming upstream skip."
                 log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "INFO")

    # --- Verify CSV ---
    if "CSV" in STORAGE_METHODS:
        method = "CSV"
        verification_details[method] = {"status": "CHECKING", "path": None, "message": None}
        log_etl_event(f"Verifying {method} storage...", city_name)
        xcom_key = f'csv_path_{city_slug}'
        task_id_load = f'process_{city_slug}.load_csv_{city_slug}'
        file_path = ti.xcom_pull(task_ids=task_id_load, key=xcom_key)
        verification_details[method]["path"] = file_path

        if file_path and isinstance(file_path, str):
            if os.path.exists(file_path):
                try:
                    # Check if file has more than just a potential header (heuristic)
                    if os.path.getsize(file_path) > 20:
                        # Could add a pandas read check for more robustness, but adds overhead
                        # e.g., try: pd.read_csv(file_path, nrows=1); except pd.errors.EmptyDataError: ...
                        verification_details[method]["status"] = "VERIFIED"
                        log_etl_event(f"{method} file verified: {file_path}", city_name)
                    else:
                        overall_verification_passed = False
                        verification_details[method]["status"] = "FAILED_EMPTY"
                        verification_details[method]["message"] = "CSV file exists but seems empty or header-only."
                        log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
                except OSError as e:
                     overall_verification_passed = False
                     verification_details[method]["status"] = "FAILED_ACCESS"
                     verification_details[method]["message"] = f"Error accessing CSV file: {e}"
                     log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
            else:
                 overall_verification_passed = False
                 verification_details[method]["status"] = "FAILED_NOT_FOUND"
                 verification_details[method]["message"] = "CSV path retrieved but file does not exist."
                 log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({file_path})", city_name, "ERROR")
        else:
            load_task_state = ti.get_task_instance(task_id=task_id_load).current_state() if ti.task else 'unknown'
            if load_task_state not in ('success', 'skipped'):
                 overall_verification_passed = False
                 verification_details[method]["status"] = "FAILED_NO_XCOM"
                 verification_details[method]["message"] = f"CSV path not found in XComs (Load task state: {load_task_state})."
                 log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "WARNING")
            else:
                 verification_details[method]["status"] = "SKIPPED_NO_XCOM"
                 verification_details[method]["message"] = f"CSV path not found in XComs (Load task state: {load_task_state}). Assuming upstream skip."
                 log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "INFO")


    # --- Verify S3 JSON ---
    if "S3" in STORAGE_METHODS:
        method = "S3_JSON"
        verification_details[method] = {"status": "CHECKING", "key": None, "path": None, "message": None}
        log_etl_event(f"Verifying {method} storage...", city_name)
        xcom_key_s3_key = f's3_json_key_{city_slug}'
        xcom_key_s3_path = f's3_json_path_{city_slug}'
        task_id_load = f'process_{city_slug}.load_json_s3_{city_slug}'
        s3_key = ti.xcom_pull(task_ids=task_id_load, key=xcom_key_s3_key)
        s3_path = ti.xcom_pull(task_ids=task_id_load, key=xcom_key_s3_path) # Get full path for logging
        verification_details[method]["key"] = s3_key
        verification_details[method]["path"] = s3_path

        if s3_key and isinstance(s3_key, str):
             try:
                s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
                # Check if the key exists in the specified bucket
                if s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
                    # Optionally, check object size (requires extra API call)
                    s3_obj = s3_hook.get_key(key=s3_key, bucket_name=S3_BUCKET_NAME)
                    if s3_obj.content_length > 2: # Check if not effectively empty
                        verification_details[method]["status"] = "VERIFIED"
                        log_etl_event(f"{method} object verified: {s3_path}", city_name)
                    else:
                        overall_verification_passed = False
                        verification_details[method]["status"] = "FAILED_EMPTY"
                        verification_details[method]["message"] = "S3 object exists but is empty/minimal."
                        log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({s3_path})", city_name, "ERROR")
                else:
                    overall_verification_passed = False
                    verification_details[method]["status"] = "FAILED_NOT_FOUND"
                    verification_details[method]["message"] = "S3 key retrieved but object does not exist."
                    log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({s3_path})", city_name, "ERROR")
             except Exception as e:
                 overall_verification_passed = False
                 verification_details[method]["status"] = "FAILED_ACCESS"
                 verification_details[method]["message"] = f"Error verifying S3 object: {e}"
                 log_etl_event(f"{method} Verification Failed: {verification_details[method]['message']} ({s3_path})", city_name, "ERROR")
        else:
            load_task_state = ti.get_task_instance(task_id=task_id_load).current_state() if ti.task else 'unknown'
            if load_task_state not in ('success', 'skipped'):
                overall_verification_passed = False
                verification_details[method]["status"] = "FAILED_NO_XCOM"
                verification_details[method]["message"] = f"S3 key not found in XComs (Load task state: {load_task_state})."
                log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "WARNING")
            else:
                 verification_details[method]["status"] = "SKIPPED_NO_XCOM"
                 verification_details[method]["message"] = f"S3 key not found in XComs (Load task state: {load_task_state}). Assuming upstream skip."
                 log_etl_event(f"{method} Verification Skipped: {verification_details[method]['message']}", city_name, "INFO")

    # --- Final Logging and Result ---
    log_etl_event(f"Verification details: {json.dumps(verification_details)}", city_name)
    if overall_verification_passed:
        log_etl_event(f"Overall data load verification PASSED for {city_name}.", city_name, "INFO")
    else:
        # Compile failure messages
        failed_messages = [f"{method}: {details.get('message', 'Unknown reason')}"
                           for method, details in verification_details.items()
                           if details.get("status", "").startswith("FAILED")]
        error_summary = "; ".join(failed_messages) if failed_messages else "Verification failed for unknown reasons."
        log_etl_event(f"Overall data load verification FAILED for {city_name}. Issues: {error_summary}", city_name, "ERROR")
        # Trigger notification function (which currently just logs)
        send_error_notification(
             city_name=city_name,
             error_message=f"Data load verification failed. Issues: {error_summary}",
             **context
        )

    # Push the final True/False status to XComs for the summary task
    xcom_key_verify_status = f'verification_status_{city_slug}'
    ti.xcom_push(key=xcom_key_verify_status, value=overall_verification_passed)

    # Return the status, although this task's success/failure isn't driven by this boolean directly
    return overall_verification_passed


def cleanup_old_files(**context):
    """
    Cleans up old data files/objects based on `days_to_keep`.
    - Local JSON: Moves dated directories older than `days_to_keep` to ARCHIVE_DIR_PATH.
    - S3 JSON: Deletes objects under date prefixes older than `days_to_keep`.
    - CSV: Currently skipped as they are history files (not date-partitioned).
    Logs actions and errors but does not fail the pipeline on cleanup issues.
    """
    days_to_keep = 7 # Configure how many days of data to retain
    log_etl_event(f"Starting cleanup task: Removing data older than {days_to_keep} days.")
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    cutoff_str_yyyymmdd = cutoff_date.strftime('%Y%m%d')
    log_etl_event(f"Cleanup cutoff date (exclusive): {cutoff_date.strftime('%Y-%m-%d')} ({cutoff_str_yyyymmdd})")

    # --- Local JSON Cleanup ---
    if "LOCAL" in STORAGE_METHODS and os.path.exists(LOCAL_DIR_PATH):
        log_etl_event(f"Starting local JSON cleanup in: {LOCAL_DIR_PATH}")
        archived_count = 0
        failed_archive_count = 0
        try:
            ensure_directory_exists(ARCHIVE_DIR_PATH) # Ensure archive destination exists
            for dir_name in os.listdir(LOCAL_DIR_PATH):
                # Check if item is a directory, named as a date (YYYYMMDD), and older than cutoff
                local_date_dir = os.path.join(LOCAL_DIR_PATH, dir_name)
                if os.path.isdir(local_date_dir) and dir_name.isdigit() and len(dir_name) == 8 and dir_name < cutoff_str_yyyymmdd:
                    log_etl_event(f"Identified old local data directory: {local_date_dir}", status="INFO")
                    archive_target_path = os.path.join(ARCHIVE_DIR_PATH, dir_name)
                    try:
                        # Atomically move the directory to the archive path
                        shutil.move(local_date_dir, archive_target_path)
                        log_etl_event(f"Successfully archived '{dir_name}' to {archive_target_path}", status="INFO")
                        archived_count += 1
                    except Exception as move_err:
                        log_etl_event(f"Failed to archive local directory {local_date_dir}: {move_err}. Check permissions/target.", status="ERROR")
                        failed_archive_count += 1
                        # Decide if you want to attempt deletion after failed move: shutil.rmtree(local_date_dir)?
            log_etl_event(f"Local JSON cleanup: Archived {archived_count} directories, Failed to archive {failed_archive_count}.", status="INFO")
        except Exception as e:
            log_etl_event(f"Error during local file cleanup process: {e}", status="ERROR")

    # --- S3 JSON Cleanup ---
    if "S3" in STORAGE_METHODS:
        log_etl_event(f"Starting S3 JSON cleanup in bucket '{S3_BUCKET_NAME}' (prefix '{S3_KEY_PREFIX}/')")
        deleted_count = 0
        try:
            s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            keys_to_delete = []

            # List date-based "prefixes" (common prefixes ending with /)
            # Example: 'weather_data/20231020/'
            s3_prefixes = s3_hook.list_prefixes(bucket_name=S3_BUCKET_NAME, prefix=f"{S3_KEY_PREFIX}/", delimiter='/')

            if s3_prefixes:
                 log_etl_event(f"Found {len(s3_prefixes)} potential date prefixes in S3 to check.", status="INFO")
                 for prefix in s3_prefixes:
                      # Extract date part (assuming format .../YYYYMMDD/)
                      parts = prefix.strip('/').split('/')
                      if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 8:
                          date_str = parts[-1]
                          if date_str < cutoff_str_yyyymmdd:
                               log_etl_event(f"Identified old S3 prefix to clear: {prefix}", status="INFO")
                               # List all keys within this old prefix (no delimiter needed here)
                               keys_in_prefix = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=prefix)
                               if keys_in_prefix:
                                    log_etl_event(f"Found {len(keys_in_prefix)} objects under prefix {prefix} for deletion.", status="DEBUG") # Debug log
                                    keys_to_delete.extend(keys_in_prefix)
                          # else: log_etl_event(f"Prefix {prefix} is not older than cutoff.", status="DEBUG") # Debug log

            if keys_to_delete:
                log_etl_event(f"Total S3 objects identified for deletion: {len(keys_to_delete)}. Proceeding with batch deletion...", status="INFO")
                # S3 delete_objects works in batches (max 1000 per request)
                chunk_size = 1000
                for i in range(0, len(keys_to_delete), chunk_size):
                    batch = keys_to_delete[i:i + chunk_size]
                    delete_result = s3_hook.delete_objects(bucket=S3_BUCKET_NAME, keys=batch)
                    # delete_result contains info about successes/errors if needed
                    deleted_this_batch = len(batch) # Assume success unless exceptions occurred
                    deleted_count += deleted_this_batch
                    log_etl_event(f"S3 deletion batch successful (approx {deleted_this_batch} keys).", status="INFO")
                log_etl_event(f"S3 cleanup finished. Total objects deleted: {deleted_count}", status="INFO")
            else:
                log_etl_event("No old S3 objects found matching the criteria for deletion.", status="INFO")

        except Exception as e:
            log_etl_event(f"Error during S3 object cleanup: {str(e)}", status="ERROR")

    # --- CSV Cleanup ---
    if "CSV" in STORAGE_METHODS:
        # These are history files, not date partitioned. Cleanup logic is different.
        # Example ideas (not implemented):
        # - Archive/delete if file size exceeds a threshold.
        # - Read file, filter out old rows, rewrite (complex and potentially slow).
        log_etl_event("Skipping CSV cleanup: History files are not currently configured for automatic date-based cleanup.", status="INFO")

    log_etl_event("File cleanup task completed.", status="INFO")
    return True # Return True even if parts failed, task is best-effort


def create_pipeline_summary(**context):
    """
    Analyzes task instance states and verification XComs for each city
    to generate a comprehensive summary of the DAG run. Saves summary to
    log files (JSONL, CSV) and pushes the summary dictionary to XComs.
    """
    log_etl_event("Creating pipeline execution summary...")
    ti = context['ti'] # Current task instance (for accessing other TIs)
    dag_run = context['dag_run'] # Current DAG run object
    execution_date = dag_run.execution_date # Logical execution date
    dag_run_id = dag_run.run_id             # Unique ID for this run

    # --- Performance Timings ---
    run_start_time_epoch = ti.xcom_pull(key=START_TIME_XCOM_KEY, task_ids='record_start_time')
    run_end_time_epoch = time.time()
    execution_time_sec = (run_end_time_epoch - run_start_time_epoch) if run_start_time_epoch else None
    execution_time_str = f"{execution_time_sec:.2f}" if execution_time_sec is not None else "N/A"

    # --- Initialize Counters ---
    city_count = len(CITIES)
    stats = {
        "SUCCESS": 0,             # Verified successfully
        "FAIL_VERIFY_LOAD": 0,    # Extracted, Transformed, but Load/Verify failed
        "FAIL_TRANSFORM": 0,      # Extract OK, Transform failed
        "FAIL_EXTRACT": 0,        # Extract failed
        "SKIPPED_THRESHOLD": 0,   # Skipped due to error threshold
        "UNKNOWN_STATE": 0,       # Failed to determine status clearly
        "ERROR_CHECKING_STATUS": 0# Error occurred within this summary function
    }
    city_statuses = {} # Store the determined status string for each city

    # --- Determine Status for Each City ---
    for city in CITIES:
        city_slug = city.lower().replace(" ", "_")
        task_group_id = f'process_{city_slug}'
        task_id_extract = f'{task_group_id}.extract_weather_{city_slug}'
        task_id_transform = f'{task_group_id}.transform_weather_{city_slug}'
        task_id_verify = f'{task_group_id}.verify_data_{city_slug}'
        city_status = "PENDING" # Default status

        try:
            # Get states of key task instances for this city in this DAG run
            # Need to access TaskInstance directly using context or DagRun object
            extract_ti = dag_run.get_task_instance(task_id=task_id_extract)
            transform_ti = dag_run.get_task_instance(task_id=task_id_transform)
            verify_ti = dag_run.get_task_instance(task_id=task_id_verify)

            extract_state = extract_ti.current_state if extract_ti else 'task_not_found'
            transform_state = transform_ti.current_state if transform_ti else 'task_not_found'
            verify_state = verify_ti.current_state if verify_ti else 'task_not_found'

            # Determine status based on task states and verification result
            if extract_state == 'skipped':
                 city_status = "SKIPPED_THRESHOLD"
            elif extract_state != 'success': # Failed, upstream_failed, or other non-success
                 city_status = "FAIL_EXTRACT"
            elif transform_state != 'success':
                 city_status = "FAIL_TRANSFORM"
            elif transform_state == 'success':
                # Extract & Transform OK, check verification XCom (pushed by verify task)
                xcom_key_verify_status = f'verification_status_{city_slug}'
                # Pull verification result from the verify task instance XCom
                # Requires verify task to have run (even if ALL_DONE)
                verification_result = verify_ti.xcom_pull(key=xcom_key_verify_status) if verify_ti else None

                if verification_result is True and verify_state == 'success':
                    city_status = "SUCCESS"
                else: # Verification failed (result=False) or verify task failed/skipped
                    city_status = "FAIL_VERIFY_LOAD"
                    # Log details if verification failed explicitly
                    if verification_result is False:
                        log_etl_event(f"Verification task for {city} succeeded but reported failure (check verify logs).", city, "WARNING")

            else: # Catch unexpected combinations
                city_status = "UNKNOWN_STATE"
                log_etl_event(f"Could not determine status for {city}. States: E={extract_state}, T={transform_state}, V={verify_state}", city, "WARNING")

        except Exception as status_err:
             # Error trying to get task states or determine status
             log_etl_event(f"Error determining final status for city {city}: {status_err}", city, "ERROR")
             city_status = "ERROR_CHECKING_STATUS"

        # Record and count the final determined status
        city_statuses[city] = city_status
        stats[city_status] = stats.get(city_status, 0) + 1 # Increment count

    # --- Calculate Overall Metrics ---
    total_failures_or_errors = sum(v for k, v in stats.items() if k not in ["SUCCESS", "SKIPPED_THRESHOLD"])
    success_rate = (stats["SUCCESS"] / city_count * 100) if city_count > 0 else 0

    # --- Assemble Summary Dictionary ---
    summary = {
        "dag_run_id": dag_run_id,
        "execution_start_iso": run_start_time_epoch and datetime.fromtimestamp(run_start_time_epoch).isoformat(),
        "execution_end_iso": run_end_time_epoch and datetime.fromtimestamp(run_end_time_epoch).isoformat(),
        "execution_time_seconds": execution_time_str,
        "total_cities_configured": city_count,
        "cities_successful_verified": stats["SUCCESS"],
        "cities_skipped_threshold": stats["SKIPPED_THRESHOLD"],
        "cities_failed_extract": stats["FAIL_EXTRACT"],
        "cities_failed_transform": stats["FAIL_TRANSFORM"],
        "cities_failed_load_or_verify": stats["FAIL_VERIFY_LOAD"],
        "cities_failed_unknown_or_error": stats["UNKNOWN_STATE"] + stats["ERROR_CHECKING_STATUS"],
        "total_failures_and_errors": total_failures_or_errors,
        "overall_verified_success_rate_percent": f"{success_rate:.2f}",
        "enabled_storage_methods": STORAGE_METHODS,
        "city_final_statuses": city_statuses # Detailed status per city
    }

    # --- Log and Save Summary ---
    log_etl_event("Pipeline Run Summary:", status="INFO")
    log_etl_event(json.dumps(summary, indent=2), status="INFO")

    try:
        # Ensure log directory exists before writing
        ensure_directory_exists(LOG_DIR_PATH)

        # 1. Save to JSON Lines file (each run is a separate JSON object on a new line)
        summary_jsonl_file = os.path.join(LOG_DIR_PATH, "pipeline_run_summaries.jsonl")
        with open(summary_jsonl_file, 'a', encoding='utf-8') as f_jsonl:
            # Use compact JSON for file, ensure_ascii=False preserves unicode characters
            f_jsonl.write(json.dumps(summary, separators=(',', ':'), ensure_ascii=False) + "\n")

        # 2. Save key metrics to a CSV file for easier analysis
        summary_csv_file = os.path.join(LOG_DIR_PATH, "pipeline_run_summary_history.csv")
        # Define header explicitly to control order and included fields
        csv_header = [
             "dag_run_id", "execution_start_iso", "execution_end_iso", "execution_time_seconds",
             "total_cities_configured", "cities_successful_verified", "cities_skipped_threshold",
             "cities_failed_extract", "cities_failed_transform", "cities_failed_load_or_verify",
             "cities_failed_unknown_or_error", "total_failures_and_errors",
             "overall_verified_success_rate_percent" # Avoid putting the nested dict 'city_final_statuses' here
        ]
        # Prepare row data as a dictionary matching the header
        csv_row = {h: summary.get(h) for h in csv_header}

        file_exists = os.path.exists(summary_csv_file)
        with open(summary_csv_file, 'a', newline='', encoding='utf-8') as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames=csv_header)
            if not file_exists or os.path.getsize(summary_csv_file) == 0:
                writer.writeheader() # Write header only once
            writer.writerow(csv_row) # Write the summary data

        log_etl_event(f"Pipeline summary saved to JSONL and CSV files in {LOG_DIR_PATH}")

    except Exception as e:
        log_etl_event(f"Error saving pipeline summary files: {str(e)}", status="ERROR")

    # --- Push Summary to XComs ---
    # This allows downstream tasks (if any) or external systems to access the summary
    ti.xcom_push(key="pipeline_summary", value=summary)
    log_etl_event("Pipeline summary pushed to XComs key 'pipeline_summary'.")

    return summary # Return the dictionary


# ----------------------------------------------------------------------------
# DAG Definition
# ----------------------------------------------------------------------------

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',                          # The owner of the DAG
    'depends_on_past': False,                    # Tasks do not depend on the success of their previous run
    'email': [NOTIFICATION_EMAIL],               # Default recipient list for email alerts
    'email_on_failure': False,                   # Set True to automatically email on task failure (requires SMTP setup)
    'email_on_retry': False,                     # Set True to automatically email on task retry
    'retries': 1,                                # Default number of retries for failed tasks
    'retry_delay': timedelta(minutes=2),         # Default delay between retries
    'execution_timeout': timedelta(minutes=15),  # Max time allowed for a single task instance execution
    'start_date': days_ago(1),                   # The date from which the DAG should start running (e.g., yesterday)
}

# Instantiate the DAG object
dag = DAG(
    dag_id='weather_etl_pipeline_v2', # Unique identifier for the DAG
    default_args=default_args,        # Apply the default arguments
    description='Hourly ETL pipeline for weather data from OpenWeatherMap to Local/S3/CSV.', # DAG description in UI
    schedule_interval=timedelta(hours=1), # How often the DAG should run (e.g., hourly)
    catchup=False,                    # If True, runs for past missed schedules; False runs only from latest schedule
    tags=['weather', 'etl', 'api', 's3', 'local', 'tutorial'], # Tags for filtering/organizing in UI
    max_active_runs=1,                # Max number of concurrent DAG runs allowed
)

# ----------------------------------------------------------------------------
# Task Group Factory Function
# ----------------------------------------------------------------------------

def create_city_task_group(city: str, dag_instance: DAG) -> TaskGroup:
    """
    Creates a TaskGroup containing all ETL tasks for a single specified city.
    Handles branching based on error threshold, extraction, transformation,
    loading to configured destinations, sensing outputs, and verification.

    Args:
        city (str): The name of the city for which to create tasks.
        dag_instance (DAG): The DAG object to which tasks should be added.

    Returns:
        TaskGroup: The TaskGroup object containing the city's processing tasks.
    """
    city_slug = city.lower().replace(" ", "_") # Create a safe task ID component from city name
    group_id = f'process_{city_slug}'        # Unique ID for the TaskGroup

    # Use a TaskGroup context manager to encapsulate city-specific tasks
    with TaskGroup(group_id=group_id, tooltip=f"ETL pipeline tasks for {city}", dag=dag_instance) as city_processing_group:

        # --- Task Definitions within the Group ---

        # 1. Branching Logic Function
        # This Python callable decides the next task based on the error threshold.
        def decide_branch_path(city_name_branch: str, **context_branch) -> str:
             """
             Called by BranchPythonOperator. Checks error threshold and returns the
             fully qualified task_id of the next task to execute ('skip' or 'extract').
             """
             task_prefix = city_name_branch.lower().replace(" ", "_")
             current_group_id = f'process_{task_prefix}' # Ensure this matches the TaskGroup ID

             # check_error_threshold returns True if threshold is met (should skip)
             if check_error_threshold(city_name_branch, **context_branch):
                  # Return the *fully qualified* task ID for the skip task
                  return f"{current_group_id}.skip_{task_prefix}"
             else:
                  # Return the *fully qualified* task ID for the extract task
                  return f"{current_group_id}.extract_weather_{task_prefix}"

        # 2. Branch Operator
        # Executes the decide_branch_path function and directs flow based on returned task ID.
        branch_on_error_threshold = BranchPythonOperator(
            task_id=f'branch_on_error_{city_slug}', # Task ID within the group
            python_callable=decide_branch_path,     # The decision function
            op_kwargs={'city_name_branch': city},  # Pass city name to the callable
            provide_context=True,                   # Make context available to callable
            dag=dag_instance                        # Explicitly assign task to the DAG
        )

        # 3. Skip Task (Dummy)
        # A placeholder task to branch to if the error threshold is met.
        skip_city = DummyOperator(
            task_id=f'skip_{city_slug}',
            dag=dag_instance # Explicitly assign task to the DAG
        )

        # 4. Extract Task (PythonOperator)
        # Executes the extract_weather_data function. Only runs if branched here.
        extract_task = PythonOperator(
            task_id=f'extract_weather_{city_slug}',
            python_callable=extract_weather_data, # Function to execute
            op_kwargs={'city_name': city},        # Arguments for the function
            #provide_context=True,                  # Provide context (like 'ti')
            dag=dag_instance                       # Explicitly assign task to the DAG
        )

        # 5. Transform Task (PythonOperator)
        # Executes transform_weather_data. Depends on extract_task success (default trigger).
        transform_task = PythonOperator(
            task_id=f'transform_weather_{city_slug}',
            python_callable=transform_weather_data,
            op_kwargs={'city_name': city},
            #provide_context=True,
            dag=dag_instance # Explicitly assign task to the DAG
        )

        # --- Dynamic Load and Sense Tasks based on STORAGE_METHODS ---
        load_tasks = []           # Keep track of load tasks
        sensor_tasks = []         # Keep track of sensor tasks dependent on loads

        # --- LOCAL Storage Tasks ---
        if "LOCAL" in STORAGE_METHODS:
            load_local = PythonOperator(
                task_id=f'load_json_local_{city_slug}',
                python_callable=load_weather_data_to_json,
                op_kwargs={'city_name': city, 'storage_type': 'LOCAL'},
                #provide_context=True,
                dag=dag_instance
            )
            sense_local = FileSensor(
                task_id=f'sense_local_file_{city_slug}',
                # Templated filepath: pulls path pushed to XCom by load_local task
                filepath=f"{{{{ ti.xcom_pull(task_ids='{group_id}.load_json_local_{city_slug}', key='local_json_path_{city_slug}') }}}}",
                fs_conn_id='fs_default',      # Use default filesystem connection
                mode=SENSOR_MODE,             # poke or reschedule
                poke_interval=LOCAL_SENSOR_POKE_INTERVAL,
                timeout=SENSOR_TIMEOUT,
                soft_fail=True,               # If sensor times out, don't fail DAG run, let verify task check
                dag=dag_instance
            )
            # Define dependency: Transform -> Load Local -> Sense Local
            transform_task >> load_local >> sense_local
            load_tasks.append(load_local)
            sensor_tasks.append(sense_local)

        # --- S3 Storage Tasks ---
        if "S3" in STORAGE_METHODS:
            load_s3 = PythonOperator(
                task_id=f'load_json_s3_{city_slug}',
                python_callable=load_weather_data_to_json,
                op_kwargs={'city_name': city, 'storage_type': 'S3'},
                #provide_context=True,
                dag=dag_instance
            )
            sense_s3 = S3KeySensor(
                task_id=f'sense_s3_key_{city_slug}',
                # Templated S3 key: pulls key pushed to XCom by load_s3 task
                bucket_key=f"{{{{ ti.xcom_pull(task_ids='{group_id}.load_json_s3_{city_slug}', key='s3_json_key_{city_slug}') }}}}",
                bucket_name=S3_BUCKET_NAME,   # Global config
                aws_conn_id=AWS_CONN_ID,      # Global config
                mode=SENSOR_MODE,
                poke_interval=S3_SENSOR_POKE_INTERVAL,
                timeout=SENSOR_TIMEOUT,
                soft_fail=True,               # Don't fail DAG if S3 key not found after timeout
                dag=dag_instance
            )
            # Define dependency: Transform -> Load S3 -> Sense S3
            transform_task >> load_s3 >> sense_s3
            load_tasks.append(load_s3)
            sensor_tasks.append(sense_s3)

        # --- CSV Storage Tasks ---
        if "CSV" in STORAGE_METHODS:
            load_csv = PythonOperator(
                task_id=f'load_csv_{city_slug}',
                python_callable=load_weather_data_to_csv,
                op_kwargs={'city_name': city},
                #provide_context=True,
                dag=dag_instance
            )
            sense_csv = FileSensor(
                task_id=f'sense_csv_file_{city_slug}',
                 # Templated filepath: pulls path pushed to XCom by load_csv task
                filepath=f"{{{{ ti.xcom_pull(task_ids='{group_id}.load_csv_{city_slug}', key='csv_path_{city_slug}') }}}}",
                fs_conn_id='fs_default',
                mode=SENSOR_MODE,
                poke_interval=LOCAL_SENSOR_POKE_INTERVAL,
                timeout=SENSOR_TIMEOUT,
                soft_fail=True,               # Don't fail DAG if CSV file not found after timeout
                dag=dag_instance
            )
            # Define dependency: Transform -> Load CSV -> Sense CSV
            transform_task >> load_csv >> sense_csv
            load_tasks.append(load_csv)
            sensor_tasks.append(sense_csv)

        # 6. Verification Task (PythonOperator)
        # Executes the verify_data_load function. Should run after loads/sensors complete.
        # Trigger rule ALL_DONE ensures it runs even if sensors soft-failed.
        verify_task = PythonOperator(
            task_id=f'verify_data_{city_slug}',
            python_callable=verify_data_load,
            op_kwargs={'city_name': city},
            #provide_context=True,
            # Run this task even if upstream sensors soft-failed, to report final status
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag_instance # Explicitly assign task to the DAG
        )

        # --- Define Final Dependencies within the Group ---
        # Branch operator directs flow either to skip_city or extract_task
        branch_on_error_threshold >> [skip_city, extract_task]
        # Transform depends on extract_task succeeding (default trigger rule = ALL_SUCCESS)
        extract_task >> transform_task

        # Verification depends on all sensor tasks completing (successfully or via soft_fail)
        # This ensures verification runs only after attempts to load/sense data are finished.
        if sensor_tasks: # Only set dependency if sensors were created
             sensor_tasks >> verify_task
        # If no storage methods are enabled, 'sensor_tasks' will be empty,
        # and 'verify_task' won't have any upstream dependencies within this group.
        # Consider adding a dummy 'no_load_tasks' if explicit path needed when STORAGE_METHODS is empty.

    # Return the constructed TaskGroup object to be used in the main DAG flow
    return city_processing_group


# ----------------------------------------------------------------------------
# Main DAG Workflow Tasks (outside TaskGroups)
# ----------------------------------------------------------------------------

# 1. Start Marker
start_pipeline = DummyOperator(task_id='start_pipeline', dag=dag)

# 2. Record Pipeline Start Time
record_start = PythonOperator(
    task_id='record_start_time',
    python_callable=record_start_time,
    #provide_context=True, # Needed to access 'ti' for XCom push
    dag=dag,
)

# 3. Initialize Local Directories
init_dirs = PythonOperator(
    task_id='initialize_local_directories',
    python_callable=initialize_directories,
    dag=dag,
    retries=0 # Directory creation usually fails definitively (permissions), retries often won't help
)

# 4. Check API Key Variable Existence (fail early if not configured)
check_api_key_var = PythonOperator(
     task_id='check_api_key_variable',
     python_callable=get_api_key, # Re-use function which raises AirflowFailException if missing
     retries=0,                  # No point retrying if Variable doesn't exist
     dag=dag,
)

# 5. Check OpenWeatherMap API Connectivity (Sensor)
api_connectivity_sensor = HttpSensor(
    task_id='check_openweathermap_api_endpoint',
    http_conn_id=OPENWEATHERMAP_CONN_ID, # Use the configured HTTP connection
    endpoint=API_ENDPOINT,             # The API endpoint to check
    request_params={                   # Params for a valid basic check request
        'q': 'London',                 # Use a known reliable city
        'appid': "{{ var.value." + OPENWEATHERMAP_API_KEY_VAR + " }}", # Template API key from Variable
        'units': 'metric'
    },
    response_check=lambda response: response.ok, # Success if status code is 2xx
    poke_interval=60,                            # Check every 60 seconds
    timeout=300,                                 # Fail after 5 minutes if no success
    mode=SENSOR_MODE,                            # 'poke' or 'reschedule'
    soft_fail=False,                             # HARD FAIL the DAG if API is unreachable at the start
    dag=dag,
)

# 6. Check S3 Connectivity (PythonOperator)
s3_connectivity_check = PythonOperator(
    task_id='check_s3_connectivity',
    python_callable=check_s3_connectivity,
    #provide_context=False, # Context not strictly needed here
    # Failures are logged within the function, but task succeeds to allow progression
    dag=dag,
)

# 7. Create and Link City Task Groups in Parallel
# List to hold prerequisite tasks for city groups
setup_tasks = [api_connectivity_sensor, s3_connectivity_check, init_dirs, check_api_key_var]
city_task_groups = [] # List to hold the created task group objects

for city in CITIES:
    # Create the TaskGroup for the current city using the factory function
    city_group = create_city_task_group(city=city, dag_instance=dag)
    # Define dependency: Each city group starts after all setup tasks are complete
    setup_tasks >> city_group
    city_task_groups.append(city_group)

# 8. Join Point Marker (after all cities)
# This Dummy task ensures downstream tasks only run after all parallel city groups finish
all_cities_processed = DummyOperator(
    task_id='all_cities_processed',
    # Run this task even if some city groups failed or were skipped
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)
# Set dependency: all_cities_processed runs after all city_task_groups are done
city_task_groups >> all_cities_processed

# 9. Cleanup Task
# Runs after all city processing attempts are complete.
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_files,
    #provide_context=True,              # Pass context if cleanup needs run_id etc.
    trigger_rule=TriggerRule.ALL_DONE, # Run cleanup even if upstream failed
    dag=dag,
)

# 10. Summary Task
# Creates the final summary report. Runs after cleanup.
summary_task = PythonOperator(
    task_id='create_pipeline_summary',
    python_callable=create_pipeline_summary,
    #provide_context=True,              # Context needed to access TIs, DAG run info
    trigger_rule=TriggerRule.ALL_DONE, # Generate summary regardless of upstream outcome
    dag=dag,
)

# 11. Performance Metrics Calculation Task
# Calculates duration using start time from XComs. Runs after summary.
performance_metrics_task = PythonOperator(
    task_id='calculate_performance_metrics',
    python_callable=calculate_performance_metrics,
    #provide_context=True,              # Context needed for 'ti', 'run_id'
    trigger_rule=TriggerRule.ALL_DONE, # Run regardless of upstream outcome
    dag=dag,
)

# 12. End Marker
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    trigger_rule=TriggerRule.ALL_DONE, # Final marker
    dag=dag,
)

# ----------------------------------------------------------------------------
# Define Main DAG Dependencies (Inter-TaskGroup/Top-Level Flow)
# ----------------------------------------------------------------------------

# Setup phase dependencies (already defined during task creation)
start_pipeline >> record_start # Record time right after start
# Prerequisite checks depend on initialization tasks completing:
record_start >> init_dirs # Initialize after recording start time
init_dirs >> check_api_key_var # Check key after init
check_api_key_var >> api_connectivity_sensor # Check API only if key var exists
init_dirs >> s3_connectivity_check # Check S3 in parallel with API check

# Teardown phase dependencies
all_cities_processed >> cleanup_task >> summary_task >> performance_metrics_task >> end_pipeline

# ----------------------------------------------------------------------------
# DAG Testing Support (Optional)
# ----------------------------------------------------------------------------
# This block allows running `python <dag_file>.py` for basic Airflow CLI checks locally,
# primarily for testing DAG parsing without needing a full Airflow environment.
if __name__ == "__main__":
     # This triggers the Airflow CLI DAG parsing/testing interface if script is run directly
     dag.cli()