import requests
import os
import json
import datetime
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery

# --- Configuration ---
SERVICE_ACCOUNT_KEY_PATH = ""
PROJECT_ID = 
GCS_BUCKET_NAME = 

BIGQUERY_DATASET_ID = 'nba_raw_data' # BigQuery Dataset where all tables will be created

# The base URL for the NBA API from your provided swagger documentation
NBA_API_BASE_URL = 'https://api.server.nbaapi.com'

# --- API Endpoints and BigQuery Table Mappings ---
# For /api/games
NBA_API_ENDPOINT_GAMES = '/api/games'
BIGQUERY_TABLE_ID_GAMES = 'api_games_raw'

# For /api/playertotals
NBA_API_ENDPOINT_PLAYERTOTALS = '/api/playertotals'
BIGQUERY_TABLE_ID_PLAYERTOTALS = 'api_playertotals_raw'

# For /api/playeradvancedstats
NBA_API_ENDPOINT_PLAYERADVANCEDSTATS = '/api/playeradvancedstats'
BIGQUERY_TABLE_ID_PLAYERADVANCEDSTATS = 'api_playeradvancedstats_raw'

# For /api/playershotchart
NBA_API_ENDPOINT_PLAYERSHOTCHART = '/api/playershotchart'
BIGQUERY_TABLE_ID_PLAYERSHOTCHART = 'api_playershotchart_raw'


def authenticate_gcp():
    """Authenticates with Google Cloud using the service account key."""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_KEY_PATH,
            scopes=[
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/devstorage.full_control', # GCS access
                'https://www.googleapis.com/auth/bigquery' # BigQuery access
            ]
        )
        print("Successfully loaded Google Cloud service account credentials.")
        return credentials
    except FileNotFoundError:
        print(f"Error: Service account key file not found at {SERVICE_ACCOUNT_KEY_PATH}")
        print("Please update SERVICE_ACCOUNT_KEY_PATH in the script.")
        return None
    except Exception as e:
        print(f"Error loading service account credentials: {e}")
        return None

def fetch_nba_data(endpoint, params=None):
    """
    Makes calls to the NBA API endpoint with given parameters, handling pagination,
    and returns a combined list of all JSON responses from all pages.
    """
    all_data = []
    page = 1
    has_more_pages = True

    # Ensure params is a mutable dictionary
    if params is None:
        params = {}
    else:
        params = params.copy() # Make a copy to avoid modifying the original dict

    while has_more_pages:
        params['page'] = page # Add or update the page parameter
        full_api_url = f"{NBA_API_BASE_URL}{endpoint}"
        print(f"\nCalling NBA API: {full_api_url} with parameters: {params}")

        try:
            response = requests.get(full_api_url, params=params)
            response.raise_for_status()
            print(f"API Call successful! Status Code: {response.status_code}")
            json_response = response.json()

            if 'data' in json_response and isinstance(json_response['data'], list):
                all_data.extend(json_response['data']) # Add data from current page
            elif isinstance(json_response, list):

                 all_data.extend(json_response)
            else:
                 if page == 1 and not all_data:
                     all_data.append(json_response)
                     print("Warning: API response does not contain a 'data' list. Appending entire response as a single item.")

            # Check for pagination info to determine if there are more pages
            if 'pagination' in json_response and isinstance(json_response['pagination'], dict):
                # Corrected keys based on your API's Swagger UI output
                current_page_from_api = json_response['pagination'].get('page', 1)
                total_pages_from_api = json_response['pagination'].get('pages', current_page_from_api)

                if current_page_from_api < total_pages_from_api:
                    page += 1
                else:
                    has_more_pages = False
            else:
                # If no pagination info, assume it's a single page response
                has_more_pages = False

        except requests.exceptions.RequestException as e:
            print(f"Error calling NBA API at {full_api_url}: {e}")
            has_more_pages = False # Stop trying if there's an error
            return None # Return None immediately if there's an error
        except json.JSONDecodeError:
            print("Error: API response is not valid JSON.")
            response_content = response.text[:500] if hasattr(response, 'text') else 'No response content available.'
            print(f"Response content: {response_content}...")
            has_more_pages = False # Stop trying if there's a JSON decode error
            return None # Return None immediately if there's an error

    print(f"Fetched {len(all_data)} records across all pages for endpoint {endpoint}.")
    return all_data

def upload_to_gcs(credentials, data, bucket_name, filename):
    """Uploads JSON data to a Google Cloud Storage bucket in NDJSON format."""
    print(f"\nUploading data to GCS bucket: {bucket_name}/{filename}")
    try:
        storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        if isinstance(data, list):
            ndjson_data = "\n".join(json.dumps(record) for record in data)
        else:
            ndjson_data = json.dumps(data)

        # Upload the NDJSON string
        blob.upload_from_string(ndjson_data, content_type='application/json')

        print(f"Successfully uploaded {filename} to gs://{bucket_name}/{filename}")
        return f"gs://{bucket_name}/{filename}"
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return None

def create_bigquery_dataset_if_not_exists(credentials, dataset_id):
    """Creates a BigQuery dataset if it doesn't already exist."""
    print(f"\nChecking/creating BigQuery dataset: {dataset_id}")
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except Exception as e:
        if e.code == 404: # Not found error
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US" 
            client.create_dataset(dataset)
            print(f"Dataset '{dataset_id}' created successfully.")
        else:
            print(f"Error checking/creating dataset '{dataset_id}': {e}")

def load_json_to_bigquery(credentials, gcs_uri, dataset_id, table_id):
    """Loads JSON data from GCS into a BigQuery table."""
    print(f"\nLoading data from GCS ({gcs_uri}) to BigQuery table: {dataset_id}.{table_id}")
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True, # Automatically detects schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
    )
    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        print(f"Starting BigQuery load job: {load_job.job_id}")
        load_job.result()  # Wait for the job to complete
        print("BigQuery load job completed.")
        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}.")
        return True
    except Exception as e:
        print(f"Error loading data to BigQuery: {e}")
        return False

def standardize_keys_to_lowercase(data):
    """
    Recursively converts all dictionary keys in the given data structure to lowercase.
    Handles nested dictionaries and lists of dictionaries.
    """
    if isinstance(data, dict):
        return {k.lower(): standardize_keys_to_lowercase(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [standardize_keys_to_lowercase(elem) for elem in data]
    else:
        return data

if __name__ == "__main__":
    # 1. Authenticate with Google Cloud
    gcp_credentials = authenticate_gcp()
    if not gcp_credentials:
        print("Google Cloud authentication failed. Exiting.")
        exit()

    # Ensure BigQuery dataset exists
    create_bigquery_dataset_if_not_exists(gcp_credentials, BIGQUERY_DATASET_ID)

    # --- Process NBA Games Data ---
    print("\n--- Processing NBA Games Data ---")
    games_params = {} # Using empty parameters for broader data pull
    nba_games_data = fetch_nba_data(NBA_API_ENDPOINT_GAMES, games_params)
    if nba_games_data:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_games_filename = f"nba_api_games_data_{timestamp}.json"
        gcs_games_uri = upload_to_gcs(gcp_credentials, nba_games_data, GCS_BUCKET_NAME, gcs_games_filename)
        if gcs_games_uri:
            success_games = load_json_to_bigquery(gcp_credentials, gcs_games_uri, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID_GAMES)
            if success_games:
                print(f"NBA Games data successfully ingested into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_GAMES}'.")
            else:
                print(f"Failed to load NBA Games data into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_GAMES}'.")
        else:
            print("Failed to upload NBA Games data to GCS.")
    else:
        print("No NBA Games data found from API or API call failed.")

    # --- Process Player Totals Data ---
    print("\n--- Processing Player Totals Data ---")
    playertotals_params = {} # Using empty parameters for broader data pull
    nba_playertotals_data = fetch_nba_data(NBA_API_ENDPOINT_PLAYERTOTALS, playertotals_params)
    if nba_playertotals_data:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_playertotals_filename = f"nba_api_playertotals_data_{timestamp}.json"
        gcs_playertotals_uri = upload_to_gcs(gcp_credentials, nba_playertotals_data, GCS_BUCKET_NAME, gcs_playertotals_filename)
        if gcs_playertotals_uri:
            success_playertotals = load_json_to_bigquery(gcp_credentials, gcs_playertotals_uri, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID_PLAYERTOTALS)
            if success_playertotals:
                print(f"Player Totals data successfully ingested into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERTOTALS}'.")
            else:
                print(f"Failed to load Player Totals data into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERTOTALS}'.")
        else:
            print("Failed to upload Player Totals data to GCS.")
    else:
        print("No Player Totals data found from API or API call failed.")

    # --- Process Player Advanced Stats Data ---
    print("\n--- Processing Player Advanced Stats Data ---")
    playeradvancedstats_params = {} # Using empty parameters for broader data pull
    nba_playeradvancedstats_data = fetch_nba_data(NBA_API_ENDPOINT_PLAYERADVANCEDSTATS, playeradvancedstats_params)
    if nba_playeradvancedstats_data:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_playeradvancedstats_filename = f"nba_api_playeradvancedstats_data_{timestamp}.json"
        gcs_playeradvancedstats_uri = upload_to_gcs(gcp_credentials, nba_playeradvancedstats_data, GCS_BUCKET_NAME, gcs_playeradvancedstats_filename)
        if gcs_playeradvancedstats_uri:
            success_playeradvancedstats = load_json_to_bigquery(gcp_credentials, gcs_playeradvancedstats_uri, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID_PLAYERADVANCEDSTATS)
            if success_playeradvancedstats:
                print(f"Player Advanced Stats data successfully ingested into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERADVANCEDSTATS}'.")
            else:
                print(f"Failed to load Player Advanced Stats data into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERADVANCEDSTATS}'.")
        else:
            print("Failed to upload Player Advanced Stats data to GCS.")
    else:
        print("No Player Advanced Stats data found from API or API call failed.")

    # --- Process Player Shot Chart Data ---
    print("\n--- Processing Player Shot Chart Data ---")
    playershotchart_params = {} # Using empty parameters for broader data pull
    nba_playershotchart_data = fetch_nba_data(NBA_API_ENDPOINT_PLAYERSHOTCHART, playershotchart_params)

    if nba_playershotchart_data:
        print("Standardizing Player Shot Chart data keys to lowercase...")
        processed_nba_playershotchart_data = standardize_keys_to_lowercase(nba_playershotchart_data)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_playershotchart_filename = f"nba_api_playershotchart_data_{timestamp}.json"
        # Pass the processed data to upload_to_gcs
        gcs_playershotchart_uri = upload_to_gcs(gcp_credentials, processed_nba_playershotchart_data, GCS_BUCKET_NAME, gcs_playershotchart_filename)
        if gcs_playershotchart_uri:
            success_playershotchart = load_json_to_bigquery(gcp_credentials, gcs_playershotchart_uri, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID_PLAYERSHOTCHART)
            if success_playershotchart:
                print(f"Player Shot Chart data successfully ingested into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERSHOTCHART}'.")
            else:
                print(f"Failed to load Player Shot Chart data into BigQuery table '{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID_PLAYERSHOTCHART}'.")
        else:
            print("Failed to upload Player Shot Chart data to GCS.")
    else:
        print("No Player Shot Chart data found from API or API call failed.")

    print("\n--- All NBA API data ingestion attempts complete ---")