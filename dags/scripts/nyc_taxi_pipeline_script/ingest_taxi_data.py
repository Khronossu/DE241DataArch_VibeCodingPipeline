import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def ingest_taxi_data(**context):
    # If using the 2018 data link you mentioned earlier, swap this URL to:
    url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    # url = "https://data.cityofnewyork.us/api/views/4c9mwp7t/rows.csv"
    #  url = "https://data.cityofnewyork.us/api/views/t29m-gskq/rows.csv"
    file_path = "/tmp/nyc_taxi_raw.csv"

    # Configure retry logic for connection errors (3 attempts)
    session = requests.Session()
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # Stream the download and limit to exactly 1001 lines (1 header + 1000 data rows)
    logging.info(f"Starting stream download from {url} (Limiting to 1000 rows)")
    with session.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, 'wb') as f:
            lines_written = 0
            # iter_lines() is highly efficient and prevents loading the whole 120M row file into memory
            for line in response.iter_lines():
                if line:
                    f.write(line + b'\n')
                    lines_written += 1
                    if lines_written >= 1001:
                        break

    # Validate row count using pandas
    logging.info("Validating downloaded file...")
    df = pd.read_csv(file_path)
    row_count = len(df)
    
    # Log the number of rows downloaded
    logging.info(f"Successfully downloaded {row_count} rows.")
    
    # Raise an error if file has less than 1000 rows
    if row_count < 1000:
        raise ValueError(f"Data validation failed: Expected at least 1000 rows, got {row_count}")

    # Push file path to XCom for downstream tasks
    context['ti'].xcom_push(key='raw_path', value=file_path)
    return file_path