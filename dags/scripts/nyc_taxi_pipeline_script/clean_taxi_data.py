import logging
import pandas as pd

def clean_taxi_data(**context):
    ti = context['ti']
    
    # 1. Pull the raw CSV file path from XCom
    raw_path = ti.xcom_pull(key="raw_path", task_id="ingest_taxi_data")
    if not raw_path:
        raise ValueError("Failed to pull 'raw_path' from XCom. Did the ingest task run successfully?")

    logging.info(f"Loading raw data from: {raw_path}")
    
    # 2. Load with pandas
    df = pd.read_csv(raw_path)
    initial_count = len(df)
    logging.info(f"Starting with {initial_count} rows.")

    # 3 & 4. Apply cleaning rules and log removals step-by-step
    
    # Drop rows with nulls in critical columns
    df = df.dropna(subset=['fare_amount', 'trip_distance', 'tpep_pickup_datetime'])
    post_null_count = len(df)
    logging.info(f"Removed {initial_count - post_null_count} rows with null values.")

    # Parse datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    
    # Clean fare amount (0 < fare <= 500)
    pre_fare_count = len(df)
    df = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 500)]
    logging.info(f"Removed {pre_fare_count - len(df)} rows with invalid fare_amount.")

    # Clean trip distance (0 < distance <= 100)
    pre_dist_count = len(df)
    df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 100)]
    logging.info(f"Removed {pre_dist_count - len(df)} rows with invalid trip_distance.")

    # Clean coordinates (NYC bounds)
    # Note: If using 2018 data, ensure these columns exist in your specific CSV, 
    # as the TLC switched to LocationIDs in late 2016 for some datasets.
    if all(col in df.columns for col in ['pickup_latitude', 'dropoff_latitude', 'pickup_longitude', 'dropoff_longitude']):
        pre_coord_count = len(df)
        lat_mask = df['pickup_latitude'].between(40.4, 41.0) & df['dropoff_latitude'].between(40.4, 41.0)
        lon_mask = df['pickup_longitude'].between(-74.3, -73.5) & df['dropoff_longitude'].between(-74.3, -73.5)
        df = df[lat_mask & lon_mask]
        logging.info(f"Removed {pre_coord_count - len(df)} rows with out-of-bounds coordinates.")
    else:
        logging.warning("Coordinate columns not found in dataset. Skipping spatial filtering.")

    final_count = len(df)
    logging.info(f"Cleaning complete. Total rows remaining: {final_count} (Total removed: {initial_count - final_count})")

    # 5. Save cleaned data
    # (If you want this visible locally via Docker, change /tmp/ to /opt/airflow/dags/data/ like earlier!)
    clean_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)
    logging.info(f"Cleaned data saved to {clean_path}")

    # 6. Push cleaned file path to XCom
    ti.xcom_push(key="clean_path", value=clean_path)
    
    return clean_path