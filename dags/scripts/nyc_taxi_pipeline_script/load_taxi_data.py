import logging
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine

def load_taxi_model(**context):
    ti = context['ti']
    
    # 1. Pull the transformed CSV path from XCom
    # transformed_path = ti.xcom_pull(key="transformed_path", task_ids="transform_taxi_data")
    transformed_path = ti.xcom_pull(key="transformed_path", task_ids="transform_taxi_data")
    if not transformed_path:
        raise ValueError("Failed to pull 'transformed_path' from XCom. Did the transform task run successfully?")

    logging.info(f"Loading transformed data from: {transformed_path}")
    
    # 2. Load data with pandas
    df = pd.read_csv(transformed_path)
    logging.info(f"Loaded {len(df)} rows for MySQL ingestion.")

    # 3. Connect to MySQL using Airflow Variables
    try:
        mysql_host = Variable.get("MYSQL_HOST")
        mysql_user = Variable.get("MYSQL_USER")
        mysql_pass = Variable.get("MYSQL_PASS")
        mysql_db = Variable.get("MYSQL_DB")
        
        # Using pymysql driver for SQLAlchemy
        db_url = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
        engine = create_engine(db_url)
        logging.info(f"Successfully connected to MySQL database: {mysql_db} at {mysql_host}")
    except KeyError as e:
        raise ValueError(f"Missing Airflow Variable: {e}. Please add it in the Airflow UI.")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MySQL: {e}")

    # 4. Create Star-Schema Tables
    logging.info("Building dimension and fact tables...")

    # -- dim_time --
    dim_time = df[['pickup_hour', 'pickup_day_of_week', 'is_weekend']].drop_duplicates().reset_index(drop=True)
    dim_time['time_id'] = dim_time.index + 1

    # -- dim_payment --
    # Ensure payment_type exists (handling edge cases if column names slightly differ)
    if 'payment_type' in df.columns:
        dim_payment = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    else:
        # Fallback if the column is missing from this specific TLC dataset month
        dim_payment = pd.DataFrame({'payment_type': [0]}) 
        df['payment_type'] = 0

    dim_payment['payment_id'] = dim_payment.index + 1

    # -- fact_trips --
    # Merge dimensions back to the main dataframe to get the Foreign Keys
    fact_trips = df.merge(dim_time, on=['pickup_hour', 'pickup_day_of_week', 'is_weekend'], how='left')
    fact_trips = fact_trips.merge(dim_payment, on='payment_type', how='left')

    # Select only the relevant fact measures and foreign keys
    fact_cols = [
        'time_id', 'payment_id', 'fare_amount', 'trip_distance', 
        'trip_duration_minutes', 'speed_mph', 'fare_per_mile'
    ]
    # Add passenger_count if it exists in the dataset
    if 'passenger_count' in fact_trips.columns:
        fact_cols.append('passenger_count')
        
    fact_trips = fact_trips[fact_cols]

    # 5 & 6. Load to MySQL and log row counts
    tables_to_load = {
        'dim_time': dim_time,
        'dim_payment': dim_payment,
        'fact_trips': fact_trips
    }

    for table_name, table_df in tables_to_load.items():
        logging.info(f"Loading {len(table_df)} rows into {table_name}...")
        table_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=1000
        )
        logging.info(f"✅ Successfully loaded {table_name}.")

    logging.info("All tables loaded successfully! Pipeline complete.")