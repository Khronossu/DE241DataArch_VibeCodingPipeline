import logging
import pandas as pd

logger = logging.getLogger(__name__)


def transform_taxi_data(**context):
    """
    Transform cleaned NYC taxi data by computing derived features,
    filtering outliers, and saving the result.

    This function is designed to be used with an Airflow PythonOperator.

    Steps:
        1. Pull the clean CSV path from XCom (key="clean_path", task_id="clean_taxi_data")
        2. Compute derived features: trip_duration_minutes, speed_mph, fare_per_mile,
           pickup_hour, pickup_day_of_week, is_weekend
        3. Filter out trips with speed_mph > 80 or trip_duration_minutes < 1
        4. Save transformed data to /tmp/nyc_taxi_transformed.csv
        5. Log descriptive statistics for the new columns
    """
    # Step 1: Pull the clean CSV path from XCom
    ti = context["ti"]
    clean_path = ti.xcom_pull(task_ids="clean_taxi_data", key="clean_path")

    if clean_path is None:
        raise ValueError(
            "No clean_path found in XCom for task_id='clean_taxi_data'. "
            "Ensure the upstream task pushed the value with key='clean_path'."
        )

    logger.info("Loading cleaned taxi data from: %s", clean_path)

    # Step 2: Load with pandas and compute derived features
    df = pd.read_csv(clean_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

    logger.info("Loaded %d rows from cleaned data.", len(df))

    # trip_duration_minutes: difference between pickup and dropoff datetimes
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60.0
    )

    # speed_mph: trip_distance / (trip_duration_minutes / 60)
    # Guard against division by zero; will be filtered out later anyway
    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60.0)

    # fare_per_mile: fare_amount / trip_distance
    # Use replace to handle division by zero gracefully
    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"].replace(0, float("nan"))

    # pickup_hour: hour extracted from tpep_pickup_datetime
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour

    # pickup_day_of_week: day of week (0=Monday)
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek

    # is_weekend: boolean True if pickup_day_of_week >= 5
    df["is_weekend"] = df["pickup_day_of_week"] >= 5

    logger.info("Derived features computed successfully.")

    # Step 3: Filter out trips with speed_mph > 80 or trip_duration_minutes < 1
    rows_before = len(df)

    mask_valid = (df["speed_mph"] <= 80) & (df["trip_duration_minutes"] >= 1)
    # Also keep rows where speed_mph might be NaN/inf due to zero duration,
    # but those are already handled by trip_duration_minutes < 1 filter
    df = df[mask_valid].copy()

    rows_after = len(df)
    rows_removed = rows_before - rows_after
    logger.info(
        "Filtered out %d rows (speed_mph > 80 or trip_duration_minutes < 1). "
        "Remaining: %d rows.",
        rows_removed,
        rows_after,
    )

    # Step 4: Save transformed data to /tmp/nyc_taxi_transformed.csv
    output_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(output_path, index=False)
    logger.info("Transformed data saved to: %s", output_path)

    # Step 5: Log descriptive statistics for the new columns
    new_columns = [
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
    ]

    stats = df[new_columns].describe(include="all")
    logger.info("Descriptive statistics for derived features:\n%s", stats.to_string())

    # Push the output path to XCom for downstream tasks
    ti.xcom_push(key="transformed_path", value=output_path)

    return output_path