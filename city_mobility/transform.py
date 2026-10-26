import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# Here we define which string columns represent categories across each dataset.
# Standardizing these will prevent duplicate categories caused by extra spaces or mixed casing.
CATEGORY_COLUMNS = {
    "stations": ["district"],
    "bikes": ["bike_type", "status"],
    "users": ["membership", "home_district"],
    "trips": ["trip_status"],
    "maintenance": ["issue_type", "resolution"]
}


# Mapping out columns that hold numerical data so we can attempt safe numeric conversions later.
NUMERIC_COLUMNS = {
    "stations": ["capacity"],
    "bikes": ["purchase_year"],
    "trips": ["duration_minutes"],
    "maintenance": ["repair_minutes"]
}


# Mapping out columns that store dates or timestamps so they can be parsed into actual temporal types.
DATE_COLUMNS = {
    "users": ["signup_date"],
    "trips": ["start_time", "end_time"],
    "maintenance": ["reported_at"]
}


def standardize_strings(df):
    # Iterating through every column to strip out leading/trailing whitespace.
    # If a string turns out to be completely empty after trimming, we convert it to a true Null value
    # to ensure missing data is treated consistently across the board.
    for column_name in df.columns:
        cleaned_value = F.trim(F.col(column_name).cast("string"))

        df = df.withColumn(
            column_name,
            F.when(
                cleaned_value == "",
                F.lit(None).cast("string")
            ).otherwise(cleaned_value)
        )

    return df


def standardize_categories(df, column_names):
    # Converts categorical fields to lowercase and trims extra spaces.
    # This keeps values uniform so that entries like 'Annual', 'ANNUAL', or 'annual' count as the same group.
    for column_name in column_names:
        df = df.withColumn(
            column_name,
            F.lower(F.trim(F.col(column_name)))
        )

    return df


def add_parsed_columns(df, dataset_name):
    # Instead of replacing original raw values, we append new parsed columns (with a _parsed suffix).
    # This design lets us evaluate malformed records down the line without overwriting the original input data.

    # Safely try converting raw numeric strings to Double precision floats.
    for column_name in NUMERIC_COLUMNS.get(dataset_name, []):
        df = df.withColumn(
            f"{column_name}_parsed",
            F.expr(f"try_cast(`{column_name}` AS DOUBLE)")
        )

    # Safely parse date and timestamp strings depending on the field type.
    for column_name in DATE_COLUMNS.get(dataset_name, []):
        if column_name in ["start_time", "end_time", "reported_at"]:
            # Parse full timestamps (date + time).
            df = df.withColumn(
                f"{column_name}_parsed",
                F.expr(f"try_to_timestamp(`{column_name}`)")
            )
        else:
            # Parse simple date values (like signup_date).
            df = df.withColumn(
                f"{column_name}_parsed",
                F.expr(f"try_cast(`{column_name}` AS DATE)")
            )

    return df


def transform_dataset(dataset_name, df):
    # Runs the full sequence of data cleaning for a single dataset:
    # 1. Clean general string spacing and null handling.
    # 2. Normalize categorical columns to lowercase.
    # 3. Create typed helper columns for numbers and dates.
    df = standardize_strings(df)

    df = standardize_categories(
        df,
        CATEGORY_COLUMNS.get(dataset_name, [])
    )

    df = add_parsed_columns(df, dataset_name)

    logger.info("Transformation completed for dataset: %s", dataset_name)

    return df


def transform_datasets(datasets):
    # Entry point function to loop through a dictionary of DataFrames and apply
    # the transformation logic to each dataset systematically.
    transformed_datasets = {}

    for dataset_name, df in datasets.items():
        transformed_datasets[dataset_name] = transform_dataset(
            dataset_name,
            df
        )

    logger.info(
        "Transformation completed for %s datasets",
        len(transformed_datasets)
    )

    return transformed_datasets