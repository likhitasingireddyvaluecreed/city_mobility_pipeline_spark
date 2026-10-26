import logging

from pyspark.sql import functions as F
from pyspark.sql import Window

# Setting up the logger so I can output processing steps and summary logs.
logger = logging.getLogger(__name__)


# I am listing all the required columns for each dataset so I can verify their presence later.
REQUIRED_COLUMNS = {
    "stations": ["station_id", "station_name", "district", "capacity"],
    "bikes": ["bike_id", "bike_type", "status", "purchase_year"],
    "users": ["user_id", "membership", "signup_date"],
    "trips": [
        "trip_id",
        "user_id",
        "bike_id",
        "start_station_id",
        "end_station_id",
        "start_time",
        "end_time",
        "duration_minutes",
        "trip_status"
    ],
    "maintenance": [
        "maintenance_id",
        "bike_id",
        "reported_at",
        "issue_type",
        "repair_minutes"
    ]
}


# I am defining valid numerical limits here to check for out-of-bound values during validation.
NUMERIC_RULES = {
    "stations": {
        "capacity": {"min": 1}
    },
    "bikes": {
        "purchase_year": {"max": 2026}
    },
    "trips": {
        "duration_minutes": {"min": 0}
    },
    "maintenance": {
        "repair_minutes": {"min": 0}
    }
}


# I am identifying columns that should contain valid date and timestamp formats.
DATE_COLUMNS = {
    "users": ["signup_date"],
    "trips": ["start_time", "end_time"],
    "maintenance": ["reported_at"]
}


# I am specifying foreign key links so I can check if referencing IDs actually exist in their parent datasets.
REFERENCE_RULES = {
    "trips": {
        "user_id": ("users", "user_id"),
        "bike_id": ("bikes", "bike_id"),
        "start_station_id": ("stations", "station_id"),
        "end_station_id": ("stations", "station_id")
    },
    "maintenance": {
        "bike_id": ("bikes", "bike_id")
    }
}


def is_missing(column_name):
    # I am cleaning and normalizing the input text first so I can catch null values, empty strings, and common placeholders.
    raw_value = F.lower(F.trim(F.col(column_name).cast("string")))

    return (
        F.col(column_name).isNull()
        | raw_value.isin(
            "",
            "null",
            "none",
            "na",
            "n/a",
            "nan",
            "unknown"
        )
    )


def validate_dataset(dataset_name, df, datasets):
    # I am keeping the original DataFrame intact while appending validation checks onto it.
    checked = df

    # I am creating lists to hold dynamically generated error conditions and helper column names.
    reasons = []
    temporary_columns = []

    # I am identifying the primary key column for the current dataset.
    key_column = {
        "stations": "station_id",
        "bikes": "bike_id",
        "users": "user_id",
        "trips": "trip_id",
        "maintenance": "maintenance_id"
    }[dataset_name]

    # I am counting primary key occurrences using a window function so I can identify duplicate records.
    checked = checked.withColumn(
        "_key_count",
        F.count("*").over(Window.partitionBy(key_column))
    )
    temporary_columns.append("_key_count")

    # I am checking if the primary key itself is missing.
    reasons.append(
        F.when(is_missing(key_column), f"Missing {key_column}")
    )

    # I am checking if non-missing primary keys appear more than once in the dataset.
    reasons.append(
        F.when(
            ~is_missing(key_column) & (F.col("_key_count") > 1),
            f"Duplicate {key_column}"
        )
    )

    # I am looping through all required fields to flag any missing values.
    for column_name in REQUIRED_COLUMNS[dataset_name]:
        if column_name != key_column:
            reasons.append(
                F.when(
                    is_missing(column_name),
                    f"Missing {column_name}"
                )
            )

    # I am iterating through numeric rules and inspecting the parsed versions of these fields.
    for column_name, rules in NUMERIC_RULES.get(
        dataset_name, {}
    ).items():

        parsed_column = f"{column_name}_parsed"

        # I am checking if a field had data but failed numeric parsing.
        reasons.append(
            F.when(
                ~is_missing(column_name)
                & F.col(parsed_column).isNull(),
                f"Malformed {column_name}"
            )
        )

        # I am checking if the parsed number falls below the allowed minimum.
        if "min" in rules:
            reasons.append(
                F.when(
                    F.col(parsed_column) < rules["min"],
                    f"Invalid {column_name}: below minimum"
                )
            )

        # I am checking if the parsed number exceeds the allowed maximum.
        if "max" in rules:
            reasons.append(
                F.when(
                    F.col(parsed_column) > rules["max"],
                    f"Invalid {column_name}: above maximum"
                )
            )

    # I am verifying that date fields containing data were successfully converted into valid dates/timestamps.
    for column_name in DATE_COLUMNS.get(dataset_name, []):
        parsed_column = f"{column_name}_parsed"

        reasons.append(
            F.when(
                ~is_missing(column_name)
                & F.col(parsed_column).isNull(),
                f"Malformed {column_name}"
            )
        )

    # I am looping through reference rules to verify that foreign keys exist in their parent tables.
    for index, (column_name, reference) in enumerate(
        REFERENCE_RULES.get(dataset_name, {}).items()
    ):
        parent_dataset, parent_key = reference
        flag_column = f"_reference_exists_{index}"
        
        # I am extracting distinct, non-missing IDs from the parent table to build a lookup reference.
        parent_ids = (
            datasets[parent_dataset]
            .select(parent_key)
            .filter(~is_missing(parent_key))
            .distinct()
            .withColumnRenamed(parent_key, f"_parent_key_{index}")
            .withColumn(flag_column, F.lit(True))
        )

        # I am performing a left join against the parent table keys.
        checked = checked.join(
            parent_ids,
            F.col(column_name) == F.col(f"_parent_key_{index}"),
            "left"
        )

        # I am flagging non-empty foreign keys that could not be matched in the parent dataset.
        reasons.append(
            F.when(
                ~is_missing(column_name)
                & F.col(flag_column).isNull(),
                f"Unmatched {column_name}"
            )
        )

        # I am tracking the joined helper columns so I can remove them later.
        temporary_columns.extend(
            [f"_parent_key_{index}", flag_column]
        )

    # I am applying complex validation rules specific to the trips dataset.
    if dataset_name == "trips":
        # I am checking if trip end times are recorded before their start times.
        reasons.append(
            F.when(
                F.col("start_time_parsed").isNotNull()
                & F.col("end_time_parsed").isNotNull()
                & (
                    F.col("end_time_parsed")
                    < F.col("start_time_parsed")
                ),
                "End time before start time"
            )
        )

        # I am converting the timestamp gap into total duration in minutes.
        calculated_duration = (
            F.col("end_time_parsed").cast("long")
            - F.col("start_time_parsed").cast("long")
        ) / 60.0

        # I am comparing the stated duration against the calculated duration, allowing a 1-minute margin.
        reasons.append(
            F.when(
                F.col("duration_minutes_parsed").isNotNull()
                & F.col("start_time_parsed").isNotNull()
                & F.col("end_time_parsed").isNotNull()
                & (
                    F.abs(
                        F.col("duration_minutes_parsed")
                        - calculated_duration
                    ) > 1
                ),
                "Duration does not match timestamps"
            )
        )

    # I am combining all captured error messages into a single semicolon-separated string column.
    checked = checked.withColumn(
        "_quarantine_reason",
        F.concat_ws("; ", *reasons)
    )

    # I am filtering out records that failed one or more checks into a quarantine dataset.
    quarantined = checked.filter(
        F.col("_quarantine_reason") != ""
    )

    # I am isolating records that passed every validation rule into a clean dataset.
    clean = checked.filter(
        F.col("_quarantine_reason") == ""
    )

    # I am dropping all intermediate helper columns while preserving data and quarantine explanations.
    clean = clean.drop(*temporary_columns)
    quarantined = quarantined.drop(*temporary_columns)

    # I am gathering row count metrics for validation logging.
    summary = {
        "dataset": dataset_name,
        "input_rows": df.count(),
        "clean_rows": clean.count(),
        "quarantined_rows": quarantined.count()
    }

    logger.info("Validation summary: %s", summary)

    return clean, quarantined, summary


def validate_datasets(datasets):
    # I am processing each dataset through the validation routine and storing the outputs.
    clean_datasets = {}
    quarantined_datasets = {}
    summaries = {}

    for dataset_name, df in datasets.items():
        clean, quarantined, summary = validate_dataset(
            dataset_name,
            df,
            datasets
        )

        clean_datasets[dataset_name] = clean
        quarantined_datasets[dataset_name] = quarantined
        summaries[dataset_name] = summary

    logger.info("Validation completed for all datasets.")

    return clean_datasets, quarantined_datasets, summaries


def validate_trips(datasets):
    # I am exposing this targeted function to maintain backward compatibility with main.py.
    clean, quarantined, summary = validate_dataset(
        "trips",
        datasets["trips"],
        datasets
    )

    return clean, quarantined, summary