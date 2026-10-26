import json
import logging
from pathlib import Path

from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


# I list the columns that should uniquely identify records.
# If the same key appears more than once, I want to investigate it.
KEY_COLUMNS = {
    "stations": "station_id",
    "bikes": "bike_id",
    "users": "user_id",
    "trips": "trip_id",
    "maintenance": "maintenance_id"
}


# I list columns where different spellings or letter cases
# might represent the same category.
CATEGORY_COLUMNS = {
    "bikes": ["bike_type", "status"],
    "users": ["membership", "home_district"],
    "trips": ["trip_status"],
    "maintenance": ["issue_type", "resolution"],
    "stations": ["district"]
}


# I list numeric fields that need range and conversion checks.
NUMERIC_COLUMNS = {
    "stations": ["capacity"],
    "bikes": ["purchase_year"],
    "trips": ["duration_minutes"],
    "maintenance": ["repair_minutes"]
}



#initial bussiness rule assumption
NUMERIC_RULES = {
    "stations": {
        # I expect station capacity to be positive because it represents
        # how many bikes a station can accommodate.
        "capacity": {"min": 1}
    },
    "bikes": {
        # The available records contain operational data from August 2026.
        # I provisionally flag purchase years after 2026, but I should
        # confirm the dataset reference date before finalizing this rule.
        "purchase_year": {"max": 2026}
    },
    "trips": {
        # I flag negative durations because elapsed trip time cannot be negative.
        # I do not set an upper limit until I investigate the longest trips.
        "duration_minutes": {"min": 0}
    },
    "maintenance": {
        # I flag negative repair durations because elapsed repair time
        # cannot be negative. The observed maximum alone does not establish
        # the maximum repair time allowed by the business.
        "repair_minutes": {"min": 0}
    }
}




# I define which columns should refer to records in another dataset.
# A reference that does not exist in the parent dataset needs investigation.
REFERENCE_RULES = [
    ("trips", "user_id", "users", "user_id"),
    ("trips", "bike_id", "bikes", "bike_id"),
    ("trips", "start_station_id", "stations", "station_id"),
    ("trips", "end_station_id", "stations", "station_id"),
    ("maintenance", "bike_id", "bikes", "bike_id")
]


def get_count(df):
    # I use one small helper whenever I need the number of rows.
    # This keeps my row-counting logic clean and avoids writing df.count() everywhere.
    return df.count()


def count_missing_values(df, column_name):
    # I treat nulls, empty strings, whitespace and common placeholders
    # as missing so they are not overlooked during profiling.
    
    # I cast everything to string first so I can safely trim spaces
    # and check for text placeholders like "null" or "n/a regardless of data type.
    value = F.trim(F.col(column_name).cast("string"))

    # I group all the conditions together to catch both actual nulls
    # and hidden text blanks in a single filter pass.
    missing_condition = (
        F.col(column_name).isNull()
        | (value == "")
        | (F.lower(value).isin(
            "null", "none", "na", "n/a", "nan", "unknown"
        ))
    )

    # I return the total count of rows that match any of these missing criteria.
    return df.filter(missing_condition).count()


def count_duplicate_rows(df):
    # I check full-row duplicates without assuming that every
    # repeated value in an individual column is an error.
    
    # If the dataframe has no columns, there can't be any duplicates,
    # so I safely return zero right away.
    if not df.columns:
        return 0

    # I group by all columns at once so Spark looks for rows
    # where every single column value matches identically.
    duplicate_groups = (
        df.groupBy(*df.columns)
        .count()
        .filter(F.col("count") > 1)
    )

    # This returns the number of extra copies, not the number
    # of rows that belong to duplicate groups.
    
    # I subtract 1 from each group count because the first copy is valid,
    # and only the extra repetitions count as duplicate copies.
    result = duplicate_groups.agg(
        F.sum(F.col("count") - 1).alias("duplicate_count")
    ).first()

    # I extract the final number safely, defaulting to zero if no result comes back.
    return int(result["duplicate_count"] or 0)


def count_duplicate_keys(df, key_column):
    # I check repeated identifiers only when a key is configured.
    # A missing key is handled separately by missing-value profiling.
    
    # I double check that the configured key column actually exists
    # in the dataframe before running any groupby operations.
    if key_column not in df.columns:
        raise ValueError(f"Key column not found: {key_column}")

    # I filter out null keys first so missing IDs don't get 
    # incorrectly counted together as duplicate primary keys.
    duplicate_keys = (
        df.filter(F.col(key_column).isNotNull())
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )

    # I return how many unique keys appeared more than once.
    return duplicate_keys.count()


def profile_categories(df, dataset_name, findings):
    # I inspect category frequencies so that unusual values
    # and inconsistent spellings become visible.
    category_summary = {}

    for column_name in CATEGORY_COLUMNS.get(dataset_name, []):
        # I skip any category column if it happens to be missing from this dataframe.
        if column_name not in df.columns:
            continue

        # I trim whitespace so value grouping counts match properly.
        value = F.trim(F.col(column_name).cast("string"))

        # I group by the cleaned category value and sort from highest frequency to lowest.
        counts = (
            df.groupBy(value.alias("value"))
            .count()
            .orderBy(F.desc("count"))
        )

        # I save the top 100 most frequent category values into my summary dictionary.
        category_summary[column_name] = [
            {
                "value": row["value"],
                "count": row["count"]
            }
            for row in counts.limit(100).collect()
        ]

        # I compare trimmed, lowercase versions of categories.
        # Multiple raw values with the same normalized value may
        # indicate inconsistent capitalization or whitespace.
        
        # I map each raw value to its lowercase form to spot casing issues
        # like "Active" vs "active" or trailing spaces.
        normalized_values = (
            df.filter(F.col(column_name).isNotNull())
            .select(
                F.col(column_name).cast("string").alias("raw_value"),
                F.lower(F.trim(F.col(column_name).cast("string"))).alias(
                    "normalized_value"
                )
            )
            .filter(F.col("normalized_value") != "")
            .distinct()
        )

        # I group by the normalized value and count how many different raw spellings exist for it.
        inconsistent_groups = (
            normalized_values
            .groupBy("normalized_value")
            .agg(F.countDistinct("raw_value").alias("raw_variants"))
            .filter(F.col("raw_variants") > 1)
        )

        variant_count = inconsistent_groups.count()

        #  I find multiple raw variants sharing the same normalized form
        if variant_count:
            findings.append({
                "dataset": dataset_name,
                "check": "inconsistent_category_format",
                "column": column_name,
                "count": variant_count,
                "details": (
                    "Different raw values share the same "
                    "trimmed, lowercase representation."
                )
            })

    return category_summary

def profile_numeric_columns(df, dataset_name, findings):
    numeric_summary = {}

    # Common text placeholders that represent missing data rather than actual numbers
    missing_values = ["", "null", "none", "na", "n/a", "nan", "unknown"]

    # Loop through each configured numeric column for the given dataset
    for column_name in NUMERIC_COLUMNS.get(dataset_name, []):
        rules = NUMERIC_RULES.get(dataset_name, {}).get(column_name, {})

        # Attempt to safely cast the column to a double (float). 
        # If Spark cannot parse text into a number, it turns it into null instead of crashing.
        numeric_df = df.withColumn(
            "_numeric_value",
            F.expr(f"try_cast(`{column_name}` AS DOUBLE)")
        )

        # Standardize raw text to lowercase and strip whitespace for uniform checking
        raw_value = F.lower(F.trim(F.col(column_name).cast("string")))

        # Check 1: Is the value completely missing or represented by a placeholder text?
        is_missing = (
            F.col(column_name).isNull()
            | raw_value.isin(missing_values)
        )

        # Check 2: Is it supposed to be a number (not missing), but failed to parse?
        is_malformed = (
            ~is_missing
            & F.col("_numeric_value").isNull()
        )

        # Check 3: Is the parsed number a decimal when it should be a whole number?
        is_non_integer = (
            F.col("_numeric_value").isNotNull()
            & (
                F.col("_numeric_value")
                != F.floor(F.col("_numeric_value"))
            )
        )

        # Check 4: Does the number violate any configured minimum or maximum boundaries?
        is_out_of_range = F.lit(False)

        if "min" in rules:
            is_out_of_range = (
                is_out_of_range
                | (F.col("_numeric_value") < rules["min"])
            )

        if "max" in rules:
            is_out_of_range = (
                is_out_of_range
                | (F.col("_numeric_value") > rules["max"])
            )

        is_out_of_range = (
            F.col("_numeric_value").isNotNull()
            & is_out_of_range
        )

        # Aggregate overall row metrics, error counts, basic stats, and distribution percentiles
        stats = numeric_df.agg(
            F.count("*").alias("row_count"),
            F.sum(
                F.when(is_missing, 1).otherwise(0)
            ).alias("missing_like_count"),
            F.sum(
                F.when(is_malformed, 1).otherwise(0)
            ).alias("malformed_count"),
            F.sum(
                F.when(is_non_integer, 1).otherwise(0)
            ).alias("non_integer_count"),
            F.sum(
                F.when(is_out_of_range, 1).otherwise(0)
            ).alias("out_of_range_count"),
            F.count("_numeric_value").alias("parsed_numeric_count"),
            F.min("_numeric_value").alias("minimum"),
            F.max("_numeric_value").alias("maximum"),
            F.avg("_numeric_value").alias("average"),
            F.expr(
                "percentile_approx(_numeric_value, 0.5)"
            ).alias("median"),
            F.expr(
                "percentile_approx(_numeric_value, 0.9)"
            ).alias("p90"),
            F.expr(
                "percentile_approx(_numeric_value, 0.95)"
            ).alias("p95"),
            F.expr(
                "percentile_approx(_numeric_value, 0.99)"
            ).alias("p99")
        ).first().asDict()

        # Save summary metrics for this column
        numeric_summary[column_name] = stats

        # Map counts to their specific issue types for quality reporting
        issue_counts = {
            "missing_value": stats["missing_like_count"],
            "malformed_numeric_value": stats["malformed_count"],
            "non_integer_numeric_value": stats["non_integer_count"],
            "numeric_value_out_of_range": stats["out_of_range_count"]
        }

        # If any quality issues are found, log them into the findings list for auditing
        for issue_name, issue_count in issue_counts.items():
            if issue_count:
                findings.append({
                    "dataset": dataset_name,
                    "column": column_name,
                    "issue": issue_name,
                    "count": int(issue_count),
                    "rules": rules
                })

    return numeric_summary


def safe_timestamp(column_name):
    # I return null when Spark cannot parse a timestamp,
    # so I can report malformed values without stopping profiling.
    return F.expr(f"try_to_timestamp(`{column_name}`)")


def profile_trip_timestamps(df, findings):
    # I check trip timestamps separately because a timestamp may
    # be missing, malformed or inconsistent with the trip duration.
    timestamp_summary = {}

    for column_name in ["start_time", "end_time"]:
        # I verify the timestamp column exists before proceeding.
        if column_name not in df.columns:
            continue

        # I grab the trimmed text version of the timestamp.
        raw_value = F.trim(F.col(column_name).cast("string"))

        # I attempt to parse the string into a proper timestamp.
        parsed_value = safe_timestamp(column_name)

        # I create a temp dataframe holding both the raw text and parsed timestamp.
        timestamp_df = df.select(
            raw_value.alias("raw_value"),
            parsed_value.alias("parsed_value")
        )

        # I count missing timestamp entries.
        missing_count = count_missing_values(df, column_name)

        # If a non-empty timestamp string fails to parse, I count it as malformed.
        malformed_count = timestamp_df.filter(
            F.col("raw_value").isNotNull()
            & (F.col("raw_value") != "")
            & F.col("parsed_value").isNull()
        ).count()

        # I store these counts in the timestamp summary dictionary.
        timestamp_summary[column_name] = {
            "missing_count": missing_count,
            "malformed_count": malformed_count
        }

        # If malformed timestamps are found, I add them to findings.
        if malformed_count:
            findings.append({
                "dataset": "trips",
                "check": "malformed_timestamp",
                "column": column_name,
                "count": malformed_count,
                "details": "A non-empty timestamp could not be parsed."
            })

    # If any required timestamp or duration column is missing, I stop comparative checks here.
    if not all(
        column in df.columns
        for column in ["start_time", "end_time", "duration_minutes"]
    ):
        return timestamp_summary

    # I compare the duration recorded in the file with the duration
    # calculated from the two timestamps, where both timestamps parse.
    
    # I parse both timestamps and convert the recorded duration into a double number.
    checked_df = (
        df.withColumn(
            "_start_ts",
            safe_timestamp("start_time")
        )
        .withColumn(
            "_end_ts",
            safe_timestamp("end_time")
        )
        .withColumn(
            "_recorded_duration",
            F.expr("try_cast(`duration_minutes` AS DOUBLE)")
        )
        .withColumn(
            "_calculated_duration",
            (
                F.col("_end_ts").cast("long")
                - F.col("_start_ts").cast("long")
            ) / 60.0
        )
    )

    # I check for impossible trips where the end time happens before the start time.
    negative_time_count = checked_df.filter(
        F.col("_start_ts").isNotNull()
        & F.col("_end_ts").isNotNull()
        & (F.col("_end_ts") < F.col("_start_ts"))
    ).count()

    # If I find trips going backward in time, I log a finding.
    if negative_time_count:
        findings.append({
            "dataset": "trips",
            "check": "end_before_start",
            "column": "start_time,end_time",
            "count": negative_time_count,
            "details": "The parsed end timestamp occurs before the start timestamp."
        })

    # A one-minute tolerance allows small rounding differences.
    # I check if the duration written in the file matches what the timestamps say.
    mismatch_count = checked_df.filter(
        F.col("_start_ts").isNotNull()
        & F.col("_end_ts").isNotNull()
        & F.col("_recorded_duration").isNotNull()
        & (
            F.abs(
                F.col("_recorded_duration")
                - F.col("_calculated_duration")
            ) > 1
        )
    ).count()

    # If the duration doesn't match the timestamps, I log a mismatch finding.
    if mismatch_count:
        findings.append({
            "dataset": "trips",
            "check": "duration_timestamp_mismatch",
            "column": "duration_minutes",
            "count": mismatch_count,
            "details": (
                "Recorded duration differs from timestamp-derived duration "
                "by more than one minute."
            )
        })

    # I store the comparison results in the timestamp summary dictionary.
    timestamp_summary["duration_timestamp_comparison"] = {
        "mismatch_count": mismatch_count,
        "end_before_start_count": negative_time_count,
        "comparison_tolerance_minutes": 1
    }

    return timestamp_summary


def profile_dataset(dataset_name, df, findings):
    # I collect the main quality indicators for one dataset.
    logger.info("Profiling dataset: %s", dataset_name)

    # I get the total number of rows so I have a baseline count for this dataset.
    row_count = get_count(df)

    missing_summary = {}

    # I loop through every column to check for missing or blank values.
    for column_name in df.columns:
        missing_count = count_missing_values(df, column_name)

        missing_summary[column_name] = missing_count

        # If I find missing values, I log them into my findings list.
        if missing_count:
            findings.append({
                "dataset": dataset_name,
                "check": "missing_values",
                "column": column_name,
                "count": missing_count,
                "details": "Null, blank or recognized placeholder values."
            })

    # I check for full duplicate rows across the entire dataset.
    duplicate_row_count = count_duplicate_rows(df)

    # If full row duplicates exist, I add them to findings.
    if duplicate_row_count:
        findings.append({
            "dataset": dataset_name,
            "check": "duplicate_rows",
            "column": None,
            "count": duplicate_row_count,
            "details": "Number of extra copies of identical full rows."
        })

    # I look up the primary key column configured for this specific dataset.
    key_column = KEY_COLUMNS.get(dataset_name)
    duplicate_key_count = 0

    # If a key column is defined and present, I check if any ID appears more than once.
    if key_column and key_column in df.columns:
        duplicate_key_count = count_duplicate_keys(df, key_column)

        # If duplicate keys are found, I log them into findings.
        if duplicate_key_count:
            findings.append({
                "dataset": dataset_name,
                "check": "duplicate_key",
                "column": key_column,
                "count": duplicate_key_count,
                "details": "Number of key values that occur more than once."
            })

    # I profile category columns for spelling variations and frequencies.
    category_summary = profile_categories(
        df, dataset_name, findings
    )

    # I profile numeric columns for parse failures and out-of-range values.
    numeric_summary = profile_numeric_columns(
        df, dataset_name, findings
    )

    timestamp_summary = {}

    # If we are profiling the trips dataset, I run our special timestamp checks too.
    if dataset_name == "trips":
        timestamp_summary = profile_trip_timestamps(df, findings)

    logger.info(
        "Finished profiling %s: %s rows",
        dataset_name,
        row_count
    )

    # I return a complete dictionary summarizing all profiling metrics for this dataset.
    return {
        "row_count": row_count,
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": df.schema.simpleString(),
        "missing_values": missing_summary,
        "duplicate_extra_rows": duplicate_row_count,
        "duplicate_key_values": duplicate_key_count,
        "key_column": key_column,
        "categories": category_summary,
        "numeric_columns": numeric_summary,
        "timestamps": timestamp_summary
    }


def profile_references(datasets, findings):
    # I check whether IDs in child datasets exist in their parent datasets.
    # I do not remove unmatched records; I only report them.
    reference_summary = []

    # I loop through all defined foreign key reference rules.
    for (
        child_name,
        child_column,
        parent_name,
        parent_column
    ) in REFERENCE_RULES:

        # If either the child or parent dataset is missing from my dictionary, I skip this check.
        if child_name not in datasets or parent_name not in datasets:
            continue

        child_df = datasets[child_name]
        parent_df = datasets[parent_name]

        # I make sure the reference columns actually exist in both dataframes.
        if child_column not in child_df.columns:
            continue

        if parent_column not in parent_df.columns:
            continue

        # I extract all valid non-null reference IDs from the child table.
        child_ids = (
            child_df
            .filter(F.col(child_column).isNotNull())
            .select(F.col(child_column).alias("_reference_id"))
        )

        # I extract unique valid IDs from the parent table to check against.
        parent_ids = (
            parent_df
            .filter(F.col(parent_column).isNotNull())
            .select(F.col(parent_column).alias("_reference_id"))
            .distinct()
        )

        # I use a left anti join to find child IDs that have no matching parent ID anywhere.
        unmatched_count = (
            child_ids.join(
                parent_ids,
                on="_reference_id",
                how="left_anti"
            )
            .count()
        )

        # I record the reference relationship summary.
        reference_summary.append({
            "child_dataset": child_name,
            "child_column": child_column,
            "parent_dataset": parent_name,
            "parent_column": parent_column,
            "unmatched_count": unmatched_count
        })

        # If I find orphaned references, I log them into my findings list for investigation.
        if unmatched_count:
            findings.append({
                "dataset": child_name,
                "check": "unmatched_reference",
                "column": child_column,
                "count": unmatched_count,
                "details": (
                    f"Some values do not exist in "
                    f"{parent_name}.{parent_column}."
                )
            })

    return reference_summary


def run_profiling(datasets, output_dir="outputs/profiling"):
    # I profile every extracted DataFrame and then check relationships
    # between the datasets.
    logger.info("Starting data profiling")

    findings = []
    dataset_summary = {}

    # I loop through each dataset one by one and run my profiling functions.
    for dataset_name, df in datasets.items():
        dataset_summary[dataset_name] = profile_dataset(
            dataset_name,
            df,
            findings
        )

    # After individual datasets are profiled, I check all foreign key references between them.
    reference_summary = profile_references(datasets, findings)

    # I keep the reports outside the raw input folder so the original
    # CSV files are not changed by profiling.
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # I bundle all summaries, references, and findings together into one report object.
    report = {
        "datasets": dataset_summary,
        "references": reference_summary,
        "findings_count": len(findings),
        "findings": findings
    }

    report_path = output_path / "quality_report.json"

    # I save the full detailed quality report as a JSON file.
    with report_path.open("w", encoding="utf-8") as report_file:
        json.dump(report, report_file, indent=2, default=str)

    # I also write a compact findings file so it is easy to review
    # the issues without opening the full quality report.
    findings_path = output_path / "findings.json"

    with findings_path.open("w", encoding="utf-8") as findings_file:
        json.dump(findings, findings_file, indent=2, default=str)

    logger.info("Profiling completed")
    logger.info("Quality report saved to: %s", report_path.resolve())
    logger.info("Findings report saved to: %s", findings_path.resolve())
    logger.info("Total finding entries: %s", len(findings))

    return report