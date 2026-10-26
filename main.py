
from pathlib import Path
import logging
from pyspark.sql import SparkSession
from city_mobility.schemas import SCHEMAS
from city_mobility.extract import extract_datasets
from city_mobility.profiling import run_profiling
from city_mobility.transform import transform_datasets
from city_mobility.validation import validate_datasets
from city_mobility.load import save_validation_outputs

from config import RAW_DATA_DIR, OUTPUTS_DIR

# I keep the log format simple so I can quickly understand
# which step the pipeline is currently performing.
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

logger = logging.getLogger(__name__)

def main():

    # My original CSV files are stored in the data/raw folder.
    data_dir = RAW_DATA_DIR

    #for profiling the report
    output_dir = OUTPUTS_DIR / "profiling"

    # I create one Spark session for this pipeline.
    # The same session will be used to read all five datasets.
    spark = (
        SparkSession.builder
        .appName("CityMobilityPipeline")
        .getOrCreate()
    )

    try:
        logger.info("Starting City Mobility pipeline")

        # I pass the folder and schemas to my reusable extraction
        # function instead of writing five separate read operations.
        datasets = extract_datasets(
            spark=spark,
            data_dir=data_dir,
            schemas=SCHEMAS
        )

        # I inspect the extracted DataFrames to make sure that
        # Spark has read the expected columns and data types.
        for dataset_name, df in datasets.items():
            logger.info("Previewing dataset: %s", dataset_name)

            # I check the structure first.
            df.printSchema()

            # I look at a few rows to understand what was read.
            df.show(5, truncate=False)

        # I now have all five DataFrames ready for the next stage.
        # I haven't cleaned or removed any records here.
        logger.info("Extraction stage completed successfully")

        # I profile the extracted data to understand its quality
        # before making any changes to the original records.
        report = run_profiling(
            datasets=datasets,
            output_dir=output_dir
        )

        logger.info("Data profiling completed successfully")
        logger.info("Quality report: %s", output_dir / "quality_report.json")
        logger.info("Findings report: %s", output_dir / "findings.json")

        # I display a small summary so I can confirm that profiling ran.
        logger.info(
            "Number of datasets profiled: %d",
            len(report.get("datasets", {}))
        )
        logger.info(
            "Number of findings recorded: %d",
            len(report.get("findings", []))
        )

        
        # I standardize values and add safely parsed columns.
        transformed_datasets = transform_datasets(datasets)

        # I check a few transformed trip records.
        transformed_datasets["trips"].select(
            "trip_id",
            "trip_status",
            "start_time",
            "start_time_parsed",
            "duration_minutes",
            "duration_minutes_parsed"
        ).show(10, truncate=False)

        # I validate all five transformed datasets.
        clean_datasets, quarantined_datasets, validation_summaries = (
            validate_datasets(transformed_datasets)
        )

        # I display the validation summary for every dataset.
        for dataset_name, summary in validation_summaries.items():
            logger.info("%s", summary)

        # I inspect a few quarantined trips to understand why they failed.
        quarantined_datasets["trips"].select(
            "trip_id",
            "_quarantine_reason"
        ).show(10, truncate=False)

        # I inspect a few quarantined maintenance records.
        quarantined_datasets["maintenance"].select(
            "maintenance_id",
            "_quarantine_reason"
        ).show(10, truncate=False)

        # I save the clean datasets, quarantined records and validation summary.
        saved_paths = save_validation_outputs(
            clean_datasets,
            quarantined_datasets,
            validation_summaries
        )

        logger.info("Saved output locations: %s", saved_paths)

    except Exception:
        # I record the error before allowing the failure
        # to propagate, so the run doesn't look successful.
        logger.exception("City Mobility pipeline failed")
        raise



    finally:
        # I stop Spark after the extraction script finishes
        spark.stop()


if __name__ == "__main__":
    main()
