
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

from config import OUTPUTS_DIR
def save_dataframe(df, output_path):
    # I create the output folder if it does not exist yet.
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # I save the DataFrame as Parquet.
    # Overwrite lets me rerun the pipeline without manually deleting old outputs.
    df.write.mode("overwrite").parquet(str(output_path))

    logger.info("Saved output to %s", output_path.resolve())


def save_validation_outputs(
    clean_datasets,
    quarantined_datasets,
    validation_summaries,
    output_dir=OUTPUTS_DIR
):
    # I keep clean data, quarantined data and reports in separate folders.
    output_path = Path(output_dir)
    clean_path = output_path / "clean"
    quarantine_path = output_path / "quarantine"
    reports_path = output_path / "reports"

    # I save every clean dataset separately as Parquet.
    for dataset_name, df in clean_datasets.items():
        save_dataframe(df, clean_path / dataset_name)

    # I preserve questionable records and their quarantine reasons.
    for dataset_name, df in quarantined_datasets.items():
        save_dataframe(df, quarantine_path / dataset_name)

    # I save the validation summary as a JSON report.
    reports_path.mkdir(parents=True, exist_ok=True)
    summary_file = reports_path / "validation_summary.json"

    with summary_file.open("w", encoding="utf-8") as file:
        json.dump(validation_summaries, file, indent=2, default=str)

    logger.info("Validation summary saved to %s", summary_file.resolve())

    return {
        "clean_dir": str(clean_path.resolve()),
        "quarantine_dir": str(quarantine_path.resolve()),
        "summary_file": str(summary_file.resolve())
    }
