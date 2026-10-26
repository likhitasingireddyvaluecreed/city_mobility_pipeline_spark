# City Mobility Pipeline

A modular PySpark project for profiling, cleaning, validating, and investigating city bike-sharing data. `main.py` runs the ETL and data-quality pipeline. `analytics.ipynb` is a separate investigation notebook that reads the saved clean datasets, performs business analyses, compares Spark SQL results with DataFrame results, inspects execution plans, and saves analytical tables as Parquet.

## Prerequisites

Install the following before starting:

- Git
- Python 3.13 (or a Python version supported by the PySpark version in `requirements.txt`)
- Java 17 or later, with `JAVA_HOME` configured

Check your installations:

```powershell
git --version
python --version
java -version
```

## Get the project

Clone the repository and enter its folder. Replace the URL with the URL of your GitHub repository.

```powershell
git clone <>
cd city_mobility_pipeline
```

If you downloaded the repository as a ZIP instead, extract it and open a terminal in the folder containing `main.py`.

## Create an environment and install dependencies

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, you can install and run commands using the environment's Python directly, for example:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Check the input data

Place the supplied CSV files in `data/raw/`:

- `stations.csv`
- `bikes.csv`
- `users.csv`
- `trips.csv`
- `maintenance.csv`

Keep the expected column names unchanged unless you also update the schemas and pipeline code.

## Run the pipeline

From the project root—the directory containing `main.py`—run:

```powershell
python main.py
```

The pipeline writes clean datasets to `outputs/clean/`, quarantined records to `outputs/quarantine/`, profiling JSON files to `outputs/profiling/`, and a validation summary to `outputs/reports/`.

Run the analytics notebook **after** the pipeline, from the project root. In VS Code, open `analytics.ipynb` and run all cells in order. The notebook writes its analytical Parquet datasets to `outputs/analytics/`. The notebook is separate from `main.py`; `python main.py` does not execute the notebook.

## Run the tests

```powershell
python -m pytest -v tests/test_validation.py
```

## Configuration

`config.py` defines project-relative paths for input data and the output root. The notebook uses project-root-relative paths, so run it from the folder containing `main.py`.

## Troubleshooting

- **`ModuleNotFoundError: No module named 'city_mobility'`:** run commands from the project root, where the `city_mobility/` package and `main.py` are located.
- **Java or Spark startup error:** check `java -version`, `JAVA_HOME`, and that the installed Java version is compatible with PySpark.
- **Input file not found:** confirm that all five CSV files are present in `data/raw/` and that their filenames match the names above.
- **Tests fail during collection:** first check imports and project-root paths; a collection error means the tests have not yet executed.
