"""Project paths shared by the City Mobility Pipeline modules."""

from pathlib import Path

# The directory containing this config.py file (the repository root).
PROJECT_ROOT = Path(__file__).resolve().parent

# Input data
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"

# Pipeline output folders
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
