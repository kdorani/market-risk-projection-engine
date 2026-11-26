from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "data" / "source"
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
BUSINESS_DATE = "2026-11-24"
SPARK_APP_NAME = "MarketRiskProjectionEngine"
