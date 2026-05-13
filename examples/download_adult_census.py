"""Download the Kaggle Adult Census dataset and register Spark assets."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

KAGGLE_DATASET = "uciml/adult-census-income"
REPO_ROOT = Path(__file__).resolve().parents[1]
DOWNLOAD_ROOT = REPO_ROOT / "downloads" / "adult_census"
EXTRACT_DIR = DOWNLOAD_ROOT / "extracted"
WAREHOUSE_DIR = REPO_ROOT / "spark-warehouse"
METASTORE_DIR = REPO_ROOT / "metastore_db"
RAW_TABLE_NAME = "adult_census_raw"
RAW_VIEW_NAME = "adult_census_raw_view"


def canonicalize_column_name(name: str) -> str:
    """Convert source column names into stable Spark-friendly identifiers."""
    stripped = name.strip().lower()
    normalized = re.sub(r"[^0-9a-z]+", "_", stripped)
    collapsed = re.sub(r"_+", "_", normalized)
    return collapsed.strip("_")


def build_spark_session(app_name: str = "adult-census-download") -> SparkSession:
    """Create a local Spark session backed by a shared warehouse and metastore."""
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    metastore_url = f"jdbc:derby:;databaseName={METASTORE_DIR};create=true"
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE_DIR))
        .config("javax.jdo.option.ConnectionURL", metastore_url)
        .enableHiveSupport()
        .getOrCreate()
    )


def _trim_string_columns(dataset: DataFrame) -> DataFrame:
    projections = []
    for field in dataset.schema.fields:
        if isinstance(field.dataType, StringType):
            projections.append(F.trim(F.col(field.name)).alias(field.name))
        else:
            projections.append(F.col(field.name))
    return dataset.select(*projections)


def _rename_columns(dataset: DataFrame) -> DataFrame:
    renamed = dataset
    for source_name in dataset.columns:
        renamed = renamed.withColumnRenamed(
            source_name,
            canonicalize_column_name(source_name),
        )
    return renamed


def load_adult_census_frame(spark: SparkSession, csv_path: Path) -> DataFrame:
    """Load the extracted Adult Census CSV into a normalized Spark DataFrame."""
    dataset = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(str(csv_path))
    )
    return _trim_string_columns(_rename_columns(dataset))


def discover_csv_file(extracted_dir: Path) -> Path:
    """Locate the main CSV payload after extraction."""
    candidates = sorted(extracted_dir.rglob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No CSV files found under {extracted_dir}")
    if len(candidates) == 1:
        return candidates[0]

    preferred = [path for path in candidates if "adult" in path.stem.lower()]
    return preferred[0] if preferred else candidates[0]


def download_dataset(
    *,
    dataset_slug: str = KAGGLE_DATASET,
    extracted_dir: Path = EXTRACT_DIR,
    force: bool = False,
) -> Path:
    """Download and extract the Kaggle dataset with local caching."""
    extracted_dir.mkdir(parents=True, exist_ok=True)
    if not force and any(extracted_dir.rglob("*.csv")):
        return extracted_dir

    kaggle_cli = shutil.which("kaggle")
    if kaggle_cli is None:
        raise RuntimeError(
            "The Kaggle CLI was not found. Install it and configure Kaggle credentials "
            "before running this example."
        )

    command = [
        kaggle_cli,
        "datasets",
        "download",
        dataset_slug,
        "--path",
        str(extracted_dir),
        "--unzip",
        "--quiet",
    ]
    if force:
        command.append("--force")

    subprocess.run(command, check=True)
    return extracted_dir


def load_extracted_dataset(
    spark: SparkSession,
    *,
    extracted_dir: Path = EXTRACT_DIR,
) -> tuple[DataFrame, Path]:
    """Load the extracted CSV file and return both the frame and source path."""
    csv_path = discover_csv_file(extracted_dir)
    return load_adult_census_frame(spark, csv_path), csv_path


def create_raw_assets(
    spark: SparkSession,
    dataset: DataFrame,
    *,
    table_name: str = RAW_TABLE_NAME,
    view_name: str = RAW_VIEW_NAME,
) -> DataFrame:
    """Persist the raw dataset as a managed table and a matching temp view."""
    dataset.write.mode("overwrite").saveAsTable(table_name)
    registered = spark.table(table_name)
    registered.createOrReplaceTempView(view_name)
    return registered


def prepare_raw_assets(
    *,
    force_download: bool = False,
    dataset_slug: str = KAGGLE_DATASET,
    table_name: str = RAW_TABLE_NAME,
    view_name: str = RAW_VIEW_NAME,
) -> tuple[SparkSession, DataFrame, Path]:
    """Download, load, and register the Adult Census dataset."""
    spark = build_spark_session()
    extracted_dir = download_dataset(
        dataset_slug=dataset_slug,
        extracted_dir=EXTRACT_DIR,
        force=force_download,
    )
    dataset, csv_path = load_extracted_dataset(spark, extracted_dir=extracted_dir)
    registered = create_raw_assets(
        spark,
        dataset,
        table_name=table_name,
        view_name=view_name,
    )
    return spark, registered, csv_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force-download", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    spark, dataset, csv_path = prepare_raw_assets(force_download=args.force_download)
    print(f"Loaded {dataset.count()} rows from {csv_path}")
    print(f"Managed table: {RAW_TABLE_NAME}")
    print(f"Temp view: {RAW_VIEW_NAME}")
    spark.stop()


if __name__ == "__main__":
    main()
