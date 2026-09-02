import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TRANSFORMATIONS = REPO_ROOT / "src" / "nyc_data_analysis_project_etl" / "transformations"


@pytest.fixture(scope="session")
def spark():
    # A genuine local SparkSession (no cluster, no Databricks Connect) — enough to
    # exercise the extracted logic modules' real DataFrame transformations rather
    # than mocking Spark itself. Single partition + reduced shuffle partitions
    # keep these tiny test DataFrames fast.
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-data-engineering-practice-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def load_module_from_path(path, module_name):
    # Loading each logic module by its exact file path under an explicit name
    # avoids any dependence on sys.path/sys.modules state — robust regardless of
    # what other fixtures or test files have already imported.
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def bronze_logic():
    return load_module_from_path(TRANSFORMATIONS / "bronze" / "bronze_logic.py", "bronze_logic")


@pytest.fixture(scope="session")
def silver_logic():
    return load_module_from_path(TRANSFORMATIONS / "silver" / "silver_logic.py", "silver_logic")


@pytest.fixture(scope="session")
def summary_logic():
    return load_module_from_path(TRANSFORMATIONS / "gold" / "summary_logic.py", "summary_logic")


@pytest.fixture(scope="session")
def price_hikes_logic():
    return load_module_from_path(TRANSFORMATIONS / "gold" / "price_hikes_logic.py", "price_hikes_logic")
