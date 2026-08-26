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
    # bronze/logic.py and silver/logic.py share a filename — a plain `import logic`
    # from two different test files would collide via sys.modules and silently
    # return whichever one loaded first. Loading each by its exact file path under
    # a unique synthetic name sidesteps that entirely (and needs no sys.path
    # changes here — the pipeline files' own sys.path trick is a separate,
    # pipeline-execution-only concern, not exercised by these tests).
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def bronze_logic():
    return load_module_from_path(TRANSFORMATIONS / "bronze" / "logic.py", "bronze_logic")


@pytest.fixture(scope="session")
def silver_logic():
    return load_module_from_path(TRANSFORMATIONS / "silver" / "logic.py", "silver_logic")


@pytest.fixture(scope="session")
def summary_logic():
    return load_module_from_path(TRANSFORMATIONS / "gold" / "summary_logic.py", "summary_logic")


@pytest.fixture(scope="session")
def price_hikes_logic():
    return load_module_from_path(TRANSFORMATIONS / "gold" / "price_hikes_logic.py", "price_hikes_logic")
