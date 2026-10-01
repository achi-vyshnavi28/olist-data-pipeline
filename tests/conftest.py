import os
import tempfile

# every test run writes to a throwaway lake and warehouse, set before the pipeline modules read the paths
_TMP = tempfile.mkdtemp(prefix="olist-lake-")
os.environ["PIPELINE_LAKE"] = os.path.join(_TMP, "lake")
os.environ["PIPELINE_WAREHOUSE"] = os.path.join(_TMP, "warehouse", "olist.duckdb")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
os.environ.setdefault("SPARK_DRIVER_MEMORY", "2g")

import pytest  # noqa: E402

from pipeline.spark import spark_session  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = spark_session("olist-tests")
    yield s
    s.stop()
