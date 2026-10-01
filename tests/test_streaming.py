"""Structured Streaming on real orders replayed as events. Needs Spark's local file system, so it is skipped on plain
Windows and runs in CI on Linux."""
import os
import tempfile
from pathlib import Path

import pytest

from pipeline import streaming
from pipeline.run import run_stage
from pipeline.spark import native_writes

pytestmark = pytest.mark.skipif(not native_writes(), reason="Spark streaming needs winutils on Windows; runs in CI")


def test_hourly_windows_count_every_replayed_order(spark):
    for stage in ("bronze", "silver"):
        run_stage(stage, spark)
    tmp = Path(tempfile.mkdtemp())
    events = streaming.replay(spark, tmp / "landing", files=5)
    rows = streaming.run(spark, tmp / "landing", tmp / "checkpoint")
    assert events == 2000
    assert sum(r.orders for r in rows) == events  # ordered replay: nothing falls behind the watermark
    assert all(r.window_start.minute == 0 for r in rows)  # hourly tumbling windows
