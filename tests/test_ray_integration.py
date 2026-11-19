"""Opt-in tests exercise real Ray workers, not the local thread backend."""

import os

import pytest

from raystages import Pipeline, Stage

pytestmark = pytest.mark.skipif(
    os.getenv("RAYSTAGES_REAL_RAY") != "1",
    reason="set RAYSTAGES_REAL_RAY=1 to start a real Ray runtime",
)


def _transform(x):
    if x == 2:
        raise ValueError("bad value")
    return x * 2


def _encode(x):
    return {"encoded": x}


def test_real_ray_pipeline_isolates_stage_errors():
    ray = pytest.importorskip("ray")
    ray.init(num_cpus=2, include_dashboard=False)
    try:
        stages = [Stage("transform", _transform), Stage("encode", _encode)]
        output = list(Pipeline(stages, max_items=3).run(range(5)))
        by_index = {r.index: r for r in output}
        assert set(by_index) == set(range(5))
        for index in (0, 1, 3, 4):
            assert by_index[index].value == {"encoded": index * 2}
            assert by_index[index].failure is None
        failure = by_index[2].failure
        assert failure.stage == "transform"
        assert "ValueError" in failure.error_type
        assert "bad value" in failure.message
    finally:
        ray.shutdown()
