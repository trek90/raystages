import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from raystages import Pipeline, Stage, ThreadBackend


def run(stages, items, *, max_items=32):
    with ThreadBackend() as backend:
        return list(Pipeline(stages, max_items=max_items, backend=backend).run(items))


def test_composes_stages_preserving_original_indices():
    result = run(
        [Stage("inc", lambda x: x + 1), Stage("square", lambda x: x * x)],
        range(8),
    )
    assert sorted((r.index, r.value) for r in result) == [
        (i, (i + 1) ** 2) for i in range(8)
    ]
    assert all(r.failure is None for r in result)


def test_completion_order_not_input_order():
    release = threading.Event()

    def gate(item):
        if item == 0 and not release.wait(3):
            raise RuntimeError("test did not release first item")
        return item

    backend = ThreadBackend()
    results = Pipeline([Stage("gate", gate, concurrency=2)], backend=backend).run([0, 1])
    try:
        first = next(results)
        assert first.index == 1 and first.value == 1 and first.failure is None
    finally:
        release.set()
        results.close()
        backend.close()


def test_per_stage_concurrency_cap():
    lock = threading.Lock()
    both = threading.Event()
    release = threading.Event()
    active = peak = 0

    def limited(item):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == 2:
                both.set()
        try:
            if not release.wait(3):
                raise RuntimeError("test did not release stage")
            return item
        finally:
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run, [Stage("limited", limited, concurrency=2)], range(20))
        try:
            assert both.wait(2)
        finally:
            release.set()
        results = future.result(timeout=4)
    assert len(results) == 20
    assert all(result.failure is None for result in results)
    assert peak == 2


def test_lazy_input_is_bounded_when_consumer_pauses():
    yielded = []

    def source():
        for i in range(200):
            yielded.append(i)
            yield i

    backend = ThreadBackend()
    results = Pipeline(
        [Stage("identity", lambda x: x, concurrency=4)], max_items=3, backend=backend
    ).run(source())
    try:
        first = next(results)
        assert first.failure is None
        assert 1 <= len(yielded) <= 3
    finally:
        results.close()
        backend.close()
    assert len(yielded) <= 3


def test_stage_failure_does_not_poison_other_items_or_run_downstream():
    downstream = []

    def fail_one(x):
        if x == 1:
            raise ValueError("bad input")
        return x

    def record(x):
        downstream.append(x)
        return None

    results = run([Stage("validate", fail_one), Stage("record", record)], [0, 1, 2])
    by_index = {r.index: r for r in results}
    failure = by_index[1].failure
    assert failure.stage == "validate"
    assert failure.error_type == "ValueError"
    assert "bad input" in failure.message
    assert sorted(downstream) == [0, 2]
    for i in (0, 2):
        assert by_index[i].value is None and by_index[i].failure is None


def test_empty_input_yields_nothing():
    assert run([Stage("identity", lambda x: x)], []) == []


def test_generator_close_prevents_further_admission():
    admitted = []

    def source():
        for i in range(200):
            admitted.append(i)
            yield i

    with ThreadBackend() as backend:
        results = Pipeline([Stage("identity", lambda x: x)], max_items=2, backend=backend).run(source())
        try:
            next(results)
        finally:
            results.close()
    count = len(admitted)
    with pytest.raises(StopIteration):
        next(results)
    assert len(admitted) == count <= 2


@pytest.mark.parametrize(
    ("stages", "max_items", "error"),
    [
        (lambda: [], 3, "at least one stage"),
        (lambda: [Stage("id", str), Stage("id", str)], 3, "duplicate stage"),
        (lambda: [Stage("id", str, concurrency=0)], 3, "concurrency"),
        (lambda: [Stage("id", str, max_retries=-1)], 3, "max_retries"),
        (lambda: [Stage("id", str, num_gpus=float("nan"))], 3, "num_gpus"),
        (lambda: [Stage("id", str)], True, "max_items"),
    ],
)
def test_invalid_pipeline_configuration(stages, max_items, error):
    with pytest.raises(ValueError, match=error):
        Pipeline(stages(), max_items=max_items)


def test_source_failure_cancels_active_handle_and_keeps_original_error():
    class TrackingBackend(ThreadBackend):
        def __init__(self):
            super().__init__()
            self.cancellations = []

        def cancel(self, ref):
            self.cancellations.append(ref)
            raise RuntimeError("cancellation itself failed")

    def broken_source():
        yield 0
        raise ValueError("source failed")

    with TrackingBackend() as backend:
        pipeline = Pipeline([Stage("identity", lambda x: x)], backend=backend)
        with pytest.raises(ValueError, match="source failed"):
            list(pipeline.run(broken_source()))
        assert len(backend.cancellations) == 1
