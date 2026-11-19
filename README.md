# RayStages

Run a stream of inputs through several Ray stages without submitting the
entire stream at once. For example, use different concurrency and CPU budgets
for parsing records and scoring them. This is a synchronous Python driver
library for independent items, not a distributed persistence layer.

```python
import ray
from raystages import Pipeline, Stage

def parse(line):
    return [int(x) for x in line.split(",")]

def score(numbers):
    return sum(x * x for x in numbers)

ray.init()
try:
    pipeline = Pipeline([
        Stage("parse", parse, concurrency=4),
        Stage("score", score, concurrency=2),
    ], max_items=8)
    for result in pipeline.run(["3,4", "1,2", "bad", "5"]):
        print(result.index, result.value if result.ok else result.failure)
finally:
    ray.shutdown()
```

## Scheduling and failure behavior

Each item flows through a linear series of synchronous callables. The next
stage receives the previous stage's result. Return ordinary Python values
rather than Ray object references from stage functions. Items can overlap across stages.
`max_items` bounds admitted items, not bytes; each stage also caps submitted
tasks separately. Results stream as items finish, not necessarily in input
order; `index` recovers the original position. A failed stage produces one
failure for that item and skips its later stages. Infrastructure failures
outside an individual task, such as a failing input iterator, can end the run.

Ray must already be running when the default backend is used. Stage resource
settings map to Ray task options. `max_retries` configures Ray system-failure
retries, not retries for exceptions raised by user functions. Keep stages
idempotent if you enable retries. Closing a partially consumed iterator
requests best-effort cancellation; it does not roll back side effects.

This version returns intermediate values to the driver between stages and
may send them back out for the next task. It is designed for modest payloads;
it is not a zero-copy dataflow engine or a replacement for Ray Data. There
is no fan-out, checkpointing, or task timeout. Use one active run per
pipeline/backend unless a backend explicitly supports shared use.

## Local development

```bash
python -m pip install -e '.[test]'
python -m pytest -q
python examples/batch_scores.py --threads
```

`ThreadBackend` is a local testing substitute. It does not implement Ray
serialization, placement, resource reservations, or retry behavior. Use it
as a context manager so its worker threads are cleaned up. For real-Ray
integration on a machine that permits Ray to start:

```bash
python -m pip install -e '.[ray,test]'
RAYSTAGES_REAL_RAY=1 python -m pytest -q tests/test_ray_integration.py
python examples/batch_scores.py
```
