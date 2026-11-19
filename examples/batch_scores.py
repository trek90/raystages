"""Run the same staged workload against Ray or the local thread backend."""

import argparse
from contextlib import nullcontext

from raystages import Pipeline, Stage, ThreadBackend


def parse(line: str) -> list[int]:
    return [int(x) for x in line.split(",")]


def score(numbers: list[int]) -> int:
    return sum(x * x for x in numbers)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threads", action="store_true")
    args = parser.parse_args()
    if not args.threads:
        import ray

        ray.init()
    try:
        manager = ThreadBackend() if args.threads else nullcontext(None)
        with manager as backend:
            stages = [Stage("parse", parse, concurrency=4), Stage("score", score)]
            results = list(Pipeline(stages, max_items=3, backend=backend).run(
                iter(["3,4", "1,2", "bad", "5"])
            ))
            assert {r.index: r.value for r in results if r.ok} == {0: 25, 1: 5, 3: 25}
            assert [(r.index, r.failure.stage) for r in results if not r.ok] == [(2, "parse")]
            for r in sorted(results, key=lambda result: result.index):
                print(r.index, r.value if r.ok else r.failure)
    finally:
        if not args.threads:
            ray.shutdown()


if __name__ == "__main__":
    main()
