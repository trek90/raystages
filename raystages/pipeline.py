"""Bounded execution of a linear graph of Ray tasks."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator, Sequence
from typing import Any

from .backends import Backend, RayBackend
from .model import Failure, Result, Stage


class Pipeline:
    """Run independent inputs through a fixed sequence of stages.

    Each call to run has separate, lazy scheduling state. A backend must
    return distinct, hashable handles for outstanding submissions. Wait
    returns one of those handles; get resolves it or raises a task error.
    """

    def __init__(
        self, stages: Sequence[Stage], *, max_items: int = 32, backend: Backend | None = None
    ) -> None:
        if type(max_items) is not int or max_items < 1:
            raise ValueError("max_items must be a positive integer")
        stages = tuple(stages)
        if not stages:
            raise ValueError("at least one stage is required")
        names: set[str] = set()
        for stage in stages:
            if not isinstance(stage, Stage):
                raise TypeError("stages must contain Stage instances")
            if stage.name in names:
                raise ValueError(f"duplicate stage name: {stage.name}")
            names.add(stage.name)
        self.stages = stages
        self.max_items = max_items
        self.backend = backend

    def run(self, items: Iterable[Any]) -> Iterator[Result]:
        """Yield one terminal result per input, in observed completion order.

        Each consumed item counts against max_items until its terminal result
        is yielded. Task errors become results; source iteration and backend
        coordination errors are raised to the caller.
        """
        stages = self.stages
        backend = self.backend if self.backend is not None else RayBackend()
        prepared = [backend.prepare(stage) for stage in stages]
        source = enumerate(items)
        done_reading = False
        outstanding = 0
        pending: list[deque[tuple[int, Any]]] = [deque() for _ in stages]
        in_flight = [0] * len(stages)
        active: dict[Any, tuple[int, int]] = {}

        def failure(index: int, stage_index: int, exc: Exception) -> Result:
            return Result(
                index=index, value=None,
                failure=Failure(
                    stage=stages[stage_index].name, error_type=type(exc).__name__,
                    message=str(exc),
                ),
            )

        try:
            while True:
                # Admit only enough inputs to fill the stage-0 slots. The
                # global budget separately counts downstream waiting/active
                # items, so downstream backlog cannot grow without bound.
                for stage_index in range(len(stages) - 1, -1, -1):
                    while in_flight[stage_index] < stages[stage_index].concurrency:
                        if pending[stage_index]:
                            index, value = pending[stage_index].popleft()
                        elif stage_index == 0 and not done_reading and outstanding < self.max_items:
                            try:
                                index, value = next(source)
                            except StopIteration:
                                done_reading = True
                                break
                            outstanding += 1
                        else:
                            break
                        try:
                            ref = backend.submit(prepared[stage_index], value)
                        except Exception as exc:
                            outstanding -= 1
                            yield failure(index, stage_index, exc)
                        else:
                            if ref in active:
                                raise RuntimeError("backend returned a duplicate active handle")
                            active[ref] = stage_index, index
                            in_flight[stage_index] += 1
                if not active:
                    return
                ref = backend.wait(list(active))
                if ref not in active:
                    raise RuntimeError("backend wait returned an unknown handle")
                stage_index, index = active.pop(ref)
                in_flight[stage_index] -= 1
                try:
                    value = backend.get(ref)
                except Exception as exc:
                    outstanding -= 1
                    yield failure(index, stage_index, exc)
                else:
                    if stage_index + 1 < len(stages):
                        pending[stage_index + 1].append((index, value))
                    else:
                        outstanding -= 1
                        yield Result(index=index, value=value, failure=None)
        finally:
            for ref in active:
                try:
                    backend.cancel(ref)
                except Exception:
                    # Cancellation is best-effort; never replace an
                    # iteration or coordination error with a cleanup error.
                    pass
