"""Pipeline configuration and result types."""
from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Stage:
    """One synchronous transformation and its task submission limits."""

    name: str
    fn: Callable[[Any], Any]
    concurrency: int = 2
    num_cpus: float = 1.0
    num_gpus: float = 0.0
    max_retries: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("stage name must be nonempty")
        if not callable(self.fn):
            raise TypeError("stage fn must be callable")
        if type(self.concurrency) is not int or self.concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        if type(self.max_retries) is not int or self.max_retries < 0:
            raise ValueError("max_retries must be a nonnegative integer")
        for key in ("num_cpus", "num_gpus"):
            value = getattr(self, key)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{key} must be a finite nonnegative number")


@dataclass(frozen=True, slots=True)
class Failure:
    """The stage and exception summary for a failed item."""

    stage: str
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class Result:
    """One terminal outcome; None is also a valid success value."""

    index: int
    value: Any = None
    failure: Failure | None = None

    @property
    def ok(self) -> bool:
        return self.failure is None
