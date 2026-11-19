"""Execution backends for the bounded stage pipeline."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from importlib import import_module
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from .model import Stage


class Backend(Protocol):
    """Asynchronous execution used by the synchronous pipeline scheduler.

    References must be hashable and unique among live submissions.
    Cancellation is best-effort; running work may continue.
    """

    def prepare(self, stage: Stage) -> Any: ...
    def submit(self, prepared: Any, item: Any) -> Any: ...
    def wait(self, refs: Iterable[Any]) -> Any: ...
    def get(self, ref: Any) -> Any: ...
    def cancel(self, ref: Any) -> None: ...


class RayBackend:
    """Run each stage as a Ray task on an already initialized Ray runtime.

    Ray is imported lazily. Each prepared stage carries its CPU, GPU and
    retry settings to Ray. This backend does not own the Ray lifecycle.
    """

    @staticmethod
    def _runtime() -> ModuleType:
        try:
            ray = import_module("ray")
        except ModuleNotFoundError as exc:
            if exc.name == "ray":
                raise RuntimeError(
                    "RayBackend requires Ray; install the ray extra or use ThreadBackend"
                ) from exc
            raise
        if not ray.is_initialized():
            raise RuntimeError("call ray.init() before using RayBackend")
        return ray

    def prepare(self, stage: Stage) -> Any:
        ray = self._runtime()
        fn = stage.fn

        def execute(item: Any) -> Any:
            return fn(item)

        return ray.remote(
            num_cpus=stage.num_cpus,
            num_gpus=stage.num_gpus,
            max_retries=stage.max_retries,
        )(execute)

    def submit(self, prepared: Any, item: Any) -> Any:
        self._runtime()
        return prepared.remote(item)

    def wait(self, refs: Iterable[Any]) -> Any:
        pending = list(refs)
        if not pending:
            raise ValueError("wait requires at least one reference")
        ready, _ = self._runtime().wait(pending, num_returns=1, fetch_local=False)
        return ready[0]

    def get(self, ref: Any) -> Any:
        return self._runtime().get(ref)

    def cancel(self, ref: Any) -> None:
        self._runtime().cancel(ref, force=False)


class ThreadBackend:
    """Run stages in a bounded local thread pool without importing Ray.

    Use as a context manager or call close() explicitly. This backend does
    not enforce Ray resource annotations, run retries, or serialize values.
    Submitted callables must be thread-safe.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="raystages"
        )

    def __enter__(self) -> ThreadBackend:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Cancel queued work and join running worker threads."""
        self._executor.shutdown(wait=True, cancel_futures=True)

    def prepare(self, stage: Stage) -> Callable[[Any], Any]:
        return stage.fn

    def submit(self, prepared: Callable[[Any], Any], item: Any) -> Future[Any]:
        return self._executor.submit(prepared, item)

    def wait(self, refs: Iterable[Future[Any]]) -> Future[Any]:
        pending = tuple(refs)
        if not pending:
            raise ValueError("wait requires at least one reference")
        ready, _ = wait(pending, return_when=FIRST_COMPLETED)
        # Input order is the tie-breaker when several results are ready.
        return next(ref for ref in pending if ref in ready)

    def get(self, ref: Future[Any]) -> Any:
        return ref.result()

    def cancel(self, ref: Future[Any]) -> None:
        ref.cancel()
