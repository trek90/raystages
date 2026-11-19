from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from raystages.backends import RayBackend, ThreadBackend


def test_ray_import_is_lazy_and_requires_initialization(monkeypatch):
    ray = Mock()
    ray.is_initialized.return_value = False
    importer = Mock(return_value=ray)
    monkeypatch.setattr("raystages.backends.import_module", importer)
    backend = RayBackend()
    importer.assert_not_called()
    with pytest.raises(RuntimeError, match=r"call ray\.init"):
        backend.prepare(None)
    importer.assert_called_once_with("ray")
    ray.remote.assert_not_called()


def test_ray_backend_delegates_remote_operations(monkeypatch):
    ray = Mock()
    ray.is_initialized.return_value = True
    monkeypatch.setattr("raystages.backends.import_module", lambda name: ray)
    backend = RayBackend()
    stage = SimpleNamespace(fn=lambda x: x, num_cpus=0.5, num_gpus=1, max_retries=2)
    prepared = backend.prepare(stage)
    ray.remote.assert_called_once_with(num_cpus=0.5, num_gpus=1, max_retries=2)
    ray.remote.return_value.assert_called_once()
    worker = ray.remote.return_value.call_args.args[0]
    assert worker("payload") == "payload"
    ref = backend.submit(prepared, "payload")
    prepared.remote.assert_called_once_with("payload")
    assert ref is prepared.remote.return_value
    ray.wait.return_value = ([ref], ["other"])
    assert backend.wait(iter([ref, "other"])) is ref
    ray.wait.assert_called_once_with([ref, "other"], num_returns=1, fetch_local=False)
    assert backend.get(ref) is ray.get.return_value
    ray.get.assert_called_once_with(ref)
    backend.cancel(ref)
    ray.cancel.assert_called_once_with(ref, force=False)


@pytest.mark.parametrize("backend_factory", [RayBackend, ThreadBackend])
def test_backend_wait_rejects_empty_refs(backend_factory):
    backend = backend_factory()
    try:
        with pytest.raises(ValueError, match="at least one reference"):
            backend.wait(iter(()))
    finally:
        if isinstance(backend, ThreadBackend):
            backend.close()


def test_thread_backend_runs_and_closes():
    stage = SimpleNamespace(fn=lambda x: x * 2)
    with ThreadBackend(max_workers=1) as backend:
        prepared = backend.prepare(stage)
        ref = backend.submit(prepared, 8)
        assert backend.get(backend.wait([ref])) == 16
    with pytest.raises(RuntimeError):
        backend.submit(prepared, 1)
