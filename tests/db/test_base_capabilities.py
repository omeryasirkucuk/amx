"""Unit tests for BackendCapabilities dataclass."""


def test_backend_capabilities_defaults_remote_flags_false():
    from amx.db.adapters.base import BackendCapabilities

    caps = BackendCapabilities()
    assert caps.remote_notebooks is False
    assert caps.remote_jobs is False
    assert caps.remote_pipelines is False
    assert caps.remote_streamlit_apps is False
    assert caps.remote_streams is False
    assert caps.remote_task_dependencies is False
    assert caps.remote_queries is False
