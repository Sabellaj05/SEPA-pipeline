def test_langfuse_tracing_skips_when_env_is_missing(monkeypatch):
    from agent.observability import configure_langfuse_tracing

    for name in (
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
        "LANGFUSE_BASE_URL",
        "LANGFUSE_TRACING_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)

    assert configure_langfuse_tracing() is False


def test_langfuse_tracing_can_be_disabled(monkeypatch):
    from agent.observability import configure_langfuse_tracing

    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")

    assert configure_langfuse_tracing() is False
