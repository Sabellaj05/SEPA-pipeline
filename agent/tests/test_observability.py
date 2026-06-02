import os


def test_langfuse_tracing_skips_when_env_is_missing(monkeypatch):
    from agent.observability import configure_langfuse_tracing

    for name in (
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
        "LANGFUSE_BASE_URL",
        "LANGFUSE_TRACING_ENABLED",
    ):
        monkeypatch.delenv(name, raising=False)

    assert configure_langfuse_tracing(load_env=False) is False


def test_langfuse_tracing_can_be_disabled(monkeypatch):
    from agent.observability import configure_langfuse_tracing

    monkeypatch.setenv("LANGFUSE_TRACING_ENABLED", "false")

    assert configure_langfuse_tracing(load_env=False) is False


def test_langfuse_env_file_maps_init_keys_to_runtime_keys(monkeypatch, tmp_path):
    from agent.observability import load_langfuse_environment

    for name in (
        "NEXTAUTH_URL",
        "LANGFUSE_BASE_URL",
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
        "LANGFUSE_INIT_PROJECT_PUBLIC_KEY",
        "LANGFUSE_INIT_PROJECT_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    env_file = tmp_path / ".env.langfuse"
    env_file.write_text(
        "\n".join(
            (
                "NEXTAUTH_URL=http://localhost:3000",
                "LANGFUSE_INIT_PROJECT_PUBLIC_KEY=lf_pk_test",
                "LANGFUSE_INIT_PROJECT_SECRET_KEY=lf_sk_test",
            )
        )
    )

    load_langfuse_environment(env_file)

    assert os.getenv("LANGFUSE_BASE_URL") == "http://localhost:3000"
    assert os.getenv("LANGFUSE_PUBLIC_KEY") == "lf_pk_test"
    assert os.getenv("LANGFUSE_SECRET_KEY") == "lf_sk_test"
