import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def _default_langfuse_env_file() -> Path:
    return Path(__file__).resolve().parents[1] / ".env.langfuse"


def load_langfuse_environment(env_file: str | os.PathLike[str] | None = None) -> None:
    """Load local Langfuse env and normalize init keys to SDK runtime keys."""
    if env_file is None:
        env_path = _default_langfuse_env_file()
    else:
        env_path = Path(env_file)

    if env_path.exists():
        load_dotenv(env_path, override=False)

    defaults = {
        "LANGFUSE_BASE_URL": os.getenv("NEXTAUTH_URL"),
        "LANGFUSE_PUBLIC_KEY": os.getenv("LANGFUSE_INIT_PROJECT_PUBLIC_KEY"),
        "LANGFUSE_SECRET_KEY": os.getenv("LANGFUSE_INIT_PROJECT_SECRET_KEY"),
    }
    for name, value in defaults.items():
        if value and not os.getenv(name):
            os.environ[name] = value


def configure_langfuse_tracing(
    env_file: str | os.PathLike[str] | None = None,
    *,
    load_env: bool = True,
) -> bool:
    """Enable Langfuse/OpenTelemetry tracing for Google ADK when configured."""
    if load_env:
        load_langfuse_environment(env_file)

    if os.getenv("LANGFUSE_TRACING_ENABLED", "true").lower() in {"0", "false", "no"}:
        logger.info("Langfuse tracing disabled by LANGFUSE_TRACING_ENABLED.")
        return False

    if not (
        os.getenv("LANGFUSE_PUBLIC_KEY")
        and os.getenv("LANGFUSE_SECRET_KEY")
        and os.getenv("LANGFUSE_BASE_URL")
    ):
        logger.info("Langfuse tracing not configured; missing Langfuse env vars.")
        return False

    try:
        from langfuse import get_client
        from openinference.instrumentation.google_adk import GoogleADKInstrumentor
    except ImportError as exc:
        logger.warning("Langfuse tracing packages are unavailable: %s", exc)
        return False

    get_client()

    instrumentor = GoogleADKInstrumentor()
    if not instrumentor.is_instrumented_by_opentelemetry:
        instrumentor.instrument()

    logger.info("Langfuse tracing enabled for Google ADK.")
    return True
