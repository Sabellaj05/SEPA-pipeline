import logging
import os

logger = logging.getLogger(__name__)


def configure_langfuse_tracing() -> bool:
    """Enable Langfuse/OpenTelemetry tracing for Google ADK when configured."""
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
