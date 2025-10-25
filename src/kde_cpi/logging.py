"""Centralized structlog configuration helpers."""

from __future__ import annotations

import logging
import sys

import structlog
from structlog.types import Processor

LOG_LEVELS: dict[str, int] = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


def configure_logging(
    level: str = "info",
    *,
    json_output: bool = False,
) -> None:
    """Initialize structlog with a consistent processor chain."""

    normalized = level.lower()
    if normalized not in LOG_LEVELS:
        valid = ", ".join(sorted(LOG_LEVELS))
        raise ValueError(f"Unsupported log level {level!r}. Choose one of: {valid}.")
    level_value = LOG_LEVELS[normalized]

    logging.basicConfig(level=level_value, format="%(message)s", stream=sys.stderr)

    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    renderer: structlog.types.Processor
    if json_output:
        renderer = structlog.processors.JSONRenderer(sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level_value),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


__all__ = ["configure_logging", "LOG_LEVELS"]
