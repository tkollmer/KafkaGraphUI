"""Startup validation — fail-fast on invalid config before Kafka connection."""

import sys
import logging

from config import Config

logger = logging.getLogger(__name__)


def validate_and_exit_on_error(cfg: Config) -> None:
    """Validate config. Exit code 1 with clear message on failure."""
    errors = cfg.validate()
    if errors:
        for err in errors:
            print(f"STARTUP ERROR: {err}", file=sys.stderr)
        sys.exit(1)
    logger.info("Configuration validated successfully")
