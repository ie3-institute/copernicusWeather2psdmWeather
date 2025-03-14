"""
Logging setup utilities.
"""

import logging
from pathlib import Path


def setup_logging(log_level=logging.INFO, log_file=None):
    """
    Set up logging configuration with optional file output.

    Args:
        log_level: Logging level (default: INFO)
        log_file: Optional path to log file

    Returns:
        Logger: Configured logger
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.StreamHandler()]

    if log_file:
        log_file_path = Path(log_file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
        force=True,  # Force reconfiguration of the root logger
    )
    return logging.getLogger(__name__)
