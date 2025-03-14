"""
Performance timing utilities.
"""
import time
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@contextmanager
def timer(description):
    """
    Context manager for timing code execution.

    Args:
        description: Description of the operation being timed
    """
    start = time.time()
    yield
    elapsed = time.time() - start
    logger.info(f"{description} completed in {elapsed:.2f} seconds")