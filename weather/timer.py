"""
Performance timing utilities.
"""

import time
from contextlib import contextmanager

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
    print(f"{description} completed in {elapsed:.2f} seconds")
