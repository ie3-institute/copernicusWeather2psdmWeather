"""
Configuration handling utilities.
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path="config.yaml"):
    """
    Load configuration from YAML file and resolve paths.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        dict: Configuration with resolved paths
    """
    if not os.path.exists(config_path):
        logger.warning(
            f"Configuration file {config_path} not found. Using default values."
        )
        return {
            "ROOT_DIR": str(Path.cwd()),
            "input_dir": str(Path.cwd() / "input"),
            "file_name_base": "westfalia_2024_06_01-2024_06_07",
            "batch_size": 1000,
            "log_file": None,
        }

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Resolve the ROOT_DIR if it's relative
    if "ROOT_DIR" in config:
        root_dir = Path(config["ROOT_DIR"]).expanduser().resolve()
        config["ROOT_DIR"] = str(root_dir)

        # Resolve any relative paths using ROOT_DIR
        if "input_dir" in config and not os.path.isabs(config["input_dir"]):
            config["input_dir"] = str(root_dir / config["input_dir"])

        if (
            "log_file" in config
            and config["log_file"]
            and not os.path.isabs(config["log_file"])
        ):
            config["log_file"] = str(root_dir / config["log_file"])
    else:
        # If ROOT_DIR not provided, use current working directory
        config["ROOT_DIR"] = str(Path.cwd())

        # If input_dir is not absolute and ROOT_DIR wasn't provided, resolve against cwd
        if "input_dir" not in config:
            config["input_dir"] = str(Path.cwd() / "input")

    return config
