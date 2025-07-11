"""
Configuration handling utilities.
"""

import os
from pathlib import Path

import yaml


def load_config(config_path="config.yaml"):
    """
    Load configuration from YAML file and resolve paths.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        dict: Configuration with resolved paths
    """
    if not os.path.exists(config_path):
        print(f"Configuration file {config_path} not found. Using default values.")
        return {
            "ROOT_DIR": str(Path.cwd()),
            "input_dir": str(Path.cwd() / "input"),
            "file_name_base": "westfalia_2024_06_01-2024_06_07",
            "batch_size": 1000,
        }

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

        config["input_dir"] = str(config["input_dir"])

    return config
