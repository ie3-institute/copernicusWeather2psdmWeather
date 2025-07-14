"""
Configuration handling utilities.
"""

import os

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
        raise FileNotFoundError(f"Configuration file {config_path} not found.")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

        config["input_dir"] = str(config["input_dir"])

    return config
