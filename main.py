#!/usr/bin/env python
"""
Main entry point for weather data processing application.
"""

from weather.config import load_config
from weather.processor import process_weather_data


def convert_cds_weather(config_path=None):
    if config_path is None:
        raise Exception("Config path must be provided")

    try:
        # Load configuration from YAML
        config = load_config(config_path)
        batch_size = 1000
        perform_migration = True

        # Get input directory from config
        input_dir = config.get("input_dir")
        file_name_base = config.get("file_name_base")

        print(f"Loaded configuration from {config_path}")
        print(f"Using ROOT_DIR: {config.get('ROOT_DIR')}")
        print(f"Using input directory: {input_dir}")
        print(f"Using file name base: {file_name_base}")
        print(f"Using batch size: {batch_size}")
        print(f"Database migration: {'Enabled' if perform_migration else 'Disabled'}")

        print("Starting weather data processing")
        process_weather_data(
            config_path, input_dir, file_name_base, batch_size, perform_migration
        )
        print("Processing completed successfully")
    except Exception as e:
        print(f"Error during processing: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(convert_cds_weather("config.yaml"))
