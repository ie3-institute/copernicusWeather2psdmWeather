#!/usr/bin/env python
"""
Main entry point for weather data processing application.
"""

import argparse

from weather.config import load_config
from weather.processor import process_weather_data


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Process weather data from NetCDF files."
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=str,
        default="config.yaml",
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        help="Override batch size from config file",
    )
    parser.set_defaults(perform_migration=True)
    return parser.parse_args()


def main():
    # Parse command line arguments
    args = parse_arguments()

    try:
        # Load configuration from YAML
        config = load_config(args.config_path)

        # Command line arguments override configuration file
        batch_size = (
            args.batch_size
            if args.batch_size is not None
            else config.get("batch_size", 1000)
        )
        perform_migration = True

        # Get input directory from config
        input_dir = config.get("input_dir")
        file_name_base = config.get("file_name_base")

        print(f"Loaded configuration from {args.config_path}")
        print(f"Using ROOT_DIR: {config.get('ROOT_DIR')}")
        print(f"Using input directory: {input_dir}")
        print(f"Using file name base: {file_name_base}")
        print(f"Using batch size: {batch_size}")
        print(f"Database migration: {'Enabled' if perform_migration else 'Disabled'}")

        print("Starting weather data processing")
        process_weather_data(input_dir, file_name_base, batch_size, perform_migration)
        print("Processing completed successfully")
    except Exception as e:
        print(f"Error during processing: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
