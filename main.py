#!/usr/bin/env python
"""
Utility script for processing weather data from GRIB and NetCDF files.
"""
import argparse
import sys
from pathlib import Path

from weather.config import load_config
from weather.convert import inspect_grib_file
from weather.logging_setup import setup_logging
from weather.processor import process_weather_data

# Add the parent directory to the path to import our modules
sys.path.append(str(Path(__file__).parent.parent))


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Process weather data from GRIB and NetCDF files."
    )
    parser.add_argument(
        "--config",
        dest="config_path",
        type=str,
        default="settings.yaml",
        help="Path to YAML configuration file",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subparser for inspecting GRIB files (no longer takes an argument)
    grib_parser = subparsers.add_parser("inspect-grib", help="Inspect a GRIB file")

    grib_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    # Subparser for processing NetCDF files
    netcdf_parser = subparsers.add_parser("process-netcdf", help="Process NetCDF files")

    netcdf_parser.add_argument(
        "--config",
        dest="config_path",
        type=str,
        default="settings.yaml",
        help="Path to YAML configuration file",
    )
    netcdf_parser.add_argument(
        "--batch-size",
        dest="batch_size",
        type=int,
        help="Override batch size from config file",
    )
    netcdf_parser.add_argument(
        "--log-file",
        dest="log_file",
        type=str,
        help="Override log file path from config file",
    )
    netcdf_parser.add_argument(
        "--no-migration",
        dest="perform_migration",
        action="store_false",
        help="Skip database migration after processing",
    )

    netcdf_parser.set_defaults(perform_migration=True)

    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.command == "inspect-grib":
        # Setup logging for inspecting GRIB files
        log_level = logging.DEBUG if args.verbose else logging.INFO
        logger = setup_logging(log_level=log_level)

        config = load_config(args.config_path)
        input_dir = config.get("input_dir")
        file_name_base = config.get("file_name_base")

        # Construct full path to the GRIB file using input_dir and file_name_base
        grib_file_path = (
            Path(input_dir) / f"{file_name_base}.grib"
        )  # Adjust extension as needed

        # Check if file exists
        if not grib_file_path.exists():
            print(f"Error: File '{grib_file_path}' not found")
            return 1

        try:
            inspect_grib_file(grib_file_path)
            logger.info("GRIB file inspection completed successfully.")
            return 0
        except Exception as e:
            logger.error(f"Error inspecting GRIB file: {e}", exc_info=True)
            return 1

    elif args.command == "process-netcdf":

        # Set up initial logging for argument parsing
        logger = setup_logging()

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

            input_dir = config.get("input_dir")
            file_name_base = config.get("file_name_base")

            logger.info(f"Loaded configuration from {args.config_path}")
            logger.info(f"Using ROOT_DIR: {config.get('ROOT_DIR')}")
            logger.info(f"Using input directory: {input_dir}")
            logger.info(f"Using file name base: {file_name_base}")
            logger.info(f"Using batch size: {batch_size}")
            logger.info(
                f"Database migration: {'Enabled' if perform_migration else 'Disabled'}"
            )

            logger.info("Starting weather data processing")
            process_weather_data(
                input_dir, file_name_base, batch_size, perform_migration
            )
            logger.info("Processing completed successfully")
        except Exception as e:
            logger.error(f"Error during processing: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    import logging

    exit(main())
