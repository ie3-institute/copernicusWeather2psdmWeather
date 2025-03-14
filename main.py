#!/usr/bin/env python
"""
Main entry point for weather data processing application.
"""
import argparse
from weather.config import load_config
from weather.logging_setup import setup_logging
from weather.processor import process_weather_data


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Process weather data from NetCDF files.')
    parser.add_argument('--config', dest='config_path', type=str,
                        default="settings.yaml",
                        help='Path to YAML configuration file')
    parser.add_argument('--batch-size', dest='batch_size', type=int,
                        help='Override batch size from config file')
    parser.add_argument('--log-file', dest='log_file', type=str,
                        help='Override log file path from config file')
    return parser.parse_args()


def main():
    # Set up initial logging for argument parsing
    logger = setup_logging()

    # Parse command line arguments
    args = parse_arguments()

    try:
        # Load configuration from YAML
        config = load_config(args.config_path)

        # Command line arguments override configuration file
        batch_size = args.batch_size if args.batch_size is not None else config.get('batch_size', 1000)
        log_file = args.log_file if args.log_file is not None else config.get('log_file')

        # Get input directory from config
        input_dir = config.get('input_dir')
        file_name_base = config.get('file_name_base')

        # Re-configure logging with settings from config
        logger = setup_logging(log_file=log_file)

        logger.info(f"Loaded configuration from {args.config_path}")
        logger.info(f"Using ROOT_DIR: {config.get('ROOT_DIR')}")
        logger.info(f"Using input directory: {input_dir}")
        logger.info(f"Using file name base: {file_name_base}")
        logger.info(f"Using batch size: {batch_size}")

        logger.info("Starting weather data processing")
        process_weather_data(input_dir, file_name_base, batch_size)
        logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())