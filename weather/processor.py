"""
Main weather data processing functionality with GRIB and NetCDF support.
"""

import os
import time
from pathlib import Path

import sqlalchemy
from netCDF4 import Dataset
from sqlalchemy import text
from sqlmodel import Session

from coordinates.coordinates import create_coordinates_df, insert_coordinate
from weather.convert import (
    convert_grib,
    convert_netCFD,
    get_grib_coordinates,
    inspect_grib_file,
)
from weather.database import create_database_and_tables, engine

from .db_migration import migrate_time_column
from .timer import timer


def detect_file_format(file_path):
    """
    Detect whether a file is NetCDF or GRIB format.

    Args:
        file_path: Path to the file

    Returns:
        str: 'netcdf', 'grib', or 'unknown'
    """
    file_ext = Path(file_path).suffix.lower()

    if file_ext in [".nc", ".nc4", ".netcdf"]:
        return "netcdf"
    elif file_ext in [".grib", ".grb", ".grib2", ".grb2"]:
        return "grib"
    else:
        # Try to detect by file content
        try:
            # Try opening as NetCDF
            ds = Dataset(file_path, "r")
            ds.close()
            return "netcdf"
        except OSError:
            try:
                # Try opening as GRIB
                import pygrib

                grbs = pygrib.open(file_path)
                grbs.close()
                return "grib"
            except (IOError, ValueError):
                return "unknown"


def create_coordinates_from_grib(grib_file_path, session):
    """
    Create coordinates dictionary from GRIB file.

    Args:
        grib_file_path: Path to GRIB file
        session: Database session

    Returns:
        dict: Dictionary mapping (lat_idx, lon_idx) to coordinate_id
    """

    lats, lons = get_grib_coordinates(grib_file_path)
    coordinates_dict = {}

    logger.info(
        f"Creating coordinates from GRIB file: {lats.shape[0]}x{lats.shape[1]} grid"
    )

    for lat_idx in range(lats.shape[0]):
        for lon_idx in range(lats.shape[1]):
            lat = float(lats[lat_idx, lon_idx])
            lon = float(lons[lat_idx, lon_idx])

            coordinate_id = insert_coordinate(session, lat, lon)
            coordinates_dict[(lat_idx, lon_idx)] = coordinate_id

    return coordinates_dict


def process_weather_data(
    input_dir, file_name_base, batch_size=1000, perform_migration=True
):
    """
    Process weather data from NetCDF or GRIB files and store in database.

    Args:
        input_dir: Directory containing the weather files
        file_name_base: Base name of the input files
        batch_size: Number of records to process before committing to database
        perform_migration: Whether to perform database migration after processing

    Raises:
        FileNotFoundError: If input files don't exist
        OSError: If there are issues accessing the files
        Exception: For other errors during processing
    """

    # Try to find files with different extensions and formats
    possible_files = [
        # NetCDF format (existing)
        (f"{file_name_base}_accum.nc", f"{file_name_base}_instant.nc", "netcdf_split"),
        # Single NetCDF file
        (f"{file_name_base}.nc", None, "netcdf_single"),
        # GRIB format
        (f"{file_name_base}.grib", None, "grib_single"),
        (f"{file_name_base}.grb", None, "grib_single"),
        (f"{file_name_base}.grib2", None, "grib_single"),
        (f"{file_name_base}.grb2", None, "grib_single"),
    ]

    found_files = None
    file_format = None

    for file1, file2, format_type in possible_files:
        file1_path = os.path.join(input_dir, file1)

        if os.path.exists(file1_path):
            if file2 is None:
                # Single file format
                found_files = (file1_path, None)
                file_format = format_type
                break
            else:
                # Two-file format
                file2_path = os.path.join(input_dir, file2)
                if os.path.exists(file2_path):
                    found_files = (file1_path, file2_path)
                    file_format = format_type
                    break

    if found_files is None:
        raise FileNotFoundError(
            f"No weather data files found for base name: {file_name_base}\n"
            f"Searched in directory: {input_dir}\n"
            f"Expected formats: NetCDF (.nc) or GRIB (.grib, .grb, .grib2, .grb2)"
        )

    logger.info(f"Found files: {found_files}, Format: {file_format}")

    with Session(engine) as session:
        with timer("Database initialization"):
            max_retries = 5
            retry_delay = 1  # seconds
            for attempt in range(max_retries):
                try:
                    # Enable PostGIS
                    with engine.connect() as conn:
                        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
                        conn.commit()

                    create_database_and_tables()
                    print("Database and tables created successfully")
                    break

                except sqlalchemy.exc.OperationalError as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise Exception(
                            f"Failed to connect after {max_retries} attempts"
                        ) from e

        # Process based on file format
        if file_format == "netcdf_split":
            # Existing NetCDF processing logic
            accum_file_path, instant_file_path = found_files

            with timer("Loading NetCDF files"):
                print(
                    f"Opening NetCDF files: {accum_file_path} and {instant_file_path}"
                )
                accum_data = Dataset(accum_file_path, "r", format="NETCDF4")
                instant_data = Dataset(instant_file_path, "r", format="NETCDF4")

            # Print dataset information
            print(f"Accumulated data dimensions: {accum_data.dimensions}")
            print(f"Instant data dimensions: {instant_data.dimensions}")

            with timer("Creating coordinates"):
                coordinates_dict = create_coordinates_df(instant_data, session)
                print(
                    f"Created coordinates dictionary with {len(coordinates_dict)} entries"
                )
                session.commit()
                print("Coordinates committed to database")

            with timer("Converting weather data"):
                print(f"Starting conversion of NetCDF data for {file_name_base}")
                convert_netCFD(
                    session, accum_data, instant_data, coordinates_dict, batch_size
                )
                session.commit()
                print("Weather data conversion complete")

            # Close the datasets
            accum_data.close()
            instant_data.close()

        elif file_format in ["grib_single"]:
            # GRIB processing logic
            grib_file_path = found_files[0]

            # Optional: Inspect GRIB file structure for debugging
            if logger.isEnabledFor(logging.DEBUG):
                inspect_grib_file(grib_file_path)

            with timer("Creating coordinates from GRIB"):
                coordinates_dict = create_coordinates_from_grib(grib_file_path, session)
                logger.info(
                    f"Created coordinates dictionary with {len(coordinates_dict)} entries"
                )
                session.commit()
                logger.info("Coordinates committed to database")

            with timer("Converting GRIB weather data"):
                logger.info(f"Starting conversion of GRIB data for {file_name_base}")
                convert_grib(session, grib_file_path, coordinates_dict, batch_size)
                session.commit()
                logger.info("GRIB weather data conversion complete")

        elif file_format == "netcdf_single":
            # Single NetCDF file processing
            netcdf_file_path = found_files[0]

            with timer("Loading single NetCDF file"):
                logger.info(f"Opening NetCDF file: {netcdf_file_path}")
                dataset = Dataset(netcdf_file_path, "r", format="NETCDF4")
                logger.info(f"Dataset dimensions: {dataset.dimensions}")

            with timer("Creating coordinates"):
                coordinates_dict = create_coordinates_df(dataset, session)
                logger.info(
                    f"Created coordinates dictionary with {len(coordinates_dict)} entries"
                )
                session.commit()
                logger.info("Coordinates committed to database")

            with timer("Converting weather data"):
                logger.info(
                    f"Starting conversion of single NetCDF data for {file_name_base}"
                )
                # You might need to create a separate converter for single NetCDF files
                # or modify the existing convert function
                convert_netCFD(session, dataset, dataset, coordinates_dict, batch_size)
                session.commit()
                logger.info("Weather data conversion complete")

            dataset.close()

    # Perform database migration after all data has been processed
    if perform_migration:
        migrate_time_column()
