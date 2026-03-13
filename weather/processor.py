"""
Main weather data processing functionality with GRIB and NetCDF support.
"""

import os
import time
from pathlib import Path
import xarray as xr
import sqlalchemy
from netCDF4 import Dataset
from sqlalchemy import text
from sqlmodel import Session

from coordinates.coordinates import create_coordinates_df
from weather.convert import (
    convert_grib,
    convert_netCFD,
)
from weather.database import create_database_and_tables, engine

from .db_migration import migrate_time_column
from .timer import timer
from definitions import ROOT_DIR


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
    file1_nc = f"{file_name_base}_accum.nc"
    file2_nc = f"{file_name_base}_instant.nc"
    file_grib = f"{file_name_base}.grib"

    file1_nc_path = os.path.join(ROOT_DIR, input_dir, file1_nc)
    file2_nc_path = os.path.join(ROOT_DIR, input_dir, file2_nc)

    file_grib_path =os.path.join(ROOT_DIR, input_dir, file_grib)


    if os.path.exists(file1_nc_path) and os.path.exists(file2_nc_path) and not os.path.exists(file_grib_path):
        # netCdf
        path_found_files = (file1_nc_path, file2_nc_path)
        file_format = "netcdf"

    elif os.path.exists(file_grib_path):
        # Grib
        path_found_files = (file_grib_path, None)
        file_format = "grib"
    else:
        raise FileNotFoundError(
            f"No weather data files found for base name: {file_name_base}\n"
            f"Searched in directory: {input_dir}\n"
            f"Expected formats: NetCDF (.nc) or GRIB (.grib)"
        )

    print(f"Path of found files: {path_found_files}, Format: {file_format}")

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
                        print(
                            f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise Exception(
                            f"Failed to connect after {max_retries} attempts"
                        ) from e

        # Process based on file format
        if file_format == "netcdf":
            # Existing NetCDF processing logic
            accum_file_path, instant_file_path = path_found_files

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

        elif file_format in ["grib"]:
            # GRIB processing logic
            grib_file_path = path_found_files[0]

            with timer("Creating coordinates from GRIB"):
                print(
                    f"Opening GRIB file: {grib_file_path}"
                )
                weather = xr.open_dataset(grib_file_path, engine="cfgrib")
                coordinates_dict = create_coordinates_df(weather, session)
                print(
                    f"Created coordinates dictionary with {len(coordinates_dict)} entries"
                )
                session.commit()
                print("Coordinates committed to database")

            with timer("Converting GRIB weather data"):
                print(f"Starting conversion of GRIB data for {file_name_base}")
                convert_grib(session, grib_file_path, coordinates_dict, batch_size)
                session.commit()
                print("GRIB weather data conversion complete")

    # Perform database migration after all data has been processed
    if perform_migration:
        migrate_time_column()
