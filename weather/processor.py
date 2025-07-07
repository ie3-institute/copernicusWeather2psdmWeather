"""
Main weather data processing functionality.
"""

import os
import time

import sqlalchemy
from netCDF4 import Dataset
from sqlalchemy import text
from sqlmodel import Session

from coordinates.coordinates import create_coordinates_df
from weather.convert import convert
from weather.database import create_database_and_tables, engine

from .db_migration import migrate_time_column
from .timer import timer

def process_weather_data(
    input_dir, file_name_base, batch_size=1000, perform_migration=True
):
    """
    Process weather data from NetCDF files and store in database.

    Args:
        input_dir: Directory containing the NetCDF files
        file_name_base: Base name of the input files without _accum.nc or _instant.nc
        batch_size: Number of records to process before committing to database

    Raises:
        FileNotFoundError: If input files don't exist
        OSError: If there are issues accessing the files
        Exception: For other errors during processing
    """
    accum_file_name = f"{file_name_base}_accum.nc"
    instant_file_name = f"{file_name_base}_instant.nc"

    accum_file_path = os.path.join(input_dir, accum_file_name)
    instant_file_path = os.path.join(input_dir, instant_file_name)

    # Validate input files exist
    for file_path, file_desc in [
        (accum_file_path, "accumulated data"),
        (instant_file_path, "instant data"),
    ]:
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"NetCDF file for {file_desc} not found: {file_path}"
            )

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

        with timer("Loading NetCDF files"):
            print(
                f"Opening NetCDF files: {accum_file_name} and {instant_file_name}"
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
            print(f"Starting conversion of data for {file_name_base}")
            convert(session, accum_data, instant_data, coordinates_dict, batch_size)
            session.commit()
            print("Weather data conversion complete")

        # Close the datasets
        accum_data.close()
        instant_data.close()

    # Perform database migration after all data has been processed
    if perform_migration:
        migrate_time_column()
