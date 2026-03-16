import glob
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import xarray as xr
from netCDF4 import Dataset, num2date
from pypsdm.db.weather.models import WeatherValue
from sqlmodel import Session

BASE_TIME = datetime(year=1970, month=1, day=1, tzinfo=pytz.utc)

logger = logging.getLogger(__name__)


def convert_netCFD(
    session: Session,
    accum_data: Dataset,
    instant_data: Dataset,
    coordinates: dict,
    batch_size: int = 1000,
):
    """
    Converts raw weather data from two NetCDF datasets into database entries.

    Args:
        session: SQLModel database session
        accum_data: Dataset containing accumulated weather variables
        instant_data: Dataset containing instantaneous weather variables
        coordinates: Dictionary mapping (lat_idx, lon_idx) to coordinate_id
        batch_size: Number of records to process before committing to database
    """
    # Extract time units and time values from the instant data
    time_units = instant_data.variables["valid_time"].units
    time_values = instant_data.variables["valid_time"][:]
    time_objects = num2date(time_values, units=time_units)

    # Convert time objects to formatted strings once
    formatted_times = [t.strftime("%Y-%m-%dT%H:%M:%S") + "Z" for t in time_objects]

    # Get variables as numpy arrays to avoid repeated access
    temp_array = instant_data.variables["t2m"][:]
    u100_array = instant_data.variables["u100"][:]
    v100_array = instant_data.variables["v100"][:]
    fdir_array = accum_data.variables["fdir"][:]
    ssrd_array = accum_data.variables["ssrd"][:]

    # Batch processing
    weather_values = []
    total_records = 0

    for time_idx, time_value in enumerate(formatted_times):
        print(f"Processing time index {time_idx}/{len(formatted_times)}")

        for (lat_idx, lon_idx), coordinate_id in coordinates.items():
            # Extract variables from arrays
            temp = float(temp_array[time_idx, lat_idx, lon_idx])
            u131m = float(u100_array[time_idx, lat_idx, lon_idx])
            v131m = float(v100_array[time_idx, lat_idx, lon_idx])

            fDir = float(fdir_array[time_idx, lat_idx, lon_idx])
            influx_total = float(ssrd_array[time_idx, lat_idx, lon_idx])

            # Create WeatherValue object
            weather_value = WeatherValue(
                time=datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%SZ"),
                coordinate_id=coordinate_id,
                aswdifd_s=(influx_total - fDir) / 3600,  # Difference of ssdr - fDir
                aswdir_s=fDir / 3600,  # J/m² in Wh/m²
                t2m=temp,
                u131m=u131m,
                v131m=v131m,
            )

            weather_values.append(weather_value)
            total_records += 1

            # Commit in batches to avoid memory issues
            if len(weather_values) >= batch_size:
                session.add_all(weather_values)
                session.commit()
                weather_values = []
                print(
                    f"Committed {batch_size} records. Total processed: {total_records}"
                )

    # Add any remaining weather values
    if weather_values:
        session.add_all(weather_values)
        session.commit()
        print(
            f"Final commit: {len(weather_values)} records. Total processed: {total_records}"
        )


def convert_grib(
    session: Session,
    grib_file_path: str,
    coordinates: dict,
    batch_size: int = 1000,
):
    """
    Converts raw weather data from GRIB file into database entries using cfgrib/xarray.

    Args:
        session: SQLModel database session
        grib_file_path: Path to the GRIB file
        coordinates: Dictionary mapping (lat_idx, lon_idx) to coordinate_id
        batch_size: Number of records to process before committing to database
    """
    logger.info(f"Opening GRIB file with xarray: {grib_file_path}")
    try:
        # Open GRIB file
        ds_fdir = xr.open_dataset(
            grib_file_path, engine="cfgrib", filter_by_keys={"shortName": "fdir"}
        )
        ds_ssrd = xr.open_dataset(
            grib_file_path, engine="cfgrib", filter_by_keys={"shortName": "ssrd"}
        )
        ds_t2m = xr.open_dataset(
            grib_file_path, engine="cfgrib", filter_by_keys={"shortName": "2t"}
        )
        ds_u100 = xr.open_dataset(
            grib_file_path, engine="cfgrib", filter_by_keys={"shortName": "100u"}
        )
        ds_v100 = xr.open_dataset(
            grib_file_path, engine="cfgrib", filter_by_keys={"shortName": "100v"}
        )

        # Use temperature dataset for time reference
        if "time" not in ds_t2m.coords:
            raise ValueError("No time coordinate found in GRIB file")

        time_values = ds_t2m["time"].values
        logger.info(f"Found {len(time_values)} time steps using coordinate 'time'")

        # Process data
        weather_values = []
        total_records = 0

        time_objects = [pd.to_datetime(t).to_pydatetime() for t in time_values]

        for time_idx, time_obj in enumerate(time_objects):

            # Get variable data
            temp_data = ds_t2m["t2m"].isel(time=time_idx).values
            u_wind_data = ds_u100["u100"].isel(time=time_idx).values
            v_wind_data = ds_v100["v100"].isel(time=time_idx).values

            for (lat_idx, lon_idx), coordinate_id in coordinates.items():
                try:

                    # Get the time step for hourly accumulated weather data (radiation)
                    valid_times = ds_ssrd["valid_time"].values  # shape (time, step)
                    target_time = np.datetime64(time_obj)
                    match = np.where(valid_times == target_time)
                    if match[0].size == 0:
                        logger.warning(
                            f"No matching valid_time for {target_time}, skipping."
                        )
                        continue
                    ssrd_time_idx, ssrd_step_idx = match[0][0], match[1][0]
                    ssrd_val = ds_ssrd["ssrd"].values[
                        ssrd_time_idx, ssrd_step_idx, lat_idx, lon_idx
                    ]
                    fdir_time_idx, fdir_step_idx = match[0][0], match[1][0]
                    fdir_val = ds_fdir["fdir"].values[
                        fdir_time_idx, fdir_step_idx, lat_idx, lon_idx
                    ]

                    ssrd = float(
                        ssrd_val.item() if hasattr(ssrd_val, "item") else ssrd_val
                    )
                    fdir = float(
                        fdir_val.item() if hasattr(fdir_val, "item") else fdir_val
                    )

                    # Extract the other weather values for this coordinate
                    temp = float(temp_data[lat_idx, lon_idx])
                    u_wind = float(u_wind_data[lat_idx, lon_idx])
                    v_wind = float(v_wind_data[lat_idx, lon_idx])

                    # Raise error if any value is NaN
                    if np.isnan([temp, u_wind, v_wind, ssrd, fdir]).any():
                        raise ValueError("NaN Value occurred during conversion.")

                    # Create WeatherValue object
                    weather_value = WeatherValue(
                        time=time_obj,
                        coordinate_id=coordinate_id,
                        aswdifd_s=(ssrd - fdir)
                        / 3600,  # Diffuse radiation (J/m² to Wh/m²)
                        aswdir_s=fdir / 3600,  # Direct radiation (J/m² to Wh/m²)
                        t2m=temp,
                        u131m=u_wind,
                        v131m=v_wind,
                    )

                    weather_values.append(weather_value)
                    total_records += 1

                    # Commit in batches
                    if len(weather_values) >= batch_size:
                        session.add_all(weather_values)
                        session.commit()
                        weather_values = []
                        logger.info(
                            f"Committed {batch_size} records. Total processed: {total_records}"
                        )

                except (IndexError, ValueError) as e:
                    logger.warning(
                        f"Error processing coordinate ({lat_idx}, {lon_idx}) at time {time_obj}: {e}"
                    )
                    continue

        # Add any remaining weather values
        if weather_values:
            session.add_all(weather_values)
            session.commit()
            logger.info(
                f"Final commit: {len(weather_values)} records. Total processed: {total_records}"
            )

    except Exception as e:
        logger.error(f"Error processing GRIB file: {e}", exc_info=True)
        raise
    finally:
        # Clean up cfgrib index files
        idx_pattern = grib_file_path + ".*.idx"
        for idx_file in glob.glob(idx_pattern):
            try:
                os.remove(idx_file)
                logger.info(f"Deleted cfgrib index file: {idx_file}")
            except Exception as e:
                logger.warning(f"Could not delete index file {idx_file}: {e}")