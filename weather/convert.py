import logging
from datetime import datetime

import numpy as np
import pytz
import xarray as xr
from netCDF4 import Dataset, num2date
from pypsdm.db.weather.models import WeatherValue
from sqlmodel import Session

# Correct base time: 1970-01-01 for "seconds since 1970-01-01"
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
        # Open GRIB file with cfgrib backend
        ds = xr.open_dataset(grib_file_path, engine="cfgrib")

        logger.info(f"GRIB dataset variables: {list(ds.variables.keys())}")
        logger.info(f"GRIB dataset dimensions: {dict(ds.dims)}")
        logger.info(f"GRIB dataset coordinates: {list(ds.coords.keys())}")

        # Get time dimension - cfgrib usually uses 'time' or 'valid_time'
        time_coord = None
        for coord_name in ["time", "valid_time", "forecast_time"]:
            if coord_name in ds.coords:
                time_coord = coord_name
                break

        if time_coord is None:
            raise ValueError("No time coordinate found in GRIB file")

        # Get time values
        time_values = ds[time_coord].values
        logger.info(
            f"Found {len(time_values)} time steps using coordinate '{time_coord}'"
        )

        # Map common GRIB variable names to our expected names
        # You may need to adjust these based on your specific GRIB file
        variable_mapping = {
            # Temperature at 2m
            "t2m": ["t2m", "2t", "T_2M", "TMP_2maboveground"],
            # Wind components at 100m (or closest available)
            "u100m": ["u100", "u100m", "U_100M", "100u", "UGRD_100maboveground"],
            "v100m": ["v100", "v100m", "V_100M", "100v", "VGRD_100maboveground"],
            # Solar radiation
            "ssrd": [
                "ssrd",
                "ssr",
                "DSWRF_surface",
                "surface_downwelling_shortwave_flux",
            ],
            "fdir": ["fdir", "direct_normal_irradiance", "DNI", "direct_solar"],
        }

        # Find actual variable names in the dataset
        actual_variables = {}
        for our_name, possible_names in variable_mapping.items():
            found = False
            for possible_name in possible_names:
                if possible_name in ds.variables:
                    actual_variables[our_name] = possible_name
                    logger.info(f"Found {our_name} as '{possible_name}'")
                    found = True
                    break
            if not found:
                logger.warning(
                    f"Could not find variable for {our_name}. Tried: {possible_names}"
                )

        # Check if we have all required variables
        required_vars = ["t2m", "u100m", "v100m", "ssrd"]
        missing_vars = [var for var in required_vars if var not in actual_variables]
        if missing_vars:
            logger.error(f"Missing required variables: {missing_vars}")
            available_vars = list(ds.variables.keys())
            logger.info(f"Available variables in GRIB file: {available_vars}")
            raise ValueError(f"Missing required variables: {missing_vars}")

        # Process data
        weather_values = []
        total_records = 0

        # Convert time values to datetime objects if they aren't already
        if hasattr(time_values[0], "to_pydatetime"):
            time_objects = [t.to_pydatetime() for t in time_values]
        else:
            time_objects = time_values

        for time_idx, time_obj in enumerate(time_objects):
            logger.info(
                f"Processing time {time_idx + 1}/{len(time_objects)}: {time_obj}"
            )

            # Extract data for this time step
            time_slice = ds.isel({time_coord: time_idx})

            # Get variable data
            temp_data = time_slice[actual_variables["t2m"]].values
            u_wind_data = time_slice[actual_variables["u100m"]].values
            v_wind_data = time_slice[actual_variables["v100m"]].values
            ssrd_data = time_slice[actual_variables["ssrd"]].values

            # Handle direct radiation - use ssrd if fdir not available
            if "fdir" in actual_variables:
                fdir_data = time_slice[actual_variables["fdir"]].values
            else:
                logger.warning(
                    "Direct radiation not found, using 50% of total as approximation"
                )
                fdir_data = ssrd_data * 0.5  # Rough approximation

            # Process each coordinate
            for (lat_idx, lon_idx), coordinate_id in coordinates.items():
                try:
                    # Extract values
                    temp = float(temp_data[lat_idx, lon_idx])
                    u_wind = float(u_wind_data[lat_idx, lon_idx])
                    v_wind = float(v_wind_data[lat_idx, lon_idx])
                    ssrd = float(ssrd_data[lat_idx, lon_idx])
                    fdir = float(fdir_data[lat_idx, lon_idx])

                    # Skip if any value is NaN
                    if np.isnan([temp, u_wind, v_wind, ssrd, fdir]).any():
                        continue

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
        if "ds" in locals():
            ds.close()


def get_grib_coordinates(grib_file_path):
    """
    Extract coordinate information from GRIB file using cfgrib/xarray.

    Args:
        grib_file_path: Path to the GRIB file

    Returns:
        tuple: (latitudes, longitudes) 2D arrays
    """
    logger.info(f"Extracting coordinates from GRIB file: {grib_file_path}")

    try:
        # Open GRIB file
        ds = xr.open_dataset(grib_file_path, engine="cfgrib")

        # Get latitude and longitude coordinates
        # cfgrib usually provides these as 'latitude' and 'longitude'
        lat_coord_names = ["latitude", "lat", "y"]
        lon_coord_names = ["longitude", "lon", "x"]

        lat_coord = None
        lon_coord = None

        for name in lat_coord_names:
            if name in ds.coords:
                lat_coord = name
                break

        for name in lon_coord_names:
            if name in ds.coords:
                lon_coord = name
                break

        if lat_coord is None or lon_coord is None:
            raise ValueError(
                f"Could not find latitude/longitude coordinates. Available coords: {list(ds.coords.keys())}"
            )

        # Get coordinate arrays
        lats = ds[lat_coord].values
        lons = ds[lon_coord].values

        # If coordinates are 1D, create 2D meshgrid
        if lats.ndim == 1 and lons.ndim == 1:
            logger.info(
                f"Creating 2D coordinate grid from 1D arrays: {len(lats)} lats x {len(lons)} lons"
            )
            lons_2d, lats_2d = np.meshgrid(lons, lats)
            lats = lats_2d
            lons = lons_2d
        elif lats.ndim == 2 and lons.ndim == 2:
            logger.info(f"Using 2D coordinate arrays: {lats.shape}")
        else:
            raise ValueError(
                f"Unexpected coordinate dimensions: lats {lats.shape}, lons {lons.shape}"
            )

        ds.close()
        return lats, lons

    except Exception as e:
        logger.error(f"Error extracting coordinates from GRIB file: {e}", exc_info=True)
        raise


def inspect_grib_file(grib_file_path):
    """
    Inspect a GRIB file to understand its structure.

    Args:
        grib_file_path: Path to the GRIB file
    """
    logger.info(f"Inspecting GRIB file: {grib_file_path}")

    try:
        ds = xr.open_dataset(grib_file_path, engine="cfgrib")

        print("\n=== GRIB File Structure ===")
        print("File: {grib_file_path}")
        print("\nDimensions:")
        for dim, size in ds.dims.items():
            print(f"  {dim}: {size}")

        print("\nCoordinates:")
        for coord in ds.coords:
            print(
                f"  {coord}: {ds[coord].shape} - {ds[coord].long_name if 'long_name' in ds[coord].attrs else 'No description'}"
            )

        print("\nData Variables:")
        for var in ds.data_vars:
            attrs = ds[var].attrs
            long_name = attrs.get("long_name", "No description")
            units = attrs.get("units", "No units")
            print(f"  {var}: {ds[var].shape} - {long_name} ({units})")

        print("\nGlobal Attributes:")
        for attr, value in ds.attrs.items():
            print(f"  {attr}: {value}")

        ds.close()

    except Exception as e:
        logger.error(f"Error inspecting GRIB file: {e}", exc_info=True)
        raise
