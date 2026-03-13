import numpy as np
from netCDF4 import Dataset
from pypsdm.db.weather.models import Coordinate
from sqlmodel import Session



def extract_lat_lon(weather):
    """
    Returns (lats, lons) from either:
    - Dataset (netCDF4)
    - xarray (GRIB)
    """

    # Case netCDF4 Dataset
    if hasattr(weather, "variables"):
        lats = np.asarray(weather.variables["latitude"])
        lons = np.asarray(weather.variables["longitude"])
        return lats, lons

    # Case xarray (GRIB)
    elif hasattr(weather, "coords"):
        # Try common coordinate names
        for lat_name in ["latitude", "lat"]:
            if lat_name in weather.coords:
                lats = weather[lat_name].values
                break
        else:
            raise KeyError("Latitude coordinate not found")

        for lon_name in ["longitude", "lon"]:
            if lon_name in weather.coords:
                lons = weather[lon_name].values
                break
        else:
            raise KeyError("Longitude coordinate not found")

        return lats, lons

    else:
        raise TypeError("Unsupported weather data format")

def create_coordinates_df(weather: Dataset, session: Session):
    lats, lons = extract_lat_lon(weather)

    coordinates = []
    idx_to_id = {}

    coord_id = 0
    for lat_idx, lat in enumerate(lats):
        for lon_idx, lon in enumerate(lons):
            coord = Coordinate.from_xy(coord_id, float(lon), float(lat))
            coordinates.append(coord)

            idx_to_id[(lat_idx, lon_idx)] = coord_id
            coord_id += 1

    session.add_all(coordinates)

    return idx_to_id
