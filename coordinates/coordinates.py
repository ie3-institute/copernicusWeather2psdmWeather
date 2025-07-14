import numpy as np
from netCDF4 import Dataset
from pypsdm.db.weather.models import Coordinate
from sqlmodel import Session


def create_coordinates_df(weather: Dataset, session: Session):
    lats = np.asarray(weather.variables["latitude"])
    lons = np.asarray(weather.variables["longitude"])

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
    session.commit()

    return idx_to_id