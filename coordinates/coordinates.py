import numpy as np
import pandas as pd
from netCDF4 import Dataset
from sqlmodel import Session

from weather.models import Coordinates


def create_coordinates_df(weather: Dataset, session: Session):
    # Extract coordinates
    lats = np.asarray(weather.variables["latitude"])
    lons = np.asarray(weather.variables["longitude"])

    # Create meshgrids
    lats_idx = np.arange(len(lats))
    lons_idx = np.arange(len(lons))

    # Create coordinate meshgrids
    latlons = np.array(np.meshgrid(lats, lons)).T.reshape(-1, 2)
    latlons_idx = np.array(np.meshgrid(lats_idx, lons_idx)).T.reshape(-1, 2)

    # Create DataFrame
    latlons_df = pd.DataFrame(
        {
            "coordinate": [
                f"({lon},{lat})" for lat, lon in zip(latlons[:, 0], latlons[:, 1])
            ],
            "idx": [tuple(idx) for idx in latlons_idx],
        }
    )

    # Bulk create coordinates
    coordinates = []
    for i, row in latlons_df.iterrows():
        coordinate = Coordinates(
            id=i,
            coordinate=row["coordinate"],
            coordinate_type="ICON",
        )
        coordinates.append(coordinate)

    session.add_all(coordinates)

    return {tuple(idx): i for i, idx in enumerate(latlons_idx)}
