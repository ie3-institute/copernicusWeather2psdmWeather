import numpy as np
import pandas as pd
from netCDF4 import Dataset
from sqlmodel import Session

from weather.models import Coordinate


def create_coordinates_df(weather: Dataset, session: Session):
    # Extract coordinates
    lats = np.asarray(weather.variables["latitude"])
    lons = np.asarray(weather.variables["longitude"])

    # Create coordinate meshgrids and their indices
    latlons = np.array(np.meshgrid(lats, lons)).T.reshape(-1, 2)
    latlons_idx = np.array(np.meshgrid(np.arange(len(lats)), np.arange(len(lons)))).T.reshape(-1, 2)

    # Create DataFrame with coordinates and indices
    latlons_df = pd.DataFrame({
        "coordinate": [Coordinate.from_xy(idx, lon, lat).coordinate for idx, (lat, lon) in zip(latlons_idx, latlons)],
        "idx": [tuple(idx) for idx in latlons_idx],
    })

    # Bulk create coordinates and add to session
    coordinates = [
        Coordinate(id=i, coordinate=row["coordinate"])
        for i, row in latlons_df.iterrows()
    ]

    session.add_all(coordinates)
    session.commit()

    return {tuple(idx): i for i, idx in enumerate(latlons_idx)}