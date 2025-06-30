import numpy as np
import pandas as pd
from geoalchemy2 import WKBElement
from netCDF4 import Dataset
from pypsdm.db.weather.models import Coordinate
from shapely.geometry import Point
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session


def create_coordinates_df(weather: Dataset, session: Session):
    # Extract coordinates
    lats = np.asarray(weather.variables["latitude"])
    lons = np.asarray(weather.variables["longitude"])

    # Create coordinate meshgrids and their indices
    latlons = np.array(np.meshgrid(lats, lons)).T.reshape(-1, 2)
    latlons_idx = np.array(
        np.meshgrid(np.arange(len(lats)), np.arange(len(lons)))
    ).T.reshape(-1, 2)

    # Create DataFrame with coordinates and indices
    latlons_df = pd.DataFrame(
        {
            "coordinate": [
                Coordinate.from_xy(idx, lon, lat).coordinate
                for idx, (lat, lon) in zip(latlons_idx, latlons)
            ],
            "idx": [tuple(idx) for idx in latlons_idx],
        }
    )

    # Bulk create coordinates and add to session
    coordinates = [
        Coordinate(id=i, coordinate=row["coordinate"])
        for i, row in latlons_df.iterrows()
    ]

    session.add_all(coordinates)
    session.commit()

    return {tuple(idx): i for i, idx in enumerate(latlons_idx)}


def insert_coordinate(session, lat, lon):
    """
    Insert a coordinate into the database if it doesn't already exist.

    Args:
        session: Database session
        lat: Latitude of the coordinate
        lon: Longitude of the coordinate

    Returns:
        int: The ID of the inserted or existing coordinate.
    """
    point = Point(lon, lat)
    geography_point = WKBElement(point.wkb, srid=4326)

    try:
        existing_coordinate = (
            session.query(Coordinate)
            .filter(Coordinate.coordinate == geography_point)
            .first()
        )

        if existing_coordinate:
            return existing_coordinate.id

        # Find the highest current ID in the Coordinate table
        max_id = session.query(func.max(Coordinate.id)).scalar()
        new_id = (max_id or 0) + 1
        new_coordinate = Coordinate.from_xy(id=new_id, x=lat, y=lon)

        session.add(new_coordinate)
        session.commit()

        return new_coordinate.id

    except IntegrityError as e:
        session.rollback()

        existing_coordinate = (
            session.query(Coordinate)
            .filter(Coordinate.coordinate == geography_point)
            .first()
        )

        if existing_coordinate:
            return existing_coordinate.id

        raise e
