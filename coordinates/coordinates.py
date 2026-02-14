import numpy as np
from geoalchemy2 import WKBElement
from netCDF4 import Dataset
from pypsdm.db.weather.models import Coordinate
from shapely.geometry import Point
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
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
