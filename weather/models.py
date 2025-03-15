from typing import Optional

from geoalchemy2 import Geography
from geoalchemy2.elements import WKBElement
from pydantic import ConfigDict
from sqlalchemy import Column
from sqlmodel import Field, SQLModel


class Coordinates(SQLModel, table=True):
    """Represents a geographical coordinate."""

    # Allow arbitrary types in model configuration
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: Optional[int] = Field(default=None, primary_key=True)

    # Use WKBElement type with the Geography column
    coordinate: WKBElement = Field(
        sa_column=Column(
            Geography(geometry_type="POINT", srid=4326, spatial_index=False)
        )
    )

    # Helper methods for working with coordinates
    def get_latitude(self, session) -> Optional[float]:
        """Get latitude value using PostGIS functions."""
        if self.coordinate is not None and session is not None:
            from sqlalchemy import func

            return session.scalar(func.ST_Y(func.ST_GeogFromWKB(self.coordinate)))
        return None

    def get_longitude(self, session) -> Optional[float]:
        """Get longitude value using PostGIS functions."""
        if self.coordinate is not None and session is not None:
            from sqlalchemy import func

            return session.scalar(func.ST_X(func.ST_GeogFromWKB(self.coordinate)))
        return None

    def __eq__(self, other):
        if isinstance(other, Coordinates):
            return self.coordinate == other.coordinate
        return NotImplemented

    def __hash__(self):
        return hash(str(self.coordinate))


class WeatherValue(SQLModel, table=True):
    """Represents weather data associated with a specific coordinate at a given time."""

    time: str = Field(default=None, primary_key=True)
    coordinate_id: int = Field(
        default=None, primary_key=True, foreign_key="coordinates.id"
    )

    # Diffuse irradiance in W/m²
    aswdifd_s: float
    # Direct irradiance in W/m²
    aswdir_s: float
    # Temperature in °C
    t2m: float
    # Wind velocity u component in m/s
    u131m: float
    # Wind velocity v component in m/s
    v131m: float
