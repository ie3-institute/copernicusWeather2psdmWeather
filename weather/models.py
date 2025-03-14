import uuid

from sqlmodel import Field, SQLModel
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# FIXME: This can be changed to from lat lon to POINTS, then the SqlWeatherCoordinateSource can be used in SIMONA which is a bit faster
class Coordinate(SQLModel, table=True):
    """Represents a geographical coordinate."""
    id: int = Field(default=None, primary_key=True)
    latitude: float
    longitude: float
    coordinate_type: str

    def __eq__(self, other):
        if isinstance(other, Coordinate):
            return (self.latitude == other.latitude) and (self.longitude == other.longitude)
        return NotImplemented

    def __hash__(self):
        return hash((self.latitude, self.longitude))


class WeatherValue(SQLModel, table=True):
    """Represents weather data associated with a specific coordinate at a given time."""

    # time: datetime = Field(default=None, primary_key=True)
    # tmp_time = datetime(year=1900, month=1, day=1, tzinfo=pytz.UTC)
    # UWAGA TODO: change this to timestamp with time zone! else change it manually in created database! -> For later export from sql to csv everything should be fine. For direct use of Weather from sql this needs to be checked!
    time: str = Field(default=None, primary_key=True)
    coordinate_id: int = Field(
        default=None, primary_key=True, foreign_key="coordinate.id"
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