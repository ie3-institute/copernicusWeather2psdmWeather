from sqlalchemy.ext.declarative import declarative_base
from sqlmodel import Field, SQLModel

Base = declarative_base()


class Coordinates(SQLModel, table=True):
    """Represents a geographical coordinate."""

    id: int = Field(default=None, primary_key=True)
    coordinate: str
    coordinate_type: str

    def __eq__(self, other):
        if isinstance(other, Coordinates):
            return self.coordinate == other.coordinate
        return NotImplemented

    def __hash__(self):
        return hash((self.latitude, self.longitude))


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
