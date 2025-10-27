from unittest.mock import Mock

import numpy as np
from pypsdm.db.weather.models import Coordinate

from coordinates.coordinates import create_coordinates_df


class TestCreateCoordinatesDF:

    def test_dataset_to_coordinate_conversion(self):
        """Test that Dataset lat/lon arrays are correctly converted to Coordinate objects"""
        weather = Mock()
        weather.variables = {
            "latitude": np.array([10.0, 20.0]),
            "longitude": np.array([100.0, 110.0]),
        }

        session = Mock()

        result = create_coordinates_df(weather, session)

        session.add_all.assert_called_once()
        coordinates = session.add_all.call_args[0][0]
        assert len(coordinates) == 4  # 2 lats × 2 lons = 4

        expected_ids = [0, 1, 2, 3]
        actual_ids = [coord.id for coord in coordinates]
        assert actual_ids == expected_ids

        assert all(isinstance(coord, Coordinate) for coord in coordinates)

        expected_mapping = {
            (0, 0): 0,  # lat_idx=0, lon_idx=0 -> coord_id=0
            (0, 1): 1,  # lat_idx=0, lon_idx=1 -> coord_id=1
            (1, 0): 2,  # lat_idx=1, lon_idx=0 -> coord_id=2
            (1, 1): 3,  # lat_idx=1, lon_idx=1 -> coord_id=3
        }
        assert result == expected_mapping

        session.commit.assert_called_once()

    def test_dataset_with_single_coordinate(self):
        """Test Dataset with single lat/lon pair"""
        weather = Mock()
        weather.variables = {
            "latitude": np.array([45.0]),
            "longitude": np.array([90.0]),
        }

        session = Mock()

        result = create_coordinates_df(weather, session)

        session.add_all.assert_called_once()
        coordinates = session.add_all.call_args[0][0]
        assert len(coordinates) == 1

        coord = coordinates[0]
        assert isinstance(coord, Coordinate)
        assert coord.id == 0

        assert result == {(0, 0): 0}

    def test_dataset_coordinate_ordering(self):
        """Test that coordinates are created in correct order (lat outer loop, lon inner loop)"""
        weather = Mock()
        weather.variables = {
            "latitude": np.array([1.0, 2.0, 3.0]),
            "longitude": np.array([10.0, 20.0]),
        }

        session = Mock()

        result = create_coordinates_df(weather, session)

        session.add_all.assert_called_once()
        coordinates = session.add_all.call_args[0][0]
        assert len(coordinates) == 6  # 3 lats × 2 lons = 6

        expected_ids = [0, 1, 2, 3, 4, 5]
        actual_ids = [coord.id for coord in coordinates]
        assert actual_ids == expected_ids

        expected_mapping = {
            (0, 0): 0,
            (0, 1): 1,  # lat_idx=0 (lat=1.0)
            (1, 0): 2,
            (1, 1): 3,  # lat_idx=1 (lat=2.0)
            (2, 0): 4,
            (2, 1): 5,  # lat_idx=2 (lat=3.0)
        }
        assert result == expected_mapping

        assert all(isinstance(coord, Coordinate) for coord in coordinates)

    def test_coordinate_geometry_creation(self):
        """Test that coordinates have proper geometry (WKB) created"""
        weather = Mock()
        weather.variables = {
            "latitude": np.array([45.0]),
            "longitude": np.array([90.0]),
        }

        session = Mock()

        create_coordinates_df(weather, session)

        coordinates = session.add_all.call_args[0][0]
        coord = coordinates[0]

        assert hasattr(coord, "coordinate")
        assert coord.coordinate is not None

        from geoalchemy2 import WKBElement

        assert isinstance(coord.coordinate, WKBElement)

        assert coord.coordinate.srid == 4326
