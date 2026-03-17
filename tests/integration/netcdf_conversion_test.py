import os

import pytest

from definitions import ROOT_DIR

from .weather_conversion_test_base import BaseWeatherConversionTest


class TestNetCDFConversion(BaseWeatherConversionTest):
    @pytest.fixture(autouse=True)
    def _inject_config(self, test_config):
        self.CONFIG_PATH = test_config

    def test_netcdf_conversion_creates_weather_values(self):
        csv_path = os.path.join(
            ROOT_DIR,
            "tests",
            "resources",
            "integration",
            "N51_5W6_5S51_0E9_0-20250601-20250604.csv",
        )
        self.run_weather_value_comparison(csv_path)
