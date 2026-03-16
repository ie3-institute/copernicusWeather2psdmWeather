import os
import platform
import subprocess
import unittest

from definitions import ROOT_DIR

from .weather_conversion_test_base import BaseWeatherConversionTest, setup_postgres


class TestGribConversion(BaseWeatherConversionTest):
    CONFIG_PATH = os.path.join(
        ROOT_DIR, "tests", "integration", "grib_conversion_config.yaml"
    )

    @classmethod
    def setUpClass(cls):
        if platform.system() == "Darwin":
            raise unittest.SkipTest("Skipping Docker-related tests on macOS runner")
        subprocess.run(["docker-compose", "up", "-d", "testdb"], check=True)
        setup_postgres()

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["docker-compose", "down", "-v"], check=True)

    def test_grib_conversion_creates_weather_values(self):
        csv_path = os.path.join(
            ROOT_DIR,
            "tests",
            "resources",
            "integration",
            "N51_5W6_5S51_0E9_0-20250601-20250604.csv",
        )
        self.run_weather_value_comparison(csv_path)


if __name__ == "__main__":
    unittest.main()
