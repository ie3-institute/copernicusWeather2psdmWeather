import os
from weather.database import get_engine
import unittest
import subprocess
import time
import socket
from definitions import ROOT_DIR

from sqlmodel import Session, select
from pypsdm.db.weather.models import WeatherValue
from main import convert_cds_weather

def setup_postgres(host="localhost", port=5433, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    raise RuntimeError("PostgreSQL did not become available in time.")

class TestGribConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["docker-compose", "up", "-d", "testdb"], check=True)
        setup_postgres()
        time.sleep(2)  # Extra wait to ensure DB is ready

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["docker-compose", "down", "-v"], check=True)

    def setUp(self):
        path_grib_file = os.path.join(ROOT_DIR, "tests", "grib_conversion_config.yaml")
        convert_cds_weather(path_grib_file)
        self.engine = get_engine(path_grib_file)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def test_grib_conversion_creates_weather_values(self):
        config_path = os.path.join(ROOT_DIR, "tests", "grib_conversion_config.yaml")
        engine = get_engine(config_path)
        session = Session(engine)
        results = session.exec(select(WeatherValue)).all()

        first = results[0]
        self.assertEqual(first.t2m, 291.27630615234375, "Unexpected t2m value in first WeatherValue")



if __name__ == '__main__':
    unittest.main()
