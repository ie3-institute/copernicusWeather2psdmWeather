import os
import socket
import subprocess
import time
import unittest

import yaml
from pypsdm.db.weather.models import WeatherValue
from sqlalchemy import create_engine, text
from sqlmodel import Session, select

from definitions import ROOT_DIR
from main import convert_cds_weather
from weather.database import get_engine


def setup_postgres(host="localhost", port=5433, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(1)
    raise RuntimeError("PostgreSQL did not become available in time.")


def ensure_database_exists(user, pw, port, db_name):
    from sqlalchemy.exc import OperationalError

    url = f"postgresql://{user}:{pw}@localhost:{port}/postgres"
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    retries = 30
    delay = 1
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
                )
                exists = result.scalar()
                if not exists:
                    conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            return
        except OperationalError:
            time.sleep(delay)
    raise RuntimeError(
        "PostgreSQL is up but not ready for SQL connections after waiting."
    )


class TestGribConversion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        subprocess.run(["docker-compose", "up", "-d", "testdb"], check=True)
        setup_postgres()

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["docker-compose", "down", "-v"], check=True)

    def setUp(self):
        config_path = os.path.join(
            ROOT_DIR, "tests", "integration", "grib_conversion_config.yaml"
        )
        with open(config_path) as f:
            config = yaml.safe_load(f)
        user = config["db_user"]
        pw = config["db_password"]
        port = config["db_port"]
        db_name = config["db_name"]
        ensure_database_exists(user, pw, port, db_name)
        convert_cds_weather(config_path)
        self.engine = get_engine(config_path)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def test_grib_conversion_creates_weather_values(self):
        config_path = os.path.join(
            ROOT_DIR, "tests", "integration", "grib_conversion_config.yaml"
        )
        engine = get_engine(config_path)
        session = Session(engine)
        results = session.exec(select(WeatherValue)).all()

        first = results[0]
        self.assertEqual(
            first.t2m, 291.27630615234375, "Unexpected t2m value in first WeatherValue"
        )


if __name__ == "__main__":
    unittest.main()
