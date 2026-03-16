import csv
import os
import platform
import socket
import subprocess
import time
import unittest

import pytz
import yaml
from dateutil import parser
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
        if platform.system() == "Darwin":
            raise unittest.SkipTest("Skipping Docker-related tests on macOS runner")
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
        results = self.session.exec(select(WeatherValue)).all()
        db_rows = {(str(r.time), int(r.coordinate_id)): r for r in results}

        csv_path = os.path.join(
            ROOT_DIR,
            "tests",
            "resources",
            "integration",
            "N51_5W6_5S51_0E9_0-20250601-20250604.csv",
        )
        with open(csv_path, newline="") as csvfile:
            reader = list(csv.DictReader(csvfile))
            # Check DB has no extra rows
            self.assertEqual(len(db_rows), len(reader), "DB has extra rows not in CSV")
            # Compare rows of DB and csv
            for row in reader:
                csv_time = parser.parse(row["time"]).replace(tzinfo=pytz.UTC)
                key = (str(csv_time), int(row["coordinate_id"]))
                self.assertIn(key, db_rows, f"Missing row in DB for {key}")
                db_row = db_rows[key]
                self.assertAlmostEqual(
                    float(row["t2m"]),
                    db_row.t2m,
                    places=8,
                    msg=f"Mismatch for t2m at {key}",
                )
                self.assertAlmostEqual(
                    float(row["aswdifd_s"]),
                    db_row.aswdifd_s,
                    places=8,
                    msg=f"Mismatch for aswdifd_s at {key}",
                )
                self.assertAlmostEqual(
                    float(row["aswdir_s"]),
                    db_row.aswdir_s,
                    places=8,
                    msg=f"Mismatch for aswdir_s at {key}",
                )
                self.assertAlmostEqual(
                    float(row["u131m"]),
                    db_row.u131m,
                    places=8,
                    msg=f"Mismatch for u131m at {key}",
                )
                self.assertAlmostEqual(
                    float(row["v131m"]),
                    db_row.v131m,
                    places=8,
                    msg=f"Mismatch for v131m at {key}",
                )


if __name__ == "__main__":
    unittest.main()
