import csv
from datetime import timezone

import pytest
import yaml
from dateutil import parser
from pypsdm.db.weather.models import WeatherValue
from sqlmodel import Session, select

from main import convert_cds_weather
from weather.database import get_engine


class BaseWeatherConversionTest:
    CONFIG_PATH = None  # Will be set by test

    @pytest.fixture(scope="session")
    def postgres_container(self):
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgis/postgis:15-3.3") as postgres:
            yield postgres

    @pytest.fixture(scope="session")
    def test_config(self, conf_path, tmp_path_factory, postgres_container):
        import os

        import yaml
        from sqlalchemy.engine.url import make_url

        with open(conf_path) as f:
            config = yaml.safe_load(f)
        url = postgres_container.get_connection_url()
        u = make_url(url)
        config["db_user"] = u.username
        config["db_password"] = u.password
        config["db_host"] = u.host
        config["db_port"] = u.port
        config["db_name"] = u.database
        temp_config_path = tmp_path_factory.mktemp("data") / "patched_config.yaml"
        with open(temp_config_path, "w") as f:
            yaml.safe_dump(config, f)
        yield str(temp_config_path)

    @pytest.fixture(autouse=True)
    def _setup_db(self):
        with open(self.CONFIG_PATH) as f:
            config = yaml.safe_load(f)
        convert_cds_weather(self.CONFIG_PATH)
        self.engine = get_engine(self.CONFIG_PATH)
        self.session = Session(self.engine)
        try:
            yield
        finally:
            self.session.close()

    def run_weather_value_comparison(self, csv_path: str):
        results = self.session.exec(select(WeatherValue)).all()
        db_rows = {(str(r.time), int(r.coordinate_id)): r for r in results}
        with open(csv_path, newline="") as csvfile:
            reader = list(csv.DictReader(csvfile))
            assert len(db_rows) == len(reader), "DB has extra rows not in CSV"
            for row in reader:
                csv_time = parser.parse(row["time"]).replace(tzinfo=timezone.utc)
                key = (str(csv_time), int(row["coordinate_id"]))
                assert key in db_rows, f"Missing row in DB for {key}"
                db_row = db_rows[key]
                assert (
                    abs(float(row["t2m"]) - db_row.t2m) < 1e-8
                ), f"Mismatch for t2m at {key}"
                assert (
                    abs(float(row["aswdifd_s"]) - db_row.aswdifd_s) < 1e-8
                ), f"Mismatch for aswdifd_s at {key}"
                assert (
                    abs(float(row["aswdir_s"]) - db_row.aswdir_s) < 1e-8
                ), f"Mismatch for aswdir_s at {key}"
                assert (
                    abs(float(row["u131m"]) - db_row.u131m) < 1e-8
                ), f"Mismatch for u131m at {key}"
                assert (
                    abs(float(row["v131m"]) - db_row.v131m) < 1e-8
                ), f"Mismatch for v131m at {key}"
