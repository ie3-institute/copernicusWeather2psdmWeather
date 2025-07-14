import os.path

import yaml
from sqlmodel import SQLModel, create_engine

from definitions import ROOT_DIR

with open(os.path.join(ROOT_DIR, "config.yaml"), "r") as yamlfile:
    config = yaml.safe_load(yamlfile)
user = config["db_user"]
pw = config["db_password"]
port = config["db_port"]
db_name = config["db_name"]

pg_url = f"postgresql://{user}:{pw}@localhost:{port}/{db_name}"
# echo prints all sql statements
engine = create_engine(pg_url, echo=True)


def create_database_and_tables():
    # creates database.db file and creates the table
    SQLModel.metadata.create_all(engine)
