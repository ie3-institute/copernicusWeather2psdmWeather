from sqlmodel import SQLModel
from weather.config import load_config
from sqlmodel import create_engine

def get_engine(config_path):
    config = load_config(config_path)
    user = config["db_user"]
    pw = config["db_password"]
    port = config["db_port"]
    db_name = config["db_name"]
    url = f"postgresql://{user}:{pw}@localhost:{port}/{db_name}"
    return create_engine(url)


def create_database_and_tables(config_path):
    # creates database.db file and creates the table
    engine = get_engine(config_path)
    SQLModel.metadata.create_all(engine)
