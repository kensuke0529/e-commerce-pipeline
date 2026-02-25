import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL
from dotenv import load_dotenv

load_dotenv()

snowflake_url = URL.create(
    drivername="snowflake",
    username=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    host=os.getenv("SNOWFLAKE_ACCOUNT"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    query={
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
)

engine: Engine = create_engine(snowflake_url)