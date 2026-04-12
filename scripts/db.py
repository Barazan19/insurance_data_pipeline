import os
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@insurance_postgres:5432/insurance_db"
)

def get_engine():
    return create_engine(DB_URL)