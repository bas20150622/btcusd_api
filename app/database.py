# Database connection script
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os

load_dotenv()
connection_info = {
    "drivername": os.getenv("POSTGRES_DRIVER"),
    "database": os.getenv("POSTGRES_DB"),
    "username": os.getenv("POSTGRES_USER"),
    "port": os.getenv("POSTGRES_PORT"),
    "host": os.getenv("POSTGRES_HOST"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

SQLALCHEMY_DATABASE_URL = URL.create(**connection_info)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL  # , connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
