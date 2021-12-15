# SQLalchemy models
from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, Column, Integer, Numeric
from .database import Base


class OHLCV(Base):
    __tablename__ = "ohlcv"
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer, index=True)  # UTC
    open = Column(Numeric(10, 2))
    high = Column(Numeric(10, 2))
    low = Column(Numeric(10, 2))
    close = Column(Numeric(10, 2))
    volume_BTC = Column(Numeric(16, 8))