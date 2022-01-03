# SQLalchemy models
from sqlalchemy import Table, Column, Numeric, DateTime
from .database import Base


class OHLCV(Base):
    __tablename__ = "ohlcv"
    timestamp = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    open = Column(Numeric(10, 2))
    high = Column(Numeric(10, 2))
    low = Column(Numeric(10, 2))
    close = Column(Numeric(10, 2))
    volume_BTC = Column(Numeric(16, 8))
