# Pydantic schemas
from typing import List, Optional
from decimal import Decimal
from pydantic import BaseModel, validator
from datetime import datetime


class OHLCVSchema(BaseModel):
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_BTC: Decimal

    class Config:
        orm_mode = True

    @validator("timestamp")
    def timestamp_must_be_whole_minute(cls, v):
        if int(v.timestamp()) % 60 != 0:
            raise ValueError("timestamp must be whole minute")
        return v
