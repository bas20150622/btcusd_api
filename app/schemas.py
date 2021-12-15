# Pydantic schemas
from typing import List, Optional
from decimal import Decimal
from pydantic import BaseModel


class OHLCVSchemaBase(BaseModel):
    timestamp: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume_BTC: Decimal


class OHLCVSchema(OHLCVSchemaBase):
    id: int

    class Config:
        orm_mode = True

