# Script to test schema validator
from app.schemas import OHLCVSchema
from decimal import Decimal
from datetime import datetime, timezone
import json


data = {
    "timestamp": "2022-12-15T17:58:00+00:00",
    "open": 47139.02,
    "high": 47168.55,
    "low": 47139.02,
    "close": 47168.55,
    "volume_BTC": 0.03307148,
}

print(OHLCVSchema(**data))


d1 = datetime(2022, 1, 1, 1, 1, tzinfo=timezone.utc)
d2 = datetime(2022, 1, 1, 1, 1, 30, tzinfo=timezone.utc)
d3 = int(datetime.now().timestamp())
d4 = int(datetime.utcnow().timestamp())
d5 = datetime.utcnow().replace(tzinfo=timezone.utc)

for count, d in enumerate([d1, d2, d3, d4, d5]):
    print(count + 1)
    newOHLCV = OHLCVSchema(
        timestamp=d,
        open=Decimal(1),
        close=Decimal(2),
        high=Decimal(3),
        low=Decimal(4),
        volume_BTC=Decimal(5),
    )
    print(newOHLCV)
    print(int(newOHLCV.timestamp.timestamp()) % 60)
    print()
