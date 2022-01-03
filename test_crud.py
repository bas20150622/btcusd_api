# Test CRUD
from app.database import SessionLocal
from app.models import OHLCV
from app.crud import (
    get_ohlcv,
    get_latest_timestamp,
    query_ohlcv_by_granularity,
    _parse_granularity,
)
from datetime import datetime, timezone, timedelta
from app.schemas import OHLCVSchema
from dateparser import parse

db = SessionLocal()
res = get_latest_timestamp(db)
print(res)

res = get_ohlcv(db, timestamp=datetime(2021, 12, 21, 1, 1, tzinfo=timezone.utc))
print(res.__dict__)


q = query_ohlcv_by_granularity(
    session=db,
    granularity="1d",
    start="2021-12-15 00:01:00+00:00",
    stop="2021-12-22 00:01:00+00:00",
)

# print(q)
for row in q:
    print(row)
db.close()

data = {
    "timestamp": "2022-12-15T17:58:00+01:00",
    "open": 47139.02,
    "high": 47168.55,
    "low": 47139.02,
    "close": 47168.55,
    "volume_BTC": 0.03307148,
}
newOHLC = OHLCV(**data)
print(newOHLC.__dict__)
