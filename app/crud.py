# SQL CRUD queries

from sqlalchemy.orm import Session
from sqlalchemy import func

from . import models, schemas


def get_latest_timestamp(db: Session):
    return db.query(func.max(models.OHLCV.timestamp)).scalar()


def get_ohlcv(db: Session, timestamp: int):
    """Fetches the OHLC having OHLC.timestamp closest to timestamp,
    i.e. can be OHCL data before, on or after timestamp thus
    verification of timestamp date vs OHLC.date must be done
    to choose which OHLC price to use (close, open, or halfway in between high/low)"""
    _closest_timestamp = 60 * round(timestamp / 60)
    return (
        db.query(models.OHLCV)
        .filter(models.OHLCV.timestamp == _closest_timestamp)
        .first()
    )


def check_continuity(db: Session):
    first_ts = db.query(func.min(models.OHLCV.timestamp)).scalar()
    last_ts = db.query(func.max(models.OHLCV.timestamp)).scalar()
    count = db.query(models.OHLCV).count()
    res = {}
    # CHECK 1: num entries == (last_ts - first_ts) / 60 + 1
    res["CHECK 1, equal count vs length(last_ts - first_ts)"] = (
        "True" if int((last_ts - first_ts) / 60 + 1) == count else "False"
    )
    # CHECK 2: interval between ts ordered OHLCV entries == 60
    q = db.query(
        models.OHLCV.timestamp
        - func.lag(models.OHLCV.timestamp, 1).over(order_by=models.OHLCV.timestamp)
    )[1:]
    res["CHECK 2, equal time steps"] = "True" if len(set(q)) == 1 else "False"
    return res
