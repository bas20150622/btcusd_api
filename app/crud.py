# SQL CRUD queries

from sqlalchemy.orm import Session
from sqlalchemy.sql import func, extract, distinct
from datetime import timedelta, datetime
from app.models import OHLCV
from typing import Union
from dateparser import parse
from sqlalchemy.orm import Session, Query
from sqlalchemy.sql import literal_column, func, desc
import re
from datetime import timedelta

from . import models, schemas


def create_ohlcv(db: Session, ohlcv: schemas.OHLCVSchema):
    db_ohlcv = models.OHLCV(**ohlcv.dict())
    db.add(db_ohlcv)
    db.commit()
    db.refresh(db_ohlcv)
    return db_ohlcv


def get_latest_timestamp(db: Session) -> datetime:
    return db.query(func.max(models.OHLCV.timestamp)).scalar()


def get_ohlcv(db: Session, timestamp: Union[int, datetime]) -> OHLCV:
    """Fetches the OHLC having OHLC.timestamp closest to timestamp,
    i.e. can be OHCL data before, on or after timestamp thus
    verification of timestamp date vs OHLC.date must be done
    to choose which OHLC price to use (close, open, or halfway in between high/low)"""
    if isinstance(timestamp, datetime):
        _closest_timestamp = 60 * round(timestamp.timestamp() / 60)
    else:
        _closest_timestamp = 60 * round(timestamp / 60)
    return (
        db.query(models.OHLCV)
        .filter(models.OHLCV.timestamp == datetime.utcfromtimestamp(_closest_timestamp))
        .first()
    )


def get_before_or_on_ohlcv(db: Session, timestamp: Union[int, datetime]) -> OHLCV:
    """Fetches the OHLC having OHLC.timestamp closest to timestamp,
    i.e. can be OHCL data before, on or after timestamp thus
    verification of timestamp date vs OHLC.date must be done
    to choose which OHLC price to use (close, open, or halfway in between high/low)"""
    if isinstance(timestamp, datetime):
        _closest_timestamp = 60 * int(timestamp.timestamp() / 60)
    else:
        _closest_timestamp = 60 * int(timestamp / 60)
    return (
        db.query(models.OHLCV)
        .filter(models.OHLCV.timestamp == datetime.utcfromtimestamp(_closest_timestamp))
        .first()
    )


regex = re.compile(
    r"((?P<days>\d+?) ?(d|day))?((?P<hours>\d+?) ?(hr|h|hour))?((?P<minutes>\d+?) ?(m|min))?((?P<seconds>\d+?) ?(s|sec))?"
)


def _parse_granularity(time_str: str) -> timedelta:
    """
    Parses a time string representing a timedelta in days, hours, minutes, seconds for the formats (h or hr or hour), (m or min), (s or seconds)
    """
    print(time_str)
    total_timedelta = timedelta(seconds=0)
    for parts in regex.finditer(time_str):
        # parts = regex.match(time_str)
        if not parts:
            continue
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.items():
            if param:
                time_params[name] = int(param)
        total_timedelta += timedelta(**time_params)
    return total_timedelta


"""
SELECT 
  time_bucket('1 day', timestamp) as time, 
  MAX(high) as high,
  first(open, timestamp) as open,
  last(close, timestamp) as close,
  MIN(low) as low
FROM ohlcv
GROUP by time 
ORDER by time desc
"""


def query_ohlcv_by_granularity(
    session: Session, granularity: str, start: str, stop: str
) -> Query:
    """return ohlcv data within timeframe start to stop with granularity"""
    gran: timedelta = _parse_granularity(granularity)
    start: datetime = parse(start)
    stop: datetime = parse(stop)

    q = (
        session.query(
            func.time_bucket(gran, OHLCV.timestamp).label("timestamp"),
            func.MAX(OHLCV.high).label("high"),
            func.first(OHLCV.open, OHLCV.timestamp).label("open"),
            func.last(OHLCV.close, OHLCV.timestamp).label("close"),
            func.MIN(OHLCV.low).label("low"),
            func.SUM(OHLCV.volume_BTC).label("volume_BTC"),
        )
        .filter(
            func.time_bucket(gran, OHLCV.timestamp) >= start,
            func.time_bucket(gran, OHLCV.timestamp) < stop,
        )
        .group_by(func.time_bucket(gran, OHLCV.timestamp))
        .order_by(desc(func.time_bucket(gran, OHLCV.timestamp)))
        .all()
    )
    return q


def check_continuity(db: Session) -> dict[str:str]:
    first_ts = db.query(
        extract("epoch", func.min(OHLCV.timestamp))
    ).scalar()  # select extract(epoch from min(timestamp)) from public.ohlcv
    last_ts = db.query(
        extract("epoch", func.max(OHLCV.timestamp))
    ).scalar()  # select extract(epoch from max(timestamp)) from public.ohlcv
    res = {}

    count = db.query(OHLCV).count()
    # CHECK 1: num entries == (last_ts - first_ts) / 60 + 1
    res["CHECK 1, equal count vs length(last_ts - first_ts)"] = (
        "True" if int((last_ts - first_ts) / 60 + 1) == count else "False"
    )

    # CHECK 2: interval between ts ordered OHLCV entries == 60
    q = db.query(
        distinct(
            (
                OHLCV.timestamp
                - func.lag(OHLCV.timestamp, 1).over(order_by=OHLCV.timestamp)
            ).label("_")
        ).label("diff")
    )
    # SELECT distinct timestamp - LAG(timestamp) OVER (ORDER BY timestamp) AS difference
    # FROM public.ohlcv
    print("CHECK 2: assert distinct diff==timedelta(seconds==60)")
    for res in q:
        if res.diff:
            assert res.diff == timedelta(seconds=60)

    res["CHECK 2, equal time steps"] = "True"  # assigned if above assert loop passes
    return res
