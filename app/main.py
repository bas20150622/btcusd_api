# Main app script
from typing import List

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Union, List, Dict
from . import crud, models, schemas
from .database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/ping")
async def pong():
    return {"ping": "pong!"}


@app.get("/ohlcv/{timestamp}", response_model=schemas.OHLCVSchema)
def read_ohlcv(timestamp: Union[int, datetime], db: Session = Depends(get_db)):
    ohlcv = crud.get_ohlcv(db, timestamp=timestamp)
    return ohlcv


@app.get(
    "/multi",
    response_model=Dict[str, List[schemas.OHLCVSchema]],
)
def read_multiple_ohlcv(
    granularity: str, start: str, stop: str, db: Session = Depends(get_db)
):
    ohlcv = crud.query_ohlcv_by_granularity(
        session=db, granularity=granularity, start=start, stop=stop
    )
    return {"data": ohlcv}


@app.post("/ohlcv/", response_model=schemas.OHLCVSchema)
def add_ohlcv(ohlcv: schemas.OHLCVSchema, db: Session = Depends(get_db)):
    db_ohlcv = crud.create_ohlcv(db, ohlcv)
    return db_ohlcv


@app.get("/continuity")
def do_check(db: Session = Depends(get_db)):
    """verify the continuity of the db"""
    resp = crud.check_continuity(db)
    return resp


@app.get("/latest")
def check_latest(db: Session = Depends(get_db)):
    return {"latest timestamp in db": crud.get_latest_timestamp(db)}
