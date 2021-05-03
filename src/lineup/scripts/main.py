import logging
from app.lineup import get_lineup
from fastapi import Request, FastAPI

logging.info("LineUp-Service up and running, waiting for requests...")

app = FastAPI()

@app.post("/get_data")
async def get_data(request: Request):
    logging.info("Getting request...")
    data = await request.json()
    get_lineup(data)
    return data