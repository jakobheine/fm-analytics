import logging
from app.lineup import get_lineup, get_output
from app.bot import bot_get_ready
from fastapi import Request, FastAPI

logging.info("LineUp-Service up and running, waiting for requests...")

app = FastAPI()

bot_get_ready()

@app.post("/get_data")
async def get_data(request: Request):
    logging.info("Getting request...")
    data = await request.json()
    get_lineup(data)
    return data

@app.get("/request_lineup")
def request_lineup():
    return get_output()
