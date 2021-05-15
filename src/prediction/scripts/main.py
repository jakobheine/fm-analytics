import schedule
import time
from prediction import predict
import logging

logging.info("Prediction service up and running, waiting for scheduled event...")

# execute the prediction every Monday 9:59PM UTC, which is 11:59PM MESZ
schedule.every().monday.at("21:59").do(predict)

predict()

while True:
    schedule.run_pending()
    time.sleep(1)
