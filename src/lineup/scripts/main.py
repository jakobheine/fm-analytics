import logging
import schedule
import time
from lineup import get_lineup

logging.info("LineUp-Service up and running, waiting for scheduled event.")

# print the lineup every Friday 1:00PM UTC, which is 3:00PM MESZ, 30 minutes before kick-off of matchday
schedule.every().friday.at("13:00").do(get_lineup)

while True:
    schedule.run_pending()
    time.sleep(1)