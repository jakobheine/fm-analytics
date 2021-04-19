import schedule
import time
from spitch import crawl_spitch_data

# execute the crawler every Monday 9:59PM UTC, which is 11:59PM MESZ
# schedule.every().monday.at("21:59").do(crawl_spitch_data())

crawl_spitch_data()

while True:
    schedule.run_pending()
    time.sleep(1)
