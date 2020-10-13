# %% **********************************
# Imports
# *************************************
import requests

# %% **********************************
# Constants
# *************************************
API_URL = "https://api.spitch.live/"

# %% **********************************
# Crawl contestants
# *************************************
res = requests.get(API_URL + '/contestants')
contestants = res.json()
