# %% **********************************
# Imports
# *************************************
import json
from pathlib import Path
import requests

# %% **********************************
# Constants
# *************************************
API_URL = "https://api.spitch.live"
DATA_PATH = "data/"
JSON_INDENT = 2

# %% **********************************
# Crawl contestants
# *************************************

# request contestants json
res = requests.get(API_URL + '/contestants')
contestants = res.json()

# write contestants json to file
with open(DATA_PATH + 'contestants.json', 'w') as f:
    json.dump(contestants, f, indent=JSON_INDENT, ensure_ascii=False)

# %% **********************************
# Crawl matchdays
# *************************************

# request matchdays json
res = requests.get(API_URL + '/matchdays')
matchdays = res.json()

# write matchdays json to file
with open(DATA_PATH + 'matchdays.json', 'w') as f:
    json.dump(matchdays, f, indent=JSON_INDENT, ensure_ascii=False)

# %% **********************************
# Crawl individual matchdays data
# *************************************

# create matchdays folder
p = Path(DATA_PATH + '/matchdays')
p.mkdir(exist_ok=True)

# make request for every matchday_id inside matchdays list
for index, matchday in enumerate(matchdays['matchdays']):
    if index < 3:
        matchday_id = matchday['id']
        res = requests.get(API_URL + f"/matchdays/{matchday_id}")
        matchday_json = res.json()
        with open(DATA_PATH + f"/matchdays/{matchday_id}.json", 'w') as f:
            json.dump(matchday_json, f, indent=JSON_INDENT, ensure_ascii=False)

# %% **********************************
# Crawl players
# *************************************

# create players folder
p = Path(DATA_PATH + '/players')
p.mkdir(exist_ok=True)

# make request for every player inside players list
for index, player in enumerate(contestants['players']):
    if index < 3:
        player_id = player['id']
        res = requests.get(API_URL + f"/players/{player_id}")
        player_json = res.json()
        with open(DATA_PATH + f"/players/{player_id}.json", 'w') as f:
            json.dump(player_json, f, indent=JSON_INDENT, ensure_ascii=False)

# %% **********************************
# Crawl events
# *************************************

# create matchdays folder
p = Path(DATA_PATH + '/matchdays')
p.mkdir(exist_ok=True)

# make request for every matchday_id inside matchdays list
for index, matchday in enumerate(matchdays['matchdays']):
    if index < 3: 
        matchday_id = matchday['id']

        # create folder data/matchdays/{matchday_id}
        p = Path(DATA_PATH + f"/matchdays/{matchday_id}")
        p.mkdir(exist_ok=True)

        # create players folder in data/matchdays/{matchday_id}, e.g. data/matchdays/{matchday_id}/players
        p = Path(DATA_PATH + f"/matchdays/{matchday_id}/players")
        p.mkdir(exist_ok=True)

        for index, player in enumerate(contestants['players']):
            if index < 3:
                player_id = player['id']
                res = requests.get(API_URL + f"/matchdays/{matchday_id}/players/{player_id}/events")
                event_json = res.json()
                with open(DATA_PATH + f"/matchdays/{matchday_id}/players/{player_id}.json", 'w') as f:
                  json.dump(event_json, f, indent=JSON_INDENT, ensure_ascii=False)