# %% **********************************
# Imports
# *************************************
import json
from pathlib import Path
from re import match
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# %% **********************************
# Connect to database
# *************************************
db_name = 'fm_analytics'
db_user = 'top_manager'
db_pass = 'magical_password'
db_host = 'localhost'
db_port = '5432'

db_string = 'postgres://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

# %% **********************************
# Constants
# *************************************
API_URL = "https://api.spitch.live"
JSON_INDENT = 2

PROXIES = {
  "http": "127.0.0.1:8118",
  "https": "127.0.0.1:8118",
}

# %% **********************************
# Write players to database
# *************************************
res = requests.get(API_URL + '/contestants', proxies=PROXIES)
contestants = res.json()
players = contestants['players']
df = pd.json_normalize(players)
df.to_sql('players', con=db, if_exists='replace')

# %% **********************************
# Write matchdays to database
# *************************************
res = requests.get(API_URL + '/matchdays', proxies=PROXIES)
contestants = res.json()
matchdays = contestants['matchdays']
df = pd.json_normalize(matchdays)
df.to_sql('matchdays', con=db, if_exists='replace')

# %% **********************************
# Get passed matchday ids
# *************************************
current_timestamp = datetime.now(tz=None)
matchday_ids = []
for matchday in matchdays:
    if (datetime.strptime(matchday['end'], "%Y-%m-%dT%H:%M:%SZ") < current_timestamp):
      matchday_ids.append(matchday['id'])

print(len(matchday_ids))

# %% **********************************
# Get latest crawld matchday
# *************************************
SQL_QUERY = f"""
select max(sub.index) as latest_crawled_matchday from
    (select m.index from matchdays m
inner join events e on m.id = e.matchday_id
group by m.index) sub;
"""

df = pd.read_sql_query(SQL_QUERY, db)
df

# %% **********************************
# Get Player IDs
# *************************************
player_ids = []
for player in players:
    player_ids.append(player['id'])

print(player_ids)

# %% **********************************
# Write events to database
# *************************************
for matchday_id in matchday_ids:
    for player_id in player_ids:

        request_url = "{}/matchdays/{}/players/{}/events".format(API_URL, matchday_id, player_id)
        res = requests.get(request_url, proxies=PROXIES)
        res_json = res.json()
        events = res_json['events']

        for event in events:
            event['matchday_id'] = matchday_id
            event['player_id'] = player_id

        df = pd.json_normalize(events)
        df.to_sql('events', con=db, if_exists='append')
# %%
