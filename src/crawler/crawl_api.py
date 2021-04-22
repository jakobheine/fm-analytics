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
# Functions
# *************************************
def get_market_value_id(player):

    SQL_QUERY = f"""
        select id, market_value from market_values
            where player_id='{player['id']}'
            and valid_to is null;
        """
    response=db.execute(SQL_QUERY).fetchone()

    if not response:
        print(f"""No market value found in database for player id: {player['id']}""")

        new_db_id = db.execute(f"""insert into market_values (player_id, market_value, valid_from)
                        values ('{player['id']}', {player['market_value']}, now())
                        returning id;
                    """).fetchone()[0]

        print(f"""Added market value {player['market_value']} with id {new_db_id} for player id: {player['id']}""")

        return new_db_id

    else:
        [db_id, db_market_value] = response

        if db_market_value != player['market_value']:
            print(f"""Existing market value in database: {db_market_value} does not match new market value from request: {player['market_value']} for player id: {player['id']}""")

            db.execute(f"""UPDATE market_values SET valid_to = now() WHERE id = {db_id};""")

            new_db_id = db.execute(f"""insert into market_values (player_id, market_value, valid_from)
                            values ('{player['id']}', {player['market_value']}, now())
                            returning id;
                        """).fetchone()[0]

            print(f"""Updated market value for player id: {player['id']} to: {player['market_value']}. New market value id is: {new_db_id}""")

            return new_db_id

        elif db_market_value == player['market_value']:
            return db_id

# %% **********************************
# Write matchdays to database
# *************************************
res = requests.get(API_URL + '/matchdays', proxies=PROXIES)
contestants = res.json()
matchdays = contestants['matchdays']
df = pd.json_normalize(matchdays)
df.to_sql('matchdays', con=db, if_exists='replace')

# %% **********************************
# Write players to database
# *************************************
res = requests.get(API_URL + '/contestants', proxies=PROXIES)
contestants = res.json()
players = contestants['players']
df = pd.json_normalize(players)

for index, player in df.iterrows():
    df.at[index, 'market_value'] = get_market_value_id(player)

df['market_value']
    


#df.to_sql('players', con=db, if_exists='append', index_label="id")



# %% **********************************
# Prepare writing events to database
# *************************************

# get passed matchday ids
current_timestamp = datetime.now(tz=None)
matchday_ids = []
for matchday in matchdays:
    if (datetime.strptime(matchday['end'], "%Y-%m-%dT%H:%M:%SZ") < current_timestamp):
      matchday_ids.append(matchday['id'])

print(matchday_ids)

SQL_QUERY = f"""
select max(sub.index) as latest_crawled_matchday from
    (select m.index from matchdays m
inner join events e on m.id = e.matchday_id
group by m.index) sub;
"""

df = pd.read_sql_query(SQL_QUERY, db)

# get player ids
player_ids = []
for player in players:
    player_ids.append(player['id'])

print(player_ids)

# %% **********************************
# Write events to database
# *************************************

# get events and write them to database
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

