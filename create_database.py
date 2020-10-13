# %% **********************************
# Imports
# *************************************
import os
import json
import sqlite3

# %% **********************************
# Constants
# *************************************
DATA_PATH = "data/"
DATABASE_NAME = "database.sqlite"
JSON_INDENT = 2

# %% **********************************
# Setup sqlite connection
# *************************************

# remove old sqlite database if it exists
try:
    os.remove(DATABASE_NAME)
except OSError as e:
    print(f'Warning: {DATABASE_NAME} : {e.strerror}')

# create new sqlite database and connect to it
connection = sqlite3.connect(DATABASE_NAME)

# %% **********************************
# Create players table
# *************************************
sql_query = open("sql/create_players_table.sql", "r").read()
connection.execute(sql_query)

# %% **********************************
# Load players from json
# *************************************
with open(DATA_PATH + 'contestants.json', 'r') as f:
    contestants = json.load(f)

# empty players list
players = []

# push player data into players list
for index, player in enumerate(contestants['players']):
    if index < 3:
        player = {
            'id': player['id'],
            'first_name': player['first_name'],
            'last_name': player['last_name'],
        }
        players.append(player)

# transform list of dicts to list of tuples of values
players = [tuple(player.values()) for player in players]

# %% **********************************
# Insert players into sqlite players table
# *************************************

connection.executemany(
    "insert into players (id, first_name, last_name) values (?, ?, ?)",
    players
)

for row in connection.execute("select * from players"):
    print(row)

# %% **********************************
# Commit changes and close sqlite connection
# *************************************
connection.commit()
connection.close()
