import logging
from sqlalchemy import create_engine
import pandas as pd
import requests
from datetime import datetime
from os import getenv

# constants
API_URL = "https://api.spitch.live"
JSON_INDENT = 2

PROXIES = {
  "http": "proxy:8118",
  "https": "proxy:8118",
}

logging.root.setLevel(logging.INFO)

db_name = getenv("POSTGRES_DB")
db_user = getenv("POSTGRES_USER")
db_pass = getenv("POSTGRES_PASSWORD")
db_host = 'database'
db_port = '5432'
db_string = 'postgres://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def get_market_value_id(player):
    """
    This method takes a row from the dataframe of the latest player data and returns for the market value its current ID. 
    Since the market value of a player changes often, the market value attribute is implemented as SCD2. 
    If the market value differs from the latest market value in the database, the corresponding database entry is updated
    and the resulting new ID is returned. 
    parameter:
        player: 
            Series 
            One player from the dataframe containing the latest player data.
        db:
            instance of sqlAlchemy :class:_engine.Engine
            The database engine where the player data gets looked up.

    return value:
        market_value_id: 
            Integer 
            The id in the market_value table for the given player.
            This is either the old ID from the market_value table if the market value has not changed, 
            or the new ID if the market value has changed.
    """

    GET_MARKET_VALUE = f"""
        select id, market_value from market_values
            where player_id='{player['id']}'
            and valid_to is null;
        """

    player_name = player['first_name'] + " " + player['last_name']

    db_entry = db.execute(GET_MARKET_VALUE).fetchone()

    if not db_entry:
        logging.info(f"""No market value found in database for player: {player_name}""")

        new_db_id = db.execute(f"""insert into market_values (player_id, market_value, valid_from)
                        values ('{player['id']}', {player['market_value']}, now())
                        returning id;
                    """).fetchone()[0]

        logging.info(f"""Added market value {player['market_value']} with id {new_db_id} for player: {player_name}""")

        return new_db_id

    else:
        [db_id, db_market_value] = db_entry

        if db_market_value != player['market_value']:
            logging.info(f"""Existing market value in database: {db_market_value} does not match new market value from request: {player['market_value']} for player: {player_name}""")

            db.execute(f"""UPDATE market_values SET valid_to = now() WHERE id = {db_id};""")

            new_db_id = db.execute(f"""insert into market_values (player_id, market_value, valid_from)
                            values ('{player['id']}', {player['market_value']}, now())
                            returning id;
                        """).fetchone()[0]

            logging.info(f"""Updated market value for player: {player_name} to: {player['market_value']}. New market value id is: {new_db_id}""")

            return new_db_id

        elif db_market_value == player['market_value']:
            return db_id

def get_passed_matchdays():
    logging.info('Getting passed matchday ids...')
    current_timestamp = datetime.now(tz=None)
    df_matchdays = pd.read_sql_table('matchdays', db)
    passed_matchdays = []
    for index, matchday in df_matchdays.iterrows():
        if (datetime.strptime(matchday['end'], "%Y-%m-%dT%H:%M:%SZ") < current_timestamp):
            passed_matchdays.append(matchday['number'])
    return passed_matchdays

def get_latest_crawled_matchday():
    logging.info('Getting matchday id from latest crawled event...')
    latest_crawled_matchday = db.execute(f"""select max(m.number) from events e inner join matchdays m on m.id = e.matchday_id;""").fetchone()[0]
    if latest_crawled_matchday is None:
        latest_crawled_matchday = 0
    return latest_crawled_matchday

def crawl_players():
    logging.info('Crawling latest players data...')
    res = requests.get(API_URL + '/contestants', proxies=PROXIES)
    contestants = res.json()
    latest_players_data = contestants['players']
    df = pd.json_normalize(latest_players_data)

    logging.info('Get market value id from database...')
    for index, player in df.iterrows():
        df.at[index, 'market_value'] = get_market_value_id(player)

    logging.info('Replace players database...')
    df.to_sql('players', con=db, if_exists='replace')

def crawl_matchdays():
    logging.info('Crawling matchdays...')
    res = requests.get(API_URL + '/matchdays', proxies=PROXIES)
    matchdays = res.json()['matchdays']
    df = pd.json_normalize(matchdays)
    df.to_sql('matchdays', con=db, if_exists='replace')

def crawl_events():
    passed_matchdays = get_passed_matchdays()
    latest_crawled_matchday = get_latest_crawled_matchday()

    matchdays_to_crawl = sorted(matchday for matchday in passed_matchdays if matchday > latest_crawled_matchday)
    
    logging.info("Getting id's for matchdays to crawl...")
    matchdays_to_crawl = db.execute(f"""select id, number from matchdays where number in {str(tuple(matchdays_to_crawl))}""")

    logging.info("Getting player id's...")
    df_players = pd.read_sql_table('players', db)

    logging.info(f'Crawling events...')

    for matchday in matchdays_to_crawl:
        for index, player in df_players.iterrows():
            logging.info(f"""Crawling events for player: "{player['first_name']} {player['last_name']}" for matchday {matchday['number']}...""")
            request_url = "{}/matchdays/{}/players/{}/events".format(API_URL, matchday['id'], player['id'])
            logging.info("URL: " + request_url)
            res = requests.get(request_url, proxies=PROXIES)
            res_json = res.json()
            events = res_json['events']
            for event in events:
                event['matchday_id'] = matchday['id']
                event['player_id'] = player['id']
            df = pd.json_normalize(events)
            df.to_sql('events', con=db, if_exists='append')

def crawl_spitch_data():
    crawl_matchdays()
    crawl_players()
    crawl_events()