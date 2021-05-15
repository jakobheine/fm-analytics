import pandas as pd

import logging
from os import getenv
from sqlalchemy import create_engine
from datetime import datetime
import requests

logging.root.setLevel(logging.INFO)

db_name = getenv("POSTGRES_DB")
db_user = getenv("POSTGRES_USER")
db_pass = getenv("POSTGRES_PASSWORD")
db_host = 'database'
db_port = '5432'
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def get_passed_matchdays():
    logging.info("Getting passed matchdays...")
    current_timestamp = datetime.now(tz=None)
    df_matchdays = pd.read_sql_table('matchdays', db)
    passed_matchdays = []
    for index, matchday in df_matchdays.iterrows():
        if (datetime.strptime(matchday['end'], "%Y-%m-%dT%H:%M:%SZ") < current_timestamp):
            passed_matchdays.append(matchday['number'])
    return passed_matchdays

def get_predictions(df):
    df_players = df.groupby('id')

    passed_matchdays = get_passed_matchdays()

    # get the maximum from passed_matchdays, add 1 and cast it into an array
    upcoming_matchday = [max(passed_matchdays) + 1]

    df_predictions = pd.DataFrame(columns=['id','name','type','prediction'])

    logging.info("Calculating predictions...")
    
    for player_tuple in df_players:
        for player_type_tuple in player_tuple[1].groupby('type'):
            
            df_player_type = player_type_tuple[1]

            name = df_player_type['name'].max()

            for matchday in passed_matchdays:
               if df_player_type.loc[df_player_type['matchday'] == matchday].empty == True:
                df_player_type = df_player_type.append({'id': player_tuple[0], 'matchday': matchday, 'type': player_type_tuple[0], 'count': 0 }, ignore_index=True)
    
            # mean of past 5 values 
            pred_int = int(df_player_type.nlargest(5, 'matchday')['count'].mean())

            df_predictions = df_predictions.append({'id': player_tuple[0], 'type': player_type_tuple[0], 'prediction': pred_int}, ignore_index=True)
            logging.info(f"""Predicted {pred_int} occurences for event: '{player_type_tuple[0]}' for player: '{name}' """)
    
    return df_predictions

def get_data(db):
    logging.info("Getting data from database...")
    db_response = db.execute(f"""select p.id, (coalesce(p.first_name, '') || ' ' ||coalesce(p.last_name, '')) as name, m.number as matchday, type, count(*) from events
    inner join matchdays m on events.matchday_id = m.id
    inner join players p on events.player_id = p.id
    where corrected is false
    group by p.id, name, matchday, type
    order by p.id, name, matchday, type desc""")

    df_events = pd.DataFrame(db_response.fetchall())
    df_events.columns = db_response.keys()

    return df_events

def load_points_catalogue():
    logging.info("Loading points catalogue...")
    points_catalogue = [['pass', 2], ['unsuccessfulPass', -2], ['superPass', 12], ['throwIn', 2], ['goalAssist', 100], ['farCorner', 12], ['cross', 18], ['freeKick', 12], ['interception', 15], ['goalMissedFar', -20], ['shotAtGoal', 50], ['unsuccessfulPenalty', -60], ['missedChance', -40], ['goal', 200], ['doublePack', 50], ['hattrick', 100], ['penaltyGoal', 120], ['ownGoal', -60], ['foul', -15], ['awardedPenalty', 60], ['causedPenalty', -40], ['yellowCard', -30], ['secondYellowCard', -60], ['redCard', -80], ['successfulTackle', 15], ['unsuccessfulTackle', -10], ['blockedGoalShot', 20], ['offside', -15], ['error', -40], ['savedPenalty', 200], ['lostPenalty', -20], ['defended', 40], ['goalAgainst', -25]]

    return pd.DataFrame(points_catalogue, columns=['type','score'])

def calculate_score(df):
    logging.info("Calculating score...")
    df_points_catalogue = load_points_catalogue()
    df_scores = pd.merge(df, df_points_catalogue, how="outer")
    df_scores['sum_score'] = df_scores['prediction'] * df_scores['score']
    df_scores = df_scores.groupby('id')['sum_score'].sum().reset_index()
    return df_scores

def send_predicted_scores(df):
    host = 'http://lineup-service:85'
    logging.info(f"""Sending predicted scores to: {host}""")
    res = requests.post(host + '/get_data', data=df.to_json())
    logging.info(f"""Status code: {res.status_code}""")
    return res.status_code

def predict():
    df_events = get_data(db)
    df_predictions = get_predictions(df_events)
    df_scores = calculate_score(df_predictions)
    send_predicted_scores(df_scores)