from sqlalchemy import create_engine
import pandas as pd
from os import getenv
import babel.numbers
import logging

db_name = getenv("POSTGRES_DB")
db_user = getenv("POSTGRES_USER")
db_pass = getenv("POSTGRES_PASSWORD")
db_host = 'database'
db_port = '5432'
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

logging.root.setLevel(logging.INFO)

def get_player_data(data):
    df = pd.DataFrame(data)

    GET_PLAYER_DATA = f"""
    select p.id, first_name, last_name, club_id, position, mv.market_value from players p
        inner join market_values mv on p.id = mv.player_id
    where injured = false and mv.valid_to is null;
    """
    db_response = db.execute(GET_PLAYER_DATA)
    df_players = pd.DataFrame(db_response.fetchall())
    df_players.columns = db_response.keys()

    df = pd.merge(df, df_players, how="inner", on="id").rename(columns={"sum_score": "score"})
    return df

def update_scores(df):
    df['score'] = df['score'].astype(float)

    # multiple the maximum score in the dataframe with 2 --> captain
    df.loc[df['score'].idxmax(), 'score'] *= 2

    # 100,000€ spend cost 0,8 score points --> database stores market_value in cent
    df['score_normalized'] = df['score'] - round(df['market_value']/100000000 * 0.8)

    return df

def calculate_lineup_scores(df):

    df_attacker = df.loc[df['position'] == 'attacker'].nlargest(4, 'score_normalized', keep='first')
    df_midfielder = df.loc[df['position'] == 'midfielder'].nlargest(5, 'score_normalized', keep='first')
    df_defender = df.loc[df['position'] == 'defender'].nlargest(5, 'score_normalized', keep='first')
    goalkeeper = df.loc[df['position'] == 'goalkeeper'].nlargest(1, 'score_normalized', keep='first').to_dict('records')[0]

    lineups = [[3,4,3], [3,5,2], [4,2,4], [4,3,3], [4,4,2], [4,5,1], [5,3,2], [5,4,1], [5,2,3], [3,3,4]]
    df_lineups = pd.DataFrame(lineups)
    df_lineups.columns = ['defenders', 'midfielders', 'attackers']

    for index, lineup in df_lineups.iterrows():
        df_lineups.at[index, 'defenders_players'] = ', '.join(df_defender.head(lineup['defenders'])['first_name'] + ' ' + df_defender.head(lineup['defenders'])['last_name'])
        df_lineups.at[index, 'midfielders_players'] = ', '.join(df_midfielder.head(lineup['midfielders'])['first_name'] + ' ' + df_midfielder.head(lineup['midfielders'])['last_name'])
        df_lineups.at[index, 'attackers_players'] = ', '.join(df_attacker.head(lineup['attackers'])['first_name'] + ' ' + df_attacker.head(lineup['attackers'])['last_name'])
        df_lineups.at[index, 'goalkeeper'] = goalkeeper['first_name'] + ' ' + goalkeeper['last_name']
        df_lineups.at[index, 'score_sum'] = df_defender.head(lineup['defenders'])['score_normalized'].sum() + df_midfielder.head(lineup['midfielders'])['score_normalized'].sum() + df_attacker.head(lineup['attackers'])['score_normalized'].sum() + goalkeeper['score_normalized']
        df_lineups.at[index, 'market_value_sum'] = df_defender.head(lineup['defenders'])['market_value'].sum() + df_midfielder.head(lineup['midfielders'])['market_value'].sum() + df_attacker.head(lineup['attackers'])['market_value'].sum() + goalkeeper['market_value']
    
    return df_lineups

def get_best_lineup(df_lineups):

    df_top3_lineups = df_lineups.nlargest(3, 'score_sum', keep='first')
    
    dict_best_lineup = df_top3_lineups.head(1).to_dict('records')[0]

    return dict_best_lineup

def print_lineup(dict_lineup, df):

    formation = 'Formation:\t\t{}-{}-{}\n\n'.format(dict_lineup['defenders'], dict_lineup['midfielders'], dict_lineup['attackers'])
    captain = 'Captain:\t\t{}\n\n'.format(df.loc[df['score'].idxmax()]['first_name'] + ' ' + df.loc[df['score'].idxmax()]['last_name'])
    lineup = 'Lineup:\n\nAttackers:\t\t{}\n\nMidfielders:\t\t{}\n\nDefenders:\t\t{}\n\nGoalkeeper:\t\t{}\n\n'.format(dict_lineup['attackers_players'], dict_lineup['midfielders_players'], dict_lineup['defenders_players'], dict_lineup['goalkeeper'])
    score = 'Calculated score:\t{}\n\n'.format(dict_lineup['score_sum'])
    babel.numbers.format_currency(dict_lineup['market_value_sum']/100, "EUR", locale='de')
    costs = 'Budget needed:\t\t{}'.format(babel.numbers.format_currency(dict_lineup['market_value_sum']/100, "EUR", locale='de'))

    output = '\n\nCalculated best lineup for upcoming matchday \n\n' + formation + captain + lineup + score + costs

    logging.info(output)

def get_lineup(data):
    df = get_player_data(data)
    df = update_scores(df)
    df_lineups = calculate_lineup_scores(df)
    dict_best_lineup = get_best_lineup(df_lineups)

    print_lineup(dict_best_lineup, df)