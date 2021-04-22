# %% **********************************
# Imports
# *************************************
import psycopg2
from sqlalchemy import create_engine
import plotly
import plotly.express as px
import pandas as pd

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
# Query database
# *************************************

SQL_QUERY = f"""
select * from events
inner join players p on p.id = events.player_id
"""

df = pd.read_sql_query(SQL_QUERY, db)
df
# %%
