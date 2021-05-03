from telegram.ext import Updater, CommandHandler, CallbackContext
from os import getenv
from sqlalchemy import create_engine
import pandas as pd
import requests
import logging

db_name = getenv("POSTGRES_DB")
db_user = getenv("POSTGRES_USER")
db_pass = getenv("POSTGRES_PASSWORD")
db_host = 'database'
db_port = '5432'
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

updater = Updater(token=getenv("TELEGRAM_TOKEN"), use_context=True)
dispatcher = updater.dispatcher

def get_lineup(update, context):
    res = requests.get("http://localhost:85/request_lineup")
    context.bot.send_message(chat_id=update.effective_chat.id, text=res.json())

def tell_lineup(context: CallbackContext):
    df_users = pd.read_sql_table('subscribers', db)
    message = requests.get("http://localhost:85/request_lineup").json()
    
    # send message to all users
    for user_id in df_users['id']:
        context.bot.send_message(chat_id=user_id, text=message)

def add_user(update, context):
    df = pd.DataFrame(columns=['id', 'name'])
    df = df.append({'id': update.message.from_user.id, 'name': update.message.from_user.name}, ignore_index=True)
    df.to_sql('subscribers', con=db, if_exists='append')
    logging.info(f"""Added User: '{update.message.from_user.name}' to database with id: '{update.message.from_user.id}'""")
    context.bot.send_message(chat_id=update.effective_chat.id, text=f"""Welcome to the bot, {update.message.from_user.name}!""")

    res = requests.get("http://localhost:85/request_lineup")
    context.bot.send_message(chat_id=update.effective_chat.id, text=res.json())

def bot_get_ready():
    updater.start_polling()

def get_job_queue():
    return updater.job_queue

add_user_handler = CommandHandler('add_me', add_user)
dispatcher.add_handler(add_user_handler)

get_lineup_handler = CommandHandler('get_lineup', get_lineup)
dispatcher.add_handler(get_lineup_handler)