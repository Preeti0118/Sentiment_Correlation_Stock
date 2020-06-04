import pandas as pd
import pymysql
import csv
import os
from sqlalchemy import create_engine
import sqlalchemy
import json

path = os.path.join(os.path.dirname(__file__), 'sentiment_program_variables_ps.json')
variablefile = open(path)
df_var = json.load(variablefile)

# db_pass = os.environ.get('rich_m_pass')

#engine = create_engine('mysql+pymysql://rich_m:' + db_pass + '@localhost:3306/sa_db')

engine = df_var["var_engine_path"]

def sent_a():
    # df = pd.read_csv('/news_api_20200514/sentiment_score.csv', delimiter=',')
    # df.to_sql(name="sent_a", con=conn, if_exists='replace')
    path = os.path.join(os.path.dirname(__file__), 'sentiment.csv')
    with open(path, 'r') as file:
        df = pd.read_csv(file)
    df.to_sql(name='sentiment', con=engine, index=True, index_label='id', if_exists='replace')


def headlines():
    # conn = create_engine('mysql://rich:db_pass@localhost:3306/sa_db'
    path = os.path.join(os.path.dirname(__file__), 'headlines2.csv')
    with open(path, 'r') as file:
        df = pd.read_csv(file)
    # df = pd.read_csv('news_api_20200514/headlines2.csv', delimiter=',')
    df.to_sql(name="headlines", con=engine, if_exists='replace')

#
def news():
    # conn = create_engine('mysql://rich:db_pass@localhost:3306/sa_db')
    path = os.path.join(os.path.dirname(__file__), 'news.csv')
    with open(path, 'r') as file:
        df = pd.read_csv(file)
    df.to_sql("news", con=engine, if_exists='replace', index=False)

def twitter():
    # conn = create_engine('mysql://rich:db_pass@localhost:3306/sa_db')
    path = os.path.join(os.path.dirname(__file__), 'twitter_request_tsla.csv')
    with open(path, 'r') as file:
        df = pd.read_csv(file)
    df.to_sql("twitter", con=engine, if_exists='replace', index=False)

# def yfin():
    #conn = create_engine('mysql://rich:db_pass@localhost:3306/sa_db')
    # path = os.path.join(os.path.dirname(__file__), '.csv')
    # with open(path, 'r') as file:
    #     df = pd.read_csv(file)
    # df.to_sql("yfinance", con=engine, if_exists='replace', index=False)


if __name__ == '__main__':
    sent_a()
    headlines()
    news()
    twitter()
    # yfin()