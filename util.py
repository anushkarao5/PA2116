import sys
import json
import csv
import yaml

import pandas as pd
import numpy as np

import matplotlib as mpl

import time
from datetime import datetime
# see https://stackoverflow.com/questions/415511/how-do-i-get-the-current-time-in-python
#   for some basics about datetime

import pprint

# sqlalchemy 2.0 documentation: https://www.sqlalchemy.org/
import psycopg2
from sqlalchemy import create_engine, text as sql_text

db_eng = create_engine('postgresql+psycopg2://postgres:artHunder67#@localhost:5432/airbnb',
                       connect_args={'options': '-csearch_path={}'.format('new_york_city')},
                       isolation_level = 'SERIALIZABLE')


def hello_world():
    print('hello world')
    
    
# build_query_listings_join_reviews
def build_query_listings_join_reviews(start_date, end_date):
    query = f"""
    SELECT DISTINCT l.id, l.name
    FROM listings l, reviews r 
    WHERE l.id = r.listing_id
      AND r.date >= '{start_date}'
      AND r.date <= '{end_date}'
    ORDER BY l.id;
    """
    return query

def build_query_listings_join_reviews_datetime(start_datetime, end_datetime):
    query = f"""
    SELECT DISTINCT l.id, l.name
    FROM listings l, reviews r 
    WHERE l.id = r.listing_id
      AND r.datetime >= '{start_datetime}'
      AND r.datetime <= '{end_datetime}'
    ORDER BY l.id;
    """
    return query

# time_diff function
def time_diff(time1, time2):
    return (time2-time1).total_seconds()


# running_query
# def run_query_multiple_times(q,count=10): 
#     time_list=[]
    
#     for i in range(0,count):
#         time_start=datetime.now()
#         with db_eng.connect() as conn: 
#             df=pd.read_sql(q,con=conn)
#         time_end=datetime.now()
#         diff = time_diff(time_start, time_end)
#         time_list.append(diff)
        
#     print(round(sum(time_list)/len(time_list), 4), \
#             round(min(time_list), 4), \
#             round(max(time_list), 4), \
#             round(np.std(time_list), 4))   
#     return(time_list)

# running_query_time with dbeng
def run_query_multiple_times(db_eng,q,count=10): 
    time_list=[]
    
    for i in range(0,count):
        time_start=datetime.now()
        with db_eng.connect() as conn: 
            df=pd.read_sql(q,con=conn)
        time_end=datetime.now()
        diff = time_diff(time_start, time_end)
        time_list.append(diff)
        
    print(round(sum(time_list)/len(time_list), 4), \
            round(min(time_list), 4), \
            round(max(time_list), 4), \
            round(np.std(time_list), 4))   
    return(time_list)



# adding dropping index 
def add_drop_index(db_eng,action,column,table):
    
    index_name = f"{column}_in_{table}"

    if action=="add": 
        query = f"""
        BEGIN TRANSACTION;
        CREATE INDEX IF NOT EXISTS {index_name}
        ON {table}({column});
        END TRANSACTION;
        """
    elif action=="drop":
        query = f"""
        BEGIN TRANSACTION;
        DROP INDEX IF EXISTS {index_name};
        END TRANSACTION;
        """
    else: 
        raise ValueError("Action must be 'add' or 'drop'")
    with db_eng.connect() as conn: 
        conn.execute(sql_text(query))
    
    show_indexes_query = f"""
    SELECT *
    FROM pg_indexes
    WHERE tablename = '{table}';
    """
    with db_eng.connect() as conn:
        result = conn.execute(sql_text(show_indexes_query))
        indexes=result.fetchall()
    return indexes


# building index description key 

def build_index_description_key(all_indexes,spec):
    index_func=""
    for index in all_indexes:
        if index in spec: 
            spec_temp = f"__{index[0]}_in_{index[1]}"
            index_func+=spec_temp
    index_func+="__"
    return index_func