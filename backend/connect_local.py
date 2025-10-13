import sqlite3
import pyodbc
from sqlalchemy import create_engine
import pandas as pd

# Dictionary mapping table names to a list of column names

class connect_local:
    def __init__(self, db_path = "my_local_database.db"):
        self.df_path = db_path
    
    def connect_to_db(self, engine=False):
        cnxn = sqlite3.connect(self.df_path)
        cursor = cnxn.cursor()
        if engine:
            engine_obj = create_engine(f'sqlite:///{ self.df_path}')
            return engine_obj, cursor, cnxn
        else:
            return cursor, cnxn

def select_all_from_table(cursor, cnxn, table_name):
    select_query = f"SELECT * FROM {table_name}"
    # print(select_query)
    cursor.execute(select_query)
    print(cursor)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)
    return df

def select_columns_from_table(cursor, table_name, columns):
    columns_string = ",".join(columns)
    select_query = f"SELECT {columns_string} FROM {table_name}"
    print(select_query)
    cursor.execute(select_query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)
    # close_connection(cursor, cnxn)
    return df

def clear_table(cursor, cnxn, table_name):
    drop_query = f"DELETE FROM {table_name};"
    cursor.executescript(drop_query)
    cnxn.commit()
    # close_connection(cursor, cnxn)

def clear_table_by_year(cursor, cnxn, table_name, year):
    drop_query = f"DELETE FROM {table_name} WHERE fiscal_year = {year};"
    drop_query2 = f"DELETE FROM {table_name} WHERE cap_year = {year};"
    try:
        cursor.executescript(drop_query)
    except:
        cursor.executescript(drop_query2)
    cnxn.commit()
    # close_connection(cursor, cnxn)


def initialize_database(cursor,cnxn, initial_values=False):
    drop_all_tables(cursor, cnxn)
    create_tables(cursor, cnxn)
    alter_tables(cursor, cnxn)
    if initial_values:
        insert_testing_data(cursor, cnxn)
    

def create_tables(cursor, cnxn):
    # query = "sql\\create_tables_local.sql"
    query = "sql/create_tables_local.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.executescript(query)
    cnxn.commit()

def alter_tables(cursor, cnxn):
    # query = "sql\\alter_tables_local.sql"
    query = "sql/alter_tables_local.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.executescript(query)
    cnxn.commit()

def insert_testing_data(cursor, cnxn):
    query = "sql\\insert_into_tables_local.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.executescript(query)
    cnxn.commit()

def drop_all_tables(cursor, cnxn):
    # drop_query = "sql\\drop_tables.sql"
    drop_query = "sql/drop_tables.sql"
    with open(drop_query, 'r') as f:
        drop_query = f.read()
    cursor.executescript(drop_query)
    cnxn.commit()

def close_connection(cursor, cnxn):
    cursor.close()
    cnxn.close()

