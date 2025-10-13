import pandas as pd
import pyodbc
import os
from sqlalchemy import create_engine
# Load the CSV file into a pandas DataFrame

def connect_to_sql(engine=False):
    connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};' \
        'SERVER=CN11WPLI-776\SQLSERVER;' \
        'DATABASE=finance_management;' \
        'UID=admin-111;' \
        'PWD=passwordA!'
    )
    cnxn = pyodbc.connect(
        connection_string
    )
    cursor = cnxn.cursor()
    if engine:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")
        return engine, cursor, cnxn
    else:
        return cursor, cnxn

def select_all_from_table(cursor, cnxn, table_name):
    select_query = f"SELECT * FROM {table_name}"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)
    # close_connection(cursor, cnxn)
    return df

def select_columns_from_table(cursor, table_name, columns):
    columns_string = " ".join(columns)
    select_query = f"SELECT {columns_string} FROM {table_name}"
    cursor.execute(select_query)
    rows = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df = pd.DataFrame.from_records(rows, columns=columns)
    # close_connection(cursor, cnxn)
    return df

def clear_table(cursor, cnxn, table_name):
        
    drop_query = f"TRUNCATE TABLE {table_name};"
    cursor.execute(drop_query)
    cnxn.commit()
    # close_connection(cursor, cnxn)

def clear_table(cursor, cnxn, table_name, year):
    drop_query = f"TRUNCATE TABLE {table_name} WHERE fiscal_year = {year};"
    drop_query2 = f"TRUNCATE TABLE {table_name} WHERE cap_year  = {year};"
    cursor.execute(drop_query)
    cnxn.commit()
    # close_connection(cursor, cnxn)

def initialize_database(cursor,cnxn, initial_values=True):
    drop_all_tables(cursor, cnxn)
    create_tables(cursor, cnxn)
    alter_tables(cursor, cnxn)
    if initial_values:
        insert_testing_data(cursor, cnxn)
    

def create_tables(cursor, cnxn):
    query = "sql\\create_tables.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.execute(query)
    cnxn.commit()

def alter_tables(cursor, cnxn):
    query = "sql\\alter_tables.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.execute(query)
    cnxn.commit()

def insert_testing_data(cursor, cnxn):
    query = "sql\\insert_into_tables.sql"
    with open(query, 'r') as f:
        query = f.read()
    cursor.execute(query)
    cnxn.commit()

def drop_all_tables(cursor, cnxn):
    drop_query = "sql\\drop_tables.sql"
    with open(drop_query, 'r') as f:
        drop_query = f.read()
    cursor.execute(drop_query)
    cnxn.commit()

def close_connection(cursor, cnxn):
    cursor.close()
    cnxn.close()
    