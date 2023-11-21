import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import boto3
import os
import io

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def get_data(table_file):
    buffer = io.BytesIO()
    obj = s3.Object('csci5253-peter', table_file)
    obj.download_fileobj(buffer)
    data = pd.read_parquet(buffer)
    return data

def load_data(table_file, table_name, key):
    db_url = os.environ['DB_URL']
    print(db_url)
    conn = create_engine(db_url) 
    def insert_on_conflict_nothing(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
        result = conn.execute(stmt)
        return result.rowcount 
    get_data(table_file).to_sql(table_name, conn, if_exists='append', index=False, method=insert_on_conflict_nothing)
    print(f"Loaded {table_file} to {table_name} table")
    
def load_fact_table(table_file, table_name):
    db_url = os.environ['DB_URL']
    conn = create_engine(db_url) 
    get_data(table_file).to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Loaded {table_file} to {table_name} table")