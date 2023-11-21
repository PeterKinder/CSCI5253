import pandas as pd
import numpy as np
import hashlib
import boto3
import re
import os
import io

aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def sort_columns(data):
    data_copy = data.copy()
    format = '%Y-%m-%dT%H:%M:%S.%f'
    data_copy['datetime'] = pd.to_datetime(data_copy['datetime'], format=format)
    data_copy = data_copy.sort_values(by=['datetime'], ascending=True).reset_index(drop=True)
    return data_copy

def split_columns(data):
    data_copy = data.copy()
    data_copy[['neutered', 'sex']] = data_copy['sex_upon_outcome'].str.split(expand=True)
    data_copy['year'] = data_copy['datetime'].dt.year
    data_copy['month'] = data_copy['datetime'].dt.month
    data_copy['day'] = data_copy['datetime'].dt.day
    return data_copy

def clean_columns(data):
    data_copy = data.copy()
    data_copy = data_copy.apply(lambda col: col.str.lower() if col.dtype == "object" else col)
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'\*', '', str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'a\d{6}', 'unknown', str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'\d+ grams', 'unknown', str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'\d+g', '', str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'\s+', ' ', str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r"^ +| +$", "", str(x)))
    data_copy['name'] = data_copy['name'].apply(lambda x: re.sub(r'^\s*$', "unknown", str(x)))
    data_copy['name'] = data_copy['name'].replace('nan', 'unknown')
    data_copy['sex'] = data_copy['sex'].replace(np.nan, 'unknown')
    data_copy['outcome_subtype'] = data_copy['outcome_subtype'].replace(np.nan, 'none')
    data_copy['neutered'] = data_copy['neutered'].replace('intact', 'no')
    data_copy['neutered'] = data_copy['neutered'].replace(['neutered', 'spayed'], 'yes')
    return data_copy

def drop_columns(data):
    columns = ['monthyear', 'age_upon_outcome', 'sex_upon_outcome']
    data_copy = data.drop(columns, axis=1, inplace=False)
    return data_copy

def rename_columns(data):
    data_copy = data.copy()
    data_copy.rename(columns={'animal_id': 'animal_natural_key', 'name': 'animal_name', 'datetime': 'outcome_date',
                              'date_of_birth': 'animal_dob', 'breed': 'animal_breed', 'color': 'animal_color',
                              'neutered': 'outcome_type_neutered', 'sex': 'animal_sex', 'year': 'outcome_date_year',
                              'month': 'outcome_date_month', 'day': 'outcome_date_day', 'outcome_subtype': 'outcome_type_subtype'}, inplace=True)
    return data_copy

def get_data(source_csv):
    buffer = io.BytesIO()
    obj = s3.Object('csci5253-peter', source_csv)
    obj.download_fileobj(buffer)
    data = pd.read_parquet(buffer)
    return data

def prep_data(source_csv):
    data = get_data(source_csv)
    data_copy = data.copy()
    data_copy = sort_columns(data_copy)
    data_copy = split_columns(data_copy)
    data_copy = clean_columns(data_copy)
    data_copy = drop_columns(data_copy)
    data_copy = rename_columns(data_copy)
    return data_copy

def transform_animal_dim(data):
    data_copy = data[['animal_natural_key', 'animal_name', 'animal_dob', 'animal_type',
                      'animal_breed', 'animal_color', 'animal_sex']].copy()
    surrogate_keys = [hashlib.md5(row.astype(str).str.cat(sep='').encode('utf-8')).hexdigest() for _, row in data_copy.iterrows()]
    data_copy['animal_id'] = surrogate_keys
    data_copy.drop_duplicates(inplace=True)
    data_copy.reset_index(drop=True, inplace=True)
    return surrogate_keys, data_copy    

def transform_date_dim(data):
    data_copy = data[['outcome_date', 'outcome_date_year', 'outcome_date_month', 'outcome_date_day']].copy()
    data_copy['outcome_date'] = data_copy['outcome_date'].dt.date
    surrogate_keys = [hashlib.md5(row.astype(str).str.cat(sep='').encode('utf-8')).hexdigest() for _, row in data_copy.iterrows()]
    data_copy['outcome_date_id'] = surrogate_keys
    data_copy.drop_duplicates(inplace=True)
    data_copy.reset_index(drop=True, inplace=True)
    return surrogate_keys, data_copy

def transform_type_dim(data):
    data_copy = data[['outcome_type', 'outcome_type_subtype', 'outcome_type_neutered']].copy()
    surrogate_keys = [hashlib.md5(row.astype(str).str.cat(sep='').encode('utf-8')).hexdigest() for _, row in data_copy.iterrows()]
    data_copy['outcome_type_id'] = surrogate_keys
    data_copy.drop_duplicates(inplace=True)
    data_copy.reset_index(drop=True, inplace=True)
    return surrogate_keys, data_copy

def transform_fact_table(data, animal_keys, date_keys, type_keys):
    data_copy = data[['animal_natural_key']].copy()
    data_copy['animal_id'] = animal_keys
    data_copy['outcome_date_id'] = date_keys
    data_copy['outcome_type_id'] = type_keys
    return data_copy

def transform_data(source_csv, target_dir):
    data = prep_data(source_csv)
    animal_keys, animal_dim = transform_animal_dim(data)
    date_keys, date_dim = transform_date_dim(data)
    type_keys, type_dim = transform_type_dim(data)
    fact_table = transform_fact_table(data, animal_keys, date_keys, type_keys)
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_animal_dim.parquet', Body=animal_dim.to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_date_dim.parquet', Body=date_dim.to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_type_dim.parquet', Body=type_dim.to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_fact_table.parquet', Body=fact_table.to_parquet(index=False))
    