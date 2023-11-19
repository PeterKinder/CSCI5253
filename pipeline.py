from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import argparse
import re

"""
def extract_data(source):
    return pd.read_csv(source)
"""

def sort_columns(data):
    data_copy = data.copy()
    format = "%m/%d/%Y %H:%M:%S %p"
    data_copy['DateTime'] = pd.to_datetime(data_copy['DateTime'], format=format)
    data_copy = data_copy.sort_values(by=['DateTime'], ascending=False).reset_index(drop=True)
    return data_copy

def split_columns(data):
    data_copy = data.copy()
    data_copy[['Neutered', 'Sex']] = data_copy['Sex upon Outcome'].str.split(expand=True)
    data_copy['Year'] = data_copy['DateTime'].dt.year
    data_copy['Month'] = data_copy['DateTime'].dt.month
    data_copy['Day'] = data_copy['DateTime'].dt.day
    data_copy['Hour'] = data_copy['DateTime'].dt.hour
    data_copy['Minute'] = data_copy['DateTime'].dt.minute
    return data_copy

def clean_columns(data):
    data_copy = data.copy()
    data_copy['Name'] = data_copy['Name'].apply(lambda x: re.sub(r'\*', '', str(x)))
    data_copy['Name'] = data_copy['Name'].apply(lambda x: re.sub(r'A\d{6}', 'Unknown', str(x)))
    data_copy['Name'] = data_copy['Name'].apply(lambda x: re.sub(r'\d+ Grams', 'Unknown', str(x)))
    data_copy['Name'] = data_copy['Name'].replace('nan', 'Unknown')
    data_copy['Sex'] = data_copy['Sex'].replace(np.nan, 'Unknown')
    data_copy['Outcome Subtype'] = data_copy['Outcome Subtype'].replace(np.nan, 'None')
    data_copy['Neutered'] = data_copy['Neutered'].replace('Intact', 'No')
    data_copy['Neutered'] = data_copy['Neutered'].replace(['Neutered', 'Spayed'], 'Yes')
    return data_copy

def drop_columns(data):
    columns = ['MonthYear', 'Age upon Outcome', 'Sex upon Outcome']
    data_copy = data.drop(columns, axis=1, inplace=False)
    return data_copy

def rename_columns(data):
    data_copy = data.copy()
    data_copy.columns = ['animal_natural_key', 'animal_name', 'outcome_date', 'animal_dob', 'outcome_type', 
                         'outcome_type_subtype', 'animal_type', 'animal_breed', 'animal_color', 
                         'outcome_type_neutered', 'animal_sex', 'outcome_date_year', 'outcome_date_month', 
                         'outcome_date_day', 'outcome_date_hour', 'outcome_date_minute']
    return data_copy

def transform_data(source_csv):
    data_copy = pd.read_csv(source_csv)
    data_copy = sort_columns(data_copy)
    data_copy = split_columns(data_copy)
    data_copy = clean_columns(data_copy)
    data_copy = drop_columns(data_copy)
    data_copy = rename_columns(data_copy)
    return data_copy

def load_outcome_type_dim(data, conn):
    data_copy = data[['outcome_type', 'outcome_type_subtype', 'outcome_type_neutered']].copy()
    data_copy = data_copy.drop_duplicates()
    data_copy = data_copy.reset_index(drop=True).reset_index()
    data_copy = data_copy.rename(columns={'index': 'outcome_type_id'})
    data_copy.to_sql("outcome_type_dim", conn, if_exists='append', index=False)
    return data_copy, list(data_copy.columns.values)

def load_animal_dim(data, conn):
    data_copy = data[['animal_natural_key', 'animal_name', 'animal_dob', 'animal_type',
                      'animal_breed', 'animal_color', 'animal_sex']].copy()
    data_copy = data_copy.drop_duplicates()
    data_copy = data_copy.reset_index(drop=True).reset_index()
    data_copy = data_copy.rename(columns={'index': 'animal_id'})
    data_copy.to_sql("outcome_animal_dim", conn, if_exists='append', index=False)
    return data_copy, list(data_copy.columns.values)


def load_outcome_date_dim(data, conn):
    data_copy = data[['outcome_date', 'outcome_date_year', 'outcome_date_month', 
                      'outcome_date_day', 'outcome_date_hour', 'outcome_date_minute']].copy()
    data_copy = data_copy.drop_duplicates()
    data_copy = data_copy.reset_index(drop=True).reset_index()
    data_copy = data_copy.rename(columns={'index': 'outcome_date_id'})
    data_copy.to_sql("outcome_date_dim", conn, if_exists='append', index=False)
    return data_copy, list(data_copy.columns.values)

def load_fact_table(data, dims, dims_columns, conn):
    data_copy = data.copy()
    dim_count = len(dims_columns)
    keep_columns = ['animal_natural_key']
    for i in range(dim_count):
        data_copy = pd.merge(data_copy, dims[i], how='left', on=dims_columns[i][1:])
        keep_columns.append(dims_columns[i][0])
    data_copy = data_copy[keep_columns]
    data_copy = data_copy.reset_index(drop=True).reset_index()
    data_copy = data_copy.rename(columns={'index': 'id'})
    data_copy.to_sql("outcome_fact", conn, if_exists='append', index=False)
    return data_copy

def load_data(data):
    db_url = 'postgresql+psycopg2://peter:password@db:5432/shelter'
    conn = create_engine(db_url)

    dims = []
    dims_columns = []

    dim, dim_columns = load_outcome_type_dim(data, conn)
    dims.append(dim)
    dims_columns.append(dim_columns)

    dim, dim_columns = load_animal_dim(data, conn)
    dims.append(dim)
    dims_columns.append(dim_columns)

    dim, dim_columns = load_outcome_date_dim(data, conn)
    dims.append(dim)
    dims_columns.append(dim_columns)
    

    load_fact_table(data, dims, dims_columns, conn)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    args = parser.parse_args()

    print("Starting...")
    data = extract_data(args.source)
    print("Transforming...")
    data = transform_data(data)
    print("Loading...")
    load_data(data)
    print('Complete.')