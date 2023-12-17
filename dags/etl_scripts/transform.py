import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import argparse
import hashlib
import boto3
import re
import os
import io

aws_access_key_id = ''
aws_secret_access_key = ''
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
spark = SparkSession.builder.appName('transform').getOrCreate()
SCHEMA = StructType([
    StructField("animal_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("monthyear", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("outcome_subtype", StringType(), True),
    StructField("animal_type", StringType(), True),
    StructField("sex_upon_outcome", StringType(), True),
    StructField("age_upon_outcome", StringType(), True),
    StructField("breed", StringType(), True),
    StructField("color", StringType(), True)])

def hash_key(value):
    return hashlib.md5(value.encode('utf-8')).hexdigest()

hash_udf = F.udf(hash_key)

def sort_columns(data):
    data = data.withColumn('datetime', data['datetime'].cast('timestamp'))
    data = data.sort(data.datetime.asc())
    return data

def split_columns(data):
    data = data.withColumn('neutered', F.split(data['sex_upon_outcome'], ' ').getItem(0))
    data = data.withColumn('sex', F.split(data['sex_upon_outcome'], ' ').getItem(1))
    data = data.drop('sex_upon_outcome')
    data = data.withColumn('year', F.year(data['datetime']))
    data = data.withColumn('month', F.month(data['datetime']))
    data = data.withColumn('day', F.dayofmonth(data['datetime']))
    return data

def clean_columns(data):

    for col, type in data.dtypes:
        if type == 'string':
            data = data.withColumn(col, F.lower(data[col]))
   
    data = data.withColumn('name', F.regexp_replace(data['name'], r'\*', ''))
    data = data.withColumn('name', F.regexp_replace(data['name'], r'a\d{6}', 'unknown'))
    data = data.withColumn('name', F.regexp_replace('name', r'\d+ grams', 'unknown'))
    data = data.withColumn('name', F.regexp_replace('name', r'\d+g', ''))
    data = data.withColumn('name', F.regexp_replace('name', r'\s+', ' '))
    data = data.withColumn('name', F.regexp_replace('name', r'^ +| +$', ''))
    data = data.withColumn('name', F.regexp_replace('name', r'^\s*$', 'unknown'))
    data = data.na.fill(value='unknown', subset=['name', 'sex', 'outcome_subtype'])
    data = data.withColumn('neutered', F.regexp_replace('neutered', 'intact', 'no'))
    data = data.withColumn('neutered', F.regexp_replace('neutered', 'neutered', 'yes'))
    data = data.withColumn('neutered', F.regexp_replace('neutered', 'spayed', 'yes'))

    return data

def drop_columns(data):
    columns = ['monthyear', 'age_upon_outcome', 'sex_upon_outcome']
    data = data.drop(*columns)
    return data

def rename_columns(data):
    columns={'animal_id': 'animal_natural_key', 'name': 'animal_name', 'datetime': 'outcome_date',
             'date_of_birth': 'animal_dob', 'breed': 'animal_breed', 'color': 'animal_color',
             'neutered': 'outcome_type_neutered', 'sex': 'animal_sex', 'year': 'outcome_date_year',
             'month': 'outcome_date_month', 'day': 'outcome_date_day', 'outcome_subtype': 'outcome_type_subtype'}
    for k, v in columns.items():
        data = data.withColumnRenamed(k, v)
    return data

def get_data(source_csv):
    buffer = io.BytesIO()
    obj = s3.Object('csci5253-peter', source_csv)
    obj.download_fileobj(buffer)
    data = spark.createDataFrame(pd.read_parquet(buffer), schema=SCHEMA)
    return data

def prep_data(source_csv):
    data = get_data(source_csv)
    data = sort_columns(data)
    data = split_columns(data)
    data = clean_columns(data)
    data = drop_columns(data)
    data = rename_columns(data)
    return data

def transform_animal_dim(data):
    data = data.select(['animal_natural_key', 'animal_name', 'animal_dob', 'animal_type',
                      'animal_breed', 'animal_color', 'animal_sex'])
    data = data.dropDuplicates()
    data = data.withColumn('animal_id', hash_udf(F.concat_ws('', *data.columns)))
    surrogate_keys = data.select('animal_id')
    return data    

def transform_date_dim(data):
    data = data.select(['outcome_date', 'outcome_date_year', 'outcome_date_month', 'outcome_date_day'])
    data = data.dropDuplicates()
    data = data.withColumn('outcome_date_id', hash_udf(F.concat_ws('', *data.columns)))
    surrogate_keys = data.select('outcome_date_id')
    return data

def transform_type_dim(data):
    data = data.select(['outcome_type', 'outcome_type_subtype', 'outcome_type_neutered'])
    data = data.dropDuplicates()
    data = data.withColumn('outcome_type_id', hash_udf(F.concat_ws('', *data.columns)))
    surrogate_keys = data.select('outcome_type_id')
    return data

def transform_fact_table(data):
    data = data.withColumn('animal_id', 
                           hash_udf(F.concat_ws('', *data.select(['animal_natural_key', 'animal_name', 'animal_dob', 'animal_type',
                                                                  'animal_breed', 'animal_color', 'animal_sex']).columns)))
    data = data.withColumn('outcome_date_id', 
                           hash_udf(F.concat_ws('', *data.select(['outcome_date', 'outcome_date_year', 'outcome_date_month', 
                                                                  'outcome_date_day']).columns)))
    data = data.withColumn('outcome_type_id', 
                           hash_udf(F.concat_ws('', *data.select(['outcome_type', 'outcome_type_subtype', 
                                                                  'outcome_type_neutered']).columns)))
    data = data.select(['animal_natural_key', 'animal_id', 'outcome_date_id', 'outcome_type_id'])
    return data

def transform_data(source_csv, target_dir):
    data = prep_data(source_csv)
    animal_dim = transform_animal_dim(data)
    date_dim = transform_date_dim(data)
    type_dim = transform_type_dim(data)
    fact_table = transform_fact_table(data)
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_animal_dim.parquet', Body=animal_dim.toPandas().to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_date_dim.parquet', Body=date_dim.toPandas().to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_type_dim.parquet', Body=type_dim.toPandas().to_parquet(index=False))
    s3.Bucket('csci5253-peter').put_object(Key=target_dir + '/outcome_fact_table.parquet', Body=fact_table.toPandas().to_parquet(index=False))

parser = argparse.ArgumentParser()
parser.add_argument('--source_csv', '-s', type=str, help='Source CSV file')
parser.add_argument('--target_dir', '-t', type=str, help='Target directory')
args=parser.parse_args()

transform_data(args.source_csv, args.target_dir)