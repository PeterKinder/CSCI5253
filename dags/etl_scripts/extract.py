import pandas as pd
from sodapy import Socrata
import datetime
import boto3
import os

def extract(target_file, start_date, execution_date):
    app_token = os.environ['APP_TOKEN']
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    client = Socrata("data.austintexas.gov", app_token=app_token)
    if execution_date == start_date:
        results = client.get("9t4d-g238", where="DateTime <= '{}'".format(start_date), limit=1000000)
    else:
        prior_execution_date = (datetime.datetime.strptime(execution_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        results = client.get("9t4d-g238", where="DateTime > '{}' AND DateTime <= '{}'".format(prior_execution_date, execution_date), limit=10000)
    #Notice on testing that when there is very limited records for a date, sometimes columns are missing
    #This would lead to errors down the road, so we need to make sure all columns are present
    df = pd.DataFrame(columns = ['animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth', 
                             'outcome_type', 'outcome_subtype', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
                             'breed', 'color'])
    df = pd.concat([df, pd.DataFrame(results)])
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    s3.Bucket('csci5253-peter').put_object(Key=target_file, Body=df.to_parquet(index=False))