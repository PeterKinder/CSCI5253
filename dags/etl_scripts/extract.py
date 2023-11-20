import pandas as pd
from pathlib import Path
from sodapy import Socrata
import datetime
import os

def extract(target_dir, target_file, start_date, execution_date):
    app_token = os.environ['APP_TOKEN']
    client = Socrata("data.austintexas.gov", app_token=app_token)
    if execution_date == start_date:
        results = client.get("9t4d-g238", where="DateTime <= '{}'".format(start_date), limit=1000000)
    else:
        prior_execution_date = (datetime.datetime.strptime(execution_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        results = client.get("9t4d-g238", where="DateTime > '{}' AND DateTime <= '{}'".format(prior_execution_date, execution_date), limit=10000)
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(results).to_csv(target_file, index=False)