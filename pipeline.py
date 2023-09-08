import pandas as pd
import numpy as np
import argparse

def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    transform_length = data.shape[0] - 12
    data_copy = data[['DATE']].iloc[:transform_length,:].copy().reset_index(drop=True)
    current_values = data['CPIAUCSL'][12:].values
    lag_values = data['CPIAUCSL'][:transform_length].values
    rate = (current_values - lag_values) / lag_values
    data_copy['CPIANN'] = rate
    return data_copy

def load_data(data, target):
    data.to_csv(target)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    data = extract_data(args.source)
    new_data = transform_data(data)
    load_data(new_data, args.target)
    print('Complete.')