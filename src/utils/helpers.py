# src/utils/helpers.py
import yaml
import os
from datetime import datetime
import pandas as pd
import json


def load_config(filename):
    # Implementation to load yaml or json files from config folder
    # ...
    pass

def get_all_model_ids():
    # Logic to read all model IDs from model_config.yaml
    # ...
    pass

def convert_datetime_to_influxdb_string(dt):
    if not isinstance(dt, datetime):
        dt = pd.to_datetime(dt)
    influxdb_format = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return influxdb_format

# Function to load the configuration from a JSON file
def load_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    return config