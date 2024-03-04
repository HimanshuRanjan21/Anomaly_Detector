################################
# Vijeet Nigam
# InfluxDB Functions and Classes
################################

# Importing Necessary Libraries
import numpy as np
import pandas as pd
import os, time
import configparser
import influxdb_client
import requests
import json
from io import StringIO

from datetime import datetime
from itertools import product
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client import BucketsApi, Bucket, BucketRetentionRules
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
import logging

logger = logging.getLogger(__name__)

allowed_influxDB_pairs = {
    'measurement': ['tag_data', 'KPIs', 'lab_data'],
    'field': ['raw_value', 'cleaned_value', 'value', 'KPI_value'],
    'tag': ['sensor_id', 'KPI_name', 'type_of_property']
}

class APIHandler:
    """
    APIHandler class to interact with InfluxDB.

    This class provides methods for the following operations:
    - Get the organization's ID associated with this InfluxDB client instance
    - Create a new bucket in InfluxDB
    - Write time series data into a specific InfluxDB bucket
    - Remove time series data from a specific InfluxDB bucket within a time range
    - Fetch time series data from a specific InfluxDB bucket within a time range

    Attributes:
        url (str): The URL of the InfluxDB server.
        token (str): The authentication token for accessing InfluxDB.
        org (str): The organization associated with InfluxDB data.
        client (InfluxDBClient): An InfluxDB client.
        buckets_api (BucketsApi): An InfluxDB BucketsApi object.
    """ 

    def __init__(self, url: str, token: str, org: str):
        """
        Initialize the APIHandler instance.

        Args:
            url (str): The URL of the InfluxDB server.
            token (str): The authentication token for accessing InfluxDB.
            org (str): The organization associated with InfluxDB data.

        Returns:
            None
        """
        
        # Assign arguments to instance variables
        self.url = url
        self.token = token
        self.org = org

    def get_org_id(self) -> str or None:
        """
        Retrieve the ID of the organization associated with this InfluxDB client instance.

        Returns:
            str: The ID of the organization if found, else None.
        """

        # Create the URL for the InfluxDB API endpoint to list organizations
        list_orgs_url = f"{self.url}/api/v2/orgs"

        # Define the request headers, including the authorization token
        headers = {
            'Authorization': f'Token {self.token}',
        }

        try:
            # Send the HTTP GET request to list organizations
            response = requests.get(list_orgs_url, headers=headers)

            if response.status_code == 200:
                orgs = response.json()['orgs']
                for org in orgs:
                    if 'name' in org and 'id' in org and org['name'] == self.org:
                        return org['id']


            # Return None if no matching organization was found
            return None
        except Exception as e:
            print(f"Error while retrieving organization ID: {e}")
            return None
    
    def create_bucket(self,
                    bucket_name: str,
                    retention_period: int = 0
                    ) -> bool:
        """
        Creates a new bucket in InfluxDB.

        Args:
            bucket_name (str): The name of the bucket to be created.
            retention_period (int, optional): The data retention period in seconds. 
                                            A value of 0 indicates infinite retention.
                                            Default is 0.

        Returns:
            bool: True if the bucket was created successfully, False otherwise.
        """

        # Create the URL for the InfluxDB API endpoint to create a bucket
        create_bucket_url = f"{self.url}/api/v2/buckets"

        # Define the request headers, including the authorization token
        headers = {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'application/json',  # Specify JSON content
        }

        # Get the organization ID
        org_id = self.get_org_id()
        if org_id is None:
            print(f"No organization found with the name '{self.org}'")
            return False

        # Define the request payload to create the bucket
        data = {
            "name": bucket_name,
            "orgID": org_id,
            "retentionRules": [{"type": "expire", "everySeconds": retention_period}]
        }

        try:
            # Send the HTTP POST request to create the bucket
            response = requests.post(create_bucket_url, headers=headers, json=data)

            if response.status_code == 201:
                print(f"Bucket '{bucket_name}' created successfully.")
                return True
            else:
                print(f"Failed to create the bucket. Status code: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error while creating the bucket: {e}")
            return False

    def write_time_series_data(self,
                            df: pd.DataFrame, 
                            bucket: str, 
                            measurement: str,
                            tag_name: list,
                            field_name:list
                            ) -> None:
        """
        Writes time series data to an InfluxDB bucket.

        Args:
            df (pd.DataFrame): The data to be written to InfluxDB. It is assumed that the first column of 
                            the DataFrame is named 'Timestamp' and contains timestamps in the format 
                            '%m-%d-%Y %H:%M:%S'.
            bucket (str): The name of the bucket in InfluxDB to which data will be written.
            measurement (str): The name of the measurement or table within the bucket.
            tag_name (list): A list of tags to be assigned to the data.

        Returns:
            None
        """
        logger.debug(f"Writing series data :: length of data : {len(df)}")
        start=time.time()
        # df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # df = df.dropna()
        logger.debug(f"Writing series data :: length of data after dropping nan values : {len(df)}")

        # Standardize the timestamp column name to 'Timestamp'
        df.rename(columns={df.columns[0]: 'Timestamp'}, inplace=True)

        # Convert 'Timestamp' to datetime format
        df.loc[:, 'Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m-%d-%Y %H:%M:%S')

        # Handle multiple entries with the same timestamp by adding a time component based on 
        # the cumulative count per date in 'Timestamp'
        df_grouped = df.groupby(df['Timestamp']).cumcount()
        df.loc[:, 'Timestamp'] = df['Timestamp'] + pd.to_timedelta(df_grouped, unit='s')
        
#         lines=[]
#         for index, row in df.iterrows():
#             tags = ",".join([f"{key}={value}" for key, value in zip(tag_name, row[tag_name])])
#             fields = " ".join([f"{key}={value}" for key, value in row.items() if key == '_value'])
#             line = f"{measurement},{tags} {fields} {int(row['Timestamp'].timestamp())}"
#             lines.append(line)
        start_i=time.time()
        df.loc[:, 'Timestamp'] = df['Timestamp'].astype('int64') // 10**9
        
        def format_field(key, value):
            if isinstance(value, str):
                return f'{key}="{value}"'  # Enclose strings in quotes
            else:
                return f"{key}={value}"

        # Use DataFrame's vectorized operations to concatenate the tag names and values
        tag_columns = df[tag_name].astype(str)
        df.loc[:, 'tags']= tag_columns.apply(lambda x: ','.join(f"{key}={x[key]}" for key in tag_name), axis=1)

        # Similarly, concatenate the field names and values
        field_columns = df[field_name]
        df.loc[:, 'fields'] = field_columns.apply(lambda x: ','.join(format_field(field, x[field]) for field in field_name if (x[field] not in ["nan", np.nan, None])&(x[field]==x[field])), axis=1)

        # Combine everything into the line protocol format
        df.loc[:, 'line']= measurement + "," + df['tags'] + " " + df['fields'] + " " + df['Timestamp'].astype('str')

        # Join all lines into a single string
        data = '\n'.join(df['line'].tolist())
        # pd.Series([data]).to_csv("writing query.csv")
        end_i=time.time()
        print(end_i-start_i)

        # Construct the URL for writing data
        write_url = f"{self.url}/api/v2/write?org={self.org}&bucket={bucket}&precision=s"

        headers = {
            'Authorization': f'Token {self.token}',
            'Content-Type': 'text/plain'
        }

        # Send the HTTP POST request to write the data to InfluxDB
        try:
            response = requests.post(write_url, headers=headers, data=data)
            if response.status_code == 204:
                print(response.content)
                print("Time-series data successfully pushed to InfluxDB.")
            else:
                print(f"Failed to write data to InfluxDB. Status code: {response.status_code}")
                logger.error(f"Could not write data to Influx :: Response : {response}")
                print(f'The response is ::{response.content}')
                raise Exception(f"Could not write data to DB")
        except Exception as e:
            print(f"Error while writing to InfluxDB: {e}")
        
        end=time.time()
        print(f'Time taken:{end-start}')

    def remove_time_series_data(self, 
                        start_time: str,
                        end_time: str,
                        bucket: str,
                        measurement: str,
                        tag: dict 
                        ) -> None:
        """
        Removes time series data from InfluxDB for a given time range, bucket, measurement and tag.

        Args:
            start_time (str): Start time for the data in the format 'YYYY-MM-DDTHH:MM:SSZ'.
            end_time (str): End time for the data in the same format.
            bucket (str): The name of the bucket in InfluxDB from which the data is to be removed.
            measurement (str): The measurement within the bucket from which data is to be removed.
            tag (dict): A dictionary containing tags for removing data. The keys are the tag names 
                        and the values are lists of elements with the tag name.

        Returns:
            None
        """

        # create an instance of the InfluxDBClient
        client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        
        # access the delete api
        delete_api = client.delete_api()

        # split the tag dictionary into separate lists of keys and values
        keys = list(tag.keys())
        values = list(tag.values())

        # flag to check if delete operation was called
        deletion_flag = False

        for combination in product(*values):
            output1 = [f'_measurement = "{measurement}"']

            # build the delete predicate for each combination of tags
            for i in range(len(keys)):
                output = ''.join(f'{keys[i]} = "{combination[i]}"')
                output1.append(output)
            
            delete_predicate = ' AND '.join(output1)

            # execute the delete operation
            delete_api.delete(start_time, end_time, delete_predicate, bucket=bucket, org=self.org)
            
            # mark the flag as True since delete operation was called
            deletion_flag = True

        # Close the client resource
        client.__del__()

        if deletion_flag:
            print("Time-series data successfully removed from InfluxDB.")
    
    def fetch_time_series_data(self,
                        start_time: str,
                        end_time: str,
                        resampling_freq: str,
                        bucket: str,
                        measurement: str,
                        tag: dict,
                        field_name:list=["_value"]  # Adjusted parameter to be a list of field names
                        ) -> pd.DataFrame:
        """
        Fetches time series data from InfluxDB for a given time range, resampling frequency, 
        bucket, measurement, tag, and field names.
        """
        start = time.time()
        
        # Initialize a new dataframe to store the results
        df_all = pd.DataFrame()

        # Split the tag dictionary into separate lists of keys and values
        keys = list(tag.keys())
        values = list(tag.values())

        for combination in product(*values):
            output1 = []

            # Build the flux query filter for each combination of tags
            for i in range(len(keys)):
                output = ''.join(f'r["{keys[i]}"] == "{combination[i]}"')
                tag_id = combination
                output1.append(output)
            
            filter_condition = ' and '.join(output1)
            
            field_output=[]
            for i in field_name:
                output= ''.join(f'r["_field"] == "{i}"')
                field_output.append(output)
            
            field_condition= ' or '.join(field_output)

            # Formulate the flux query for multiple field names
            flux_query = f'''
                from(bucket: "{bucket}")
                |> range(start: {start_time}, stop: {end_time})
                |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                |> filter(fn: (r) => {filter_condition})
                |> filter(fn: (r) => {field_condition})
                |> aggregateWindow(every: {resampling_freq}, fn: mean, createEmpty: false)
                |> yield(name: "filtered_data")
            '''
            logger.debug(f"Fetching time series data :: query : {flux_query}")
            query_url = f"{self.url}/api/v2/query?org={self.org}"

            # Headers
            headers = {
                "Authorization": f"Token {self.token}",
                "Content-Type": "application/vnd.flux"
            }
            
            response = requests.post(query_url, headers=headers, data=flux_query)

            # Check if the request was successful
            if response.status_code == 200:
                print('Query Successful')
                try:
                    df = pd.read_csv(StringIO(response.text), header=0)
                    # Since we have multiple fields, we need to handle each field separately
                    for field in field_name:
                        series = pd.Series(data=df[field].values, 
                                           index=pd.to_datetime(df['_time']), 
                                           name=f"{tag_id[0]}_{field}")
                        if df_all.empty:
                            
                            df_all = pd.DataFrame(series).rename(columns={series.name: f"{tag_id[0]}_{field}"})
                        else:
                            # If df_all already exists and has data, join with the new series
                            df_all = df_all.join(series, how='outer')
                        # df_all = pd.concat([df_all, series], axis=1)
                except pd.errors.EmptyDataError:
                    print(f"No data found for tag: {combination[0]}")
            else:
                print(f"Failed to query data. HTTP Status Code: {response.status_code}")
                print(response.text)

        df_all.rename_axis('Timestamp', inplace=True)
        df_all.sort_index(inplace=True)
        end = time.time()
        print(f'Time taken: {end - start}')
        
        print(f"Bucket: {bucket} >> Measurement: {measurement}")
        return df_all


# Create a ConfigParser instance
config = configparser.ConfigParser()

# Read the config file
current_file_path = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file_path)
config.read(os.path.join(current_directory, '..', '..', 'config', 'DBconfig.ini'))

# Get the username and password from the 'database' section

# token = config['InfluxDB']['token']
# org = config['InfluxDB']['org']
# url = config['InfluxDB']['url']
url = "http://34.29.169.181:8086"
token = "XwxIMQs0iaQF_TO3L43JpJON3JzN1fH6ZC6jaTHWESxmcve78uJLszEybs6SrZA51v3JBjfatS9djEDqWbCJuw=="
org = "algo8"

# api = APIHandler(url, token, org)

api = APIHandler(url=url, token=token, org=org)

# df = pd.read_csv(r"")

##--------------------------------------------------------------------------------------------------------------------------------------##

if __name__ == "__main__":
    ser = api.fetch_time_series_data(start_time="2019-01-24T04:39:09Z", end_time="2023-07-24T04:39:09Z", resampling_freq='1d', bucket="iocl_paradip_dev", measurement="tag_data", tag={'sensor_id':['013TI0055.DACA.PV'], 'sensor_data':['raw_sensor_data']}, field_name=["_value"])
    print(ser)

# curl -X POST "http://34.29.169.181:8086/api/v2/write?org=algo8&bucket=predictive_maintenance&precision=s" \
#   -H "Authorization: Token XwxIMQs0iaQF_TO3L43JpJON3JzN1fH6ZC6jaTHWESxmcve78uJLszEybs6SrZA51v3JBjfatS9djEDqWbCJuw==" \
#   -H "Content-Type: text/plain" \
#   --data-raw 'testing_measurement_1m,equipment=mabCompressor mabCompressor_airMassFlowinTon(TonTon|hr)__anomalyScore=21.18204680407152'
