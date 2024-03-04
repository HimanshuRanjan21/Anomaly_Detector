# from ._influxdatabase import api
import pandas as pd
from datetime import datetime, timedelta

flowsheet_entity_influx_bucket_mapping = {
    "Paradip": "iocl_DT_test",
    "test": "paradip_test"
}

def convert_datetime_to_influxdb_string(dt):
    if not isinstance(dt, datetime):
        dt = pd.to_datetime(dt)
    influxdb_format = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return influxdb_format


class SensorDBmanager:
    """
    Acts as an intermediary between objects that call to interact with sensor data and the service (in this case, _influxdatabase.py file) that interacts with sensor database
    """
    def __init__(self, api, DB='InfluxDB'):
        self.api = api
        self.DB = DB

    def fetch_sensor_data(self, 
                        start_time,
                        end_time,
                        sensors: list,
                        resampling_freq: str='1m',
                        flowsheet_entity: str="Paradip",
                        measurement: str='tag_data',
                        sensor_data_type: list=['raw_sensor_data'],
                        field_name:list=['_value']
                        ) -> pd.DataFrame:
        df = None
        if self.DB=='InfluxDB':
            bucket = flowsheet_entity_influx_bucket_mapping[flowsheet_entity]
            
            if isinstance(start_time, datetime):
                start_time = convert_datetime_to_influxdb_string(start_time)
            if isinstance(end_time, datetime):
                end_time = convert_datetime_to_influxdb_string(end_time)
            print(start_time, end_time)
            df = self.api.fetch_time_series_data(start_time=start_time,
                                                 end_time=end_time,
                                                 resampling_freq=resampling_freq,
                                                 bucket=bucket,
                                                 measurement=measurement,
                                                 tag={'sensor_id':sensors, "sensor_data": sensor_data_type},
                                                 field_name=field_name
                                                 )
        return df
    
    def fetch_KPI_data(self, 
                        start_time,
                        end_time,
                        KPIs: list,
                        resampling_freq: str='1d',
                        flowsheet_entity: str="Paradip",
                        measurement: str='KPI_data',
                        field_name:list=['_value']
                        ) -> pd.DataFrame:
        """
        Fetches KPI data
        """
        df = None
        if self.DB=='InfluxDB':
            bucket = flowsheet_entity_influx_bucket_mapping[flowsheet_entity]
            if isinstance(start_time, datetime):
                start_time = convert_datetime_to_influxdb_string(start_time)
            if isinstance(end_time, datetime):
                end_time = convert_datetime_to_influxdb_string(end_time)
            df = self.api.fetch_time_series_data(start_time=start_time,
                                                 end_time=end_time,
                                                 resampling_freq=resampling_freq,
                                                 bucket=bucket,
                                                 measurement=measurement,
                                                 tag={'KPI_name':KPIs},
                                                 field_name=field_name
                                                 )
        return df
    
    def fetch_lab_data(self, 
                        start_time,
                        end_time,
                        process_fluid: list,
                        properties: list,
                        resampling_freq: str='1m',
                        flowsheet_entity: str="Paradip",
                        measurement: str='lab_data',
                        field_name:list=['_value']
                        ) -> pd.DataFrame:
        """
        Fetches KPI data
        """
        df = None
        if self.DB=='InfluxDB':
            bucket = flowsheet_entity_influx_bucket_mapping[flowsheet_entity]
            if isinstance(start_time, datetime):
                start_time = convert_datetime_to_influxdb_string(start_time)
            if isinstance(end_time, datetime):
                end_time = convert_datetime_to_influxdb_string(end_time)
            df = self.api.fetch_time_series_data(start_time=start_time,
                                                 end_time=end_time,
                                                 resampling_freq=resampling_freq,
                                                 bucket=bucket,
                                                 measurement=measurement,
                                                 tag={'process_fluid':process_fluid, "properties": properties},
                                                 field_name=field_name
                                                 )
        return df
    
    def write_sensor_data(self,
                          df: pd.DataFrame, 
                          flowsheet_entity: str="Paradip",
                          measurement: str="tag_data", 
                          data_type: str="cleaned_data",
                          field_name: list=['_value']
                          ) -> None:
        """
        Writes sensor data to DB
        """

        # Convert df in the form acceptable to write_time_series_data function
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)
        
        sensor_name = (df.columns[0])
        df.rename(columns={df.columns[0]: '_value'}, inplace=True)
        df['sensor_id'] = sensor_name
        df['sensor_data']  = data_type
        df.reset_index(inplace=True)

        # This melted_df part is written in a way that df.columns[2] is 'sensor_id' and df.columns[3] is 'sensor_data'
        # df
        #
        #   Timestamp                  _value       sensor_id    sensor_data
        # 0 2021-01-08 00:00:00        31.944055    1TI1180      cleaned_data
        # 1 2021-01-09 00:00:00        30.189806    1TI1180      cleaned_data
        # 2 2021-01-10 00:00:00        32.447337    1TI1180      cleaned_data

        if self.DB=='InfluxDB':
            bucket = flowsheet_entity_influx_bucket_mapping[flowsheet_entity]
            self.api.write_time_series_data(df=df, 
                                            bucket='iocl_DT_test', 
                                            measurement=measurement, 
                                            tag_name=['sensor_id', 'sensor_data'],
                                            field_name=field_name
                                            )
    
    def write_KPI_data(self,
                          df: pd.Series, 
                          flowsheet_entity: str="Paradip", 
                          measurement: str="KPI_data",
                          field_name: list=['_value']
                          ) -> None:
        """
        Writes KPI data to DB
        """

        # Convert df in the form acceptable to write_time_series_data function
        if not pd.api.types.is_datetime64_any_dtype(df.index):
            df.index = pd.to_datetime(df.index)
        
        df = pd.DataFrame(df)
        KPI_name = (df.columns[0])
        df.rename(columns={df.columns[0]: '_value'}, inplace=True)
        df['KPI_name'] = KPI_name
        # df['KPI_data']  = data_type
        df.reset_index(inplace=True)

        # This melted_df part is written in a way that df.columns[2] is 'sensor_id' and df.columns[3] is 'sensor_data'
        # df
        #
        #   Timestamp                  _value       sensor_id    sensor_data
        # 0 2021-01-08 00:00:00        31.944055    1TI1180      cleaned_data
        # 1 2021-01-09 00:00:00        30.189806    1TI1180      cleaned_data
        # 2 2021-01-10 00:00:00        32.447337    1TI1180      cleaned_data

        if self.DB=='InfluxDB':
            bucket = flowsheet_entity_influx_bucket_mapping[flowsheet_entity]
            self.api.write_time_series_data(df=df, 
                                            bucket=bucket, 
                                            measurement=measurement, 
                                            tag_name=['KPI_name'],
                                            field_name=field_name   # [df.columns[2], df.columns[3]]
                                            )

# tag_data_manager = SensorDBmanager(api=api)

# if __name__=='__main__':
#     print(tag_data_manager.fetch_sensor_data(start_time=datetime(2020,1,1), end_time=datetime(2023,1,1), resampling_freq='1d', bucket="HEx_pilot", measurement="tag_data", tag={'sensor_id':['1TI1208'], 'sensor_data':['raw_data']}))