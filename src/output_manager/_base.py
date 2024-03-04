from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from ..data_ingestion.influxDB import api
from ..utils.helpers import convert_datetime_to_influxdb_string
from datetime import datetime,timedelta

class OutputManager:
    def __init__(self, model_config: dict) -> None:
        self.model_config = model_config

        # Output config
        self.output_config = model_config["output_storage"]
        self.DB = self.output_config["DB"]

    def save_features(self, df: pd.DataFrame) -> None:
        # api.write_time_series_data(df=df, bucket="")
        pass

    def save_results(self, final_output) -> None:

        df=pd.DataFrame()
        k=0
        for output in final_output:
            dataframe=final_output[output]
            if k==0:
                df=dataframe
            else:
                df=pd.concat([df,dataframe],axis=1, join="outer")
            k=k+1
# fixing the delta here
        rolling_time=self.model_config["feature_engineering"]["rolling_window"]
        rolling_time=int(rolling_time[:-1])-2
        
        start_time=df.index[0]+timedelta(minutes=rolling_time)

        df=df.loc[df.index>start_time]



        print(f'The DATAFRAME to be pushed into influx is {df}')

        
        if self.DB=="InfluxDB":
            measurement = self.output_config["credentials"]["measurement"]
            bucket = self.output_config["credentials"]["bucket"]

            # Gather tags and fields
            data_storage = self.output_config["data_storage"]["result"]
            tags = data_storage["tags"]
            fields = data_storage["fields"]
            if fields==["all"]:
                fields = df.columns

            # Add tags in df
            for key, val in tags.items():
                df[key] = val

            # Make timestamp the first column
            df.reset_index(inplace=True)
            df.to_csv('data_toInflux.csv')
            print("The dataframe sent to api is ::",df)
            # n = len(df)//10
            # for i in range(1):
            #     temp_df = df.iloc[i*10:(i+1)*10].copy()
            #     temp_df = temp_df.dropna()
            #     print(df)
            api.write_time_series_data(
                
                df=df,
                bucket=bucket,
                measurement=measurement,
                tag_name=tags.keys(),
                field_name=fields
            )