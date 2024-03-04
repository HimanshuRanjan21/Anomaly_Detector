# Import dependencies
from datetime import datetime
import pandas as pd
from .influxDB import api
from ..utils.helpers import convert_datetime_to_influxdb_string
import logging

# Configure the root logger
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


class DataIngestion():
    def __init__(self, model_config, DB: str="InfluxDB"):
        logger.info(f"Initializing data ingestion")
        self.model_config = model_config
        self.model_id = model_config["model_id"]
        self.DB = DB
        logger.info(f"Initializing data ingestion :: data ingestion object initialized :: model_id : {self.model_id}")
    
    def _fetch_time_series(self, cred: dict, start_time: datetime, end_time: datetime) -> pd.Series:
        logger.debug(f"Fetching time series")
        if cred['source'] == self.DB:
            logger.debug(f"Fetching time series :: DB : Influx")
            db_details = cred['db_details']
            
            start_time = convert_datetime_to_influxdb_string(start_time)
            end_time = convert_datetime_to_influxdb_string(end_time)
            logger.debug(f"Fetching time series :: start time : {start_time} :: end time : {end_time}")

            series = api.fetch_time_series_data(
                start_time=start_time,
                end_time=end_time,
                resampling_freq=cred['granularity'],
                bucket=db_details['bucket'],
                measurement=db_details['measurement'],
                tag=db_details['tags']
            )

            logger.debug(f"Fetching time series :: series : {series} :: series type : {type(series)}")

            if len(series.columns)==1:
                logger.debug(f"Fetching time series :: series has a single column")
                series = series[series.columns[0]]
                series.name = cred['name']  # Assign name to series
            else:
                logger.error(f"Fetching time series :: failed :: series has either multiple or 0 columns :: number of columns : {len(series.columns)}")
                raise Exception(f"Fetched series has more than 1 columns. credentials : {cred}")
        
        # Check if the index is already of datetime dtype
        if not isinstance(series.index, pd.DatetimeIndex):
            try:
                # Try converting the index to a datetime dtype
                series.index = pd.to_datetime(series.index)
            except Exception as e:
                # Handle exceptions (e.g., invalid format, etc.)
                logger.error(f"Fetching time series :: series index could not be converted to datetime index :: Error occured : {e}")
                raise Exception(f"An error occurred while converting index to datetime: {e}")

        return series

    def fetch_raw_data(self, start_time: datetime, end_time: datetime):
        logger.info(f"Fetching raw data :: start time : {start_time} :: end time : {end_time}")
        df = pd.DataFrame()

        # Iteratively fetch time series
        for series in self.model_config['time_series']:
            logger.debug(f"Fetching times series :: series : {series}")
            temp_df = self._fetch_time_series(cred=series, start_time=start_time, end_time=end_time)
            df = pd.concat([df, temp_df], axis=1)

        df.index = pd.to_datetime(df.index)
        df.sort_index(inplace=True)
        logger.debug(f"raw data : {df}")
        # Return the fetched time series
        return df

    def fetch_features(self):
        # Perform data cleaning based on cleaning_config
        pass