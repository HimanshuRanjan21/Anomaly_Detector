from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
from pandas import Timestamp


class DataCleaning(BaseEstimator, TransformerMixin):
    def __init__(self, model_config):
        self.model_config = model_config

    def fit(self, X, y=None):
        return self
    
    def transform(self, X: pd.DataFrame, y=None):
        # Apply universal methods
        if "universal" in self.model_config['data_cleaning']:
            for key, value in self.model_config['data_cleaning']['universal'].items():
                if key == "shutdown_period_removal":
                    X = self.remove_shutdown_period_rows(X)

        # Apply default methods
        if "default" in self.model_config['data_cleaning']:
            for key, value in self.model_config['data_cleaning']['default'].items():
                if key == "handle_missing_values" and value == "mean":
                    X = X.apply(lambda col: col.fillna(col.mean()), axis=0)

        # Apply series-specific methods
        if "series_specific" in self.model_config['data_cleaning']:
            for column, methods in self.model_config['data_cleaning']['series_specific'].items():
                for key, value in methods.items():
                    if key == "negative_values" and value == "mean":
                        mean_value = X.loc[X[column] >= 0, column].mean()
                        X.loc[X[column] < 0, column] = mean_value
        return X

    def remove_shutdown_period_rows(self, X: pd.DataFrame):
        if 'shutdown_period' in self.model_config:
            for period in self.model_config['shutdown_period']:
                start_time = pd.to_datetime(period['start_time'])
                end_time = pd.to_datetime(period['end_time'])

                # Make sure the timezones are compatible for comparison
                if start_time.tz is not None and X.index.tz is None:
                    X.index = X.index.tz_localize(start_time.tz)
                elif start_time.tz is None and X.index.tz is not None:
                    start_time = start_time.tz_localize(X.index.tz)
                    end_time = end_time.tz_localize(X.index.tz)

                X = X[(X.index < start_time) | (X.index > end_time)]
        return X
