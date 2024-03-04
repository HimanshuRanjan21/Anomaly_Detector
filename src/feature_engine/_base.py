from sklearn.base import BaseEstimator, TransformerMixin
# from tsfresh import extract_features
import pandas as pd
import logging
from .featurestore import feature_list

# Configure the root logger
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

class FeatureEngine(BaseEstimator, TransformerMixin):
    def __init__(self, model_config):
        logger.debug(f"Initializing feature engine")
        self.model_config = model_config
        self.model_id = model_config["model_id"]

        feature_engineering_config = self.model_config['feature_engineering']
        self.default_features = feature_engineering_config['default']['features']
        self.time_series_specific = feature_engineering_config['time_series_specific']
        self.rolling_window = feature_engineering_config.get('rolling_window', None)  # Added this line

        logger.debug("Initializing feature engine :: feature engine initialized")

    def fit(self, X, y=None):
        return self
    
    def transform(self, X: pd.DataFrame, y=None):
        logger.info(f"Feature engineering :: {self.model_id}")
        df = pd.DataFrame()  # Dataframe to store all the features
        series = X.columns

        logger.debug(f"Feature engineering :: columns : {series}")
        # Create features
        for ser in series:
            temp_df = self.feature(X[ser])
            df = pd.concat([df, temp_df], axis=1)
        
        return df
    
    def feature(self, df: pd.DataFrame) -> pd.DataFrame:
        roll = self.rolling_window

        # Calculate mean and add prefix
        df_mean = df.rolling(roll).mean()
        prefix1 = '_mean_' + roll
        df_mean.name = df_mean.name + prefix1

        # Calculate standard deviation and add prefix
        df_std = df.rolling(roll).std()
        prefix2 = '_std_' + roll
        df_std.name = df_std.name + prefix2

        # # Calculate autocorrelation and add prefix
        # df_corr = df.rolling(roll).apply(lambda x: x.autocorr())
        # prefix3 = '_autocorr_' + roll
        # df_corr.name = df_corr.name + prefix3

        # Calculate min and add prefix
        df_min = df.rolling(roll).min()
        prefix4 = '_min_' + roll
        df_min.name = df_min.name + prefix4

        # Calculate max and add prefix
        df_max = df.rolling(roll).max()
        prefix5 = '_max_' + roll
        df_max.name = df_max.name + prefix5

        # Concatenate all the feature DataFrames
        dfs = [df_mean, df_std, df_min, df_max]
        df_features = pd.concat(dfs, axis=1)

        return df_features