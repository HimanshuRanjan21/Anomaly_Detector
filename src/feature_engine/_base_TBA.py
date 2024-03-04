from sklearn.base import BaseEstimator, TransformerMixin
from tsfresh import extract_features
import pandas as pd


class FeatureEngine(BaseEstimator, TransformerMixin):
    def __init__(self, model_config):
        self.model_config = model_config

        feature_engineering_config = self.model_config['feature_engineering']
        self.default_features = feature_engineering_config['default']['features']
        self.time_series_specific = feature_engineering_config['time_series_specific']
        self.rolling_window = feature_engineering_config.get('rolling_window', None)  # Added this line

    def fit(self, X, y=None):
        # Placeholder, as TSFRESH is stateless
        return self

    def transform(self, X, y=None):
        feature_dfs = []

        for series_name in X.columns:
            specific_config = next((item for item in self.time_series_specific if item["series_name"] == series_name), None)
            features = specific_config["features"] if specific_config else self.default_features

            fc_parameters = {feature: None for feature in features}

            temp_df = X[[series_name]].copy().reset_index()
            temp_df['id'] = series_name
            temp_df.rename(columns={'index': 'time', series_name: 'value'}, inplace=True)

            if self.rolling_window:
                rolled_dfs = []
                for window_start in range(0, len(temp_df) - self.rolling_window + 1):
                    rolled_window = temp_df.iloc[window_start:window_start + self.rolling_window].copy()
                    rolled_window['id'] = f"{series_name}_roll_{window_start}"
                    rolled_dfs.append(rolled_window)
                temp_df = pd.concat(rolled_dfs, ignore_index=True)

            extracted_features = extract_features(temp_df,
                                                  column_id='id',
                                                  column_sort='time',
                                                  column_value='value',
                                                  default_fc_parameters=fc_parameters)

            # Add series_name as prefix to column names
            extracted_features.columns = [f"{series_name}__{col}" for col in extracted_features.columns]
            feature_dfs.append(extracted_features)

        # Concatenating along axis=1 (columns)
        final_features = pd.concat(feature_dfs, axis=1)
        
        if final_features.empty:
            print("No features could be extracted. Please check your config and input data.")
            return None
        
        # Resetting index to original DataFrame's datetime index
        final_features.index = X.index[0:len(final_features)]

        return final_features
    

if __name__=="__main__":
    # Example usage
    feature_engineering_config = {
        "default": {
        "features": ["mean", "std", "max"],
        "reference_name": "default_features"
        },
        "time_series_specific": [
        {
            "series_name": "flow_tag",
            "features": ["mean", "minimum", "maximum"],
            "reference_name": "flow_tag"
        },
        {
            "series_name": "temp_tag",
            "features": ["mean", "minimum", "maximum"],
            "reference_name": "temp_tag"
        }
        ],
        "rolling_window": 2
    }
    model_config = {}
    model_config["feature_engineering"] = feature_engineering_config

    X = pd.DataFrame({
        'flow_tag': [1, 2, 3, 4, 5],
        'temp_tag': [30, 32, 29, 28, 31]
    }, index=pd.date_range(start='2022-01-01', periods=5, freq='D'))

    engine = FeatureEngine(model_config=model_config)
    engine.fit(X)  # Placeholder
    transformed_X = engine.transform(X)

    print("X: ",X)
    print("transformed X:\n", transformed_X)