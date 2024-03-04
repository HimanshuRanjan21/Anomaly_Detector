from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import os
import pickle
from sklearn.preprocessing import StandardScaler



scalers_folder_path = "scalers"

class DataProcessing(BaseEstimator, TransformerMixin):
    def __init__(self, model_config):
        
        self.model_config = model_config

    def fit(self, X, y=None):
        return self
    
    def transform(self, X: pd.DataFrame, y=None):
        # Apply mandatory methods
        if "mandatory" in self.model_config['data_preprocessing']:
            for method in self.model_config['data_preprocessing']['mandatory']['methods']:
                if method == "remove_na":
                    X = self.remove_na(X)
                    
        # Apply default methods
        if "default" in self.model_config['data_preprocessing']:
            for method in self.model_config['data_preprocessing']['default']['methods']:
                if method == "normalize":
                    X = self.normalize(X)
        
        # Apply time_series_specific methods (for future use)
        if "time_series_specific" in self.model_config['data_preprocessing']:
            for method in self.model_config['data_preprocessing']['time_series_specific']:
                # Implement when time_series_specific methods are added to the config
                pass
            
        return X
    
    def remove_na(self, X: pd.DataFrame):
        return X.dropna()
    
    def normalize(self, X: pd.DataFrame):
        columns=X.columns
        index=X.index

        with open(os.path.join(scalers_folder_path, f"{self.model_config['model_id']}.pickle"), 'rb') as file:
            scaler = pickle.load(file)
        X=scaler.transform(X)
        X=pd.DataFrame(X,index=index,columns=columns)
        return X
    

    def save_scaler(self, X: pd.DataFrame):

        if not os.path.exists(scalers_folder_path):
            os.makedirs(scalers_folder_path)

        scaler_file_path = os.path.join(scalers_folder_path, f"{self.model_config['model_id']}.pickle")

        scaler = StandardScaler()
        scaler.fit(X)
        with open(scaler_file_path, 'wb') as file:
            pickle.dump(scaler, file)

