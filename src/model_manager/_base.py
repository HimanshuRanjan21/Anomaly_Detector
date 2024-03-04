from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import os
import pickle

# Model files path
# models_folder_path = "models"
#change this before deploymnet and training models
models_folder_path='models_new_test'

class ModelManager(BaseEstimator, TransformerMixin):
    """
    Model name storing convention:
    [model_id]_[sub_model_id]__[series].pickle
    """
    def __init__(self, model_config: dict) -> None:
        self.model_config = model_config
        self.model_id = model_config['model_id']
        self.ML_implementation = model_config['ML_implementation']

    def _fetch_model(self, sub_model_id: str, series_name: str) -> pickle:
        model_name = self.model_id + "_" + sub_model_id + "__" + series_name + ".pickle"
        # Load the model from the file
        with open(os.path.join(models_folder_path, model_name), 'rb') as f:
            loaded_model = pickle.load(f)
        print(f"Fetching model :: model type : {type(loaded_model)}")
        return loaded_model
    
    def _gather_model_df(self, X: pd.DataFrame, series_name: str) -> pd.DataFrame:
        cols = []
        for col in X.columns:
            print("series name: ", series_name, "col: ", "_".join(col.split("_")[:2]))
            if "_".join(col.split("_")[:2]) == series_name:
                cols.append(col)
        
        df = X[cols].copy()
        return df
    
    def save_model(self, models_dict: dict) -> None:
        for model in models_dict:
            sub_model_id = model["sub_model_id"]
            series_name = model['series_name']
            ML_model = model['model']

            model_name = self.model_id + "_" + sub_model_id + "__" + series_name + ".pickle"
            print(f"Model name : {model_name}")
            print(f"Filename : {os.path.join(models_folder_path, model_name)}")

            model_path = os.path.join(models_folder_path, model_name)

            os.makedirs(os.path.dirname(model_path), exist_ok=True)  # Create the directory if it doesn't exist

            with open(model_path, 'wb') as f:
                pickle.dump(ML_model, f)
            print(f"Model saved :: sub_model_id : {sub_model_id} :: series name : {series_name}")

    def create_new_models(self, X: pd.DataFrame) -> list:
        models_list = []

        # Iterate over ML_impl models
        for ML_imp in self.ML_implementation:
            sub_model_list = []
            sub_model_id = ML_imp["sub_model_id"]

            # Check if model is univariate or multivariate


            if ML_imp["implementation_type"] =="Multivariate":
                series="multivariate"
                model_type = ML_imp['model']
                series_df=X
                sub_model_list.append(
                        {
                            "model_type": model_type,
                            "series_df": series_df,
                            "sub_model_id": sub_model_id,
                            "series_name": series,
                            "implementation_type":"Multivariate"

                        })
                


            if ML_imp["implementation_type"]=="Univariate":
                series = ML_imp["series_names"]
                model_type = ML_imp['model']

                if model_type == "IsolationForest":
                    # Iterate over each series to gather model and dataframe
                    for ser in series:
                        series_df = self._gather_model_df(X=X, series_name=ser)
                        sub_model_list.append(
                            {
                                "model_type": model_type,
                                "series_df": series_df,
                                "sub_model_id": sub_model_id,
                                "series_name": ser,
                                "implementation_type":"Univariate"
                            }
                        )

                
            models_list.append(sub_model_list)
        
        return models_list

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame, y=None) -> list:
        models_list = []

        # Iterate over ML_impl models
        for ML_imp in self.ML_implementation:
            sub_model_list = []
            sub_model_id = ML_imp["sub_model_id"]
            print('%'*40)
            print(f'THE SUB MODLEl ID IS  {sub_model_id}')


            if ML_imp["implementation_type"] == "Multivariate":

                # ot = ML_imp['output']
                # Fetching model
                model_name=self.model_id + "_" + sub_model_id + "__" + "multivariate" + ".pickle"
                with open(os.path.join(models_folder_path, model_name), 'rb') as f:
                    model = pickle.load(f)
                print('*-'*40)
                print('multivaraite_model_loaded')
                

                sub_model_list.append(
                    {
                        "model": model,
                        "series_df": X,
                        "sub_model_id":sub_model_id,
                        "series_name": 'multivariate'
                    }
                )

            # Check if model is univariate or multivariate
            if ML_imp["implementation_type"] == "Univariate":
                series = ML_imp["series_names"]
                # output = ML_imp['output']

                # Iterate over each series to gather model and dataframe
                for ser in series:
                    model = self._fetch_model(sub_model_id=sub_model_id, series_name=ser)
                    series_df = self._gather_model_df(X=X, series_name=ser)
                    sub_model_list.append(
                        {
                            "model": model,
                            "series_df": series_df,
                            "sub_model_id":sub_model_id,
                            "series_name": ser
                        }
                    )
            models_list.append(sub_model_list)

        return models_list
