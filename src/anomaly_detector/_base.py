from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from keras.layers import Input, Dense
from keras.models import Model
from keras.callbacks import EarlyStopping
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.neighbors import LocalOutlierFactor


class AnomalyDetector(BaseEstimator, TransformerMixin):
    def __init__(self, model_config: dict) -> None:
        self.model_config = model_config

    def fit(self, X, y=None):
        return self
    
    def retrain_model(self, models_list: list):
        models = []

        for sub_model_list in models_list:
            for sub_model in sub_model_list:
                model_type = sub_model['model_type']
                series_df = sub_model['series_df']
                sub_model_id = sub_model['sub_model_id']
                series_name = sub_model['series_name']
                implementation_type=sub_model["implementation_type"]

                if implementation_type=="Univariate":

                    if model_type=="IsolationForest":
                        from sklearn.ensemble import IsolationForest

                        model = IsolationForest(random_state=42, contamination=0.1)
                        model.fit(series_df)

                if implementation_type=="Multivariate":
                    
                    if model_type=="Autoencoder":

                        input_layer = Input(shape=(series_df.shape[1],))
                        encoded = Dense(8, activation='relu')(input_layer)
                        decoded = Dense(series_df.shape[1], activation='linear')(encoded)
                        model = Model(inputs=input_layer, outputs=decoded)
                        model.compile(optimizer='adam', loss='mean_squared_error')

                        early_stopping = EarlyStopping(monitor='val_loss', patience=5, min_delta=0.0001, verbose=1)


                        model.fit(series_df, series_df, epochs=50, batch_size=32, validation_split=0.1, callbacks=[early_stopping])

                    if model_type == "IsolationForest":


                        model = IsolationForest(contamination=0.03)
                        model.fit(series_df)
                        
                  
                    if model_type=="LOF":
                        model=LocalOutlierFactor(contamination=0.02,novelty=True)
                        model.fit(series_df)                            
                  
                  
                  
                models.append(
                        {
                            "model": model,
                            "sub_model_id": sub_model_id,
                            "series_name": series_name
                        }
                    )
        
        return models

    # def transform(self, X: list, y=None):
    #     results={}
    #     df1 = pd.DataFrame()
    #     df2 = pd.DataFrame()

    #     for sub_model_list in X:
    #         for sub_model in sub_model_list:

    #             model = sub_model['model']
    #             series_df = sub_model['series_df']
    #             sub_model_id=sub_model["sub_model_id"]
    #             # output = sub_model['output']
    #             series_name = sub_model['series_name']

    #             if series_name == "multivariate":
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df1[f'{series_name}_multivariate'] = anomaly_scores
    #                 df1[f"{series_name}_multivariate"] = 100*(0.5 - df1[f'{series_name}_multivariate'])
    #                 df1.index = series_df.index


    #             else:

    #                 print("Sub model: \n",sub_model)
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df2[f'{series_name}'] = anomaly_scores
    #                 df2[f"{series_name}"] = 100*(0.5 - df[f'{series_name}'])
    #     df1.index = series_df.index
    #     df2.index = series_df.index

    #     return df


    # def transform(self, X: list, y=None):
    #     results = {}

    #     for sub_model_list in X:
    #         for sub_model in sub_model_list:
    #             model = sub_model['model']
    #             series_df = sub_model['series_df']
    #             sub_model_id = sub_model["sub_model_id"]
    #             series_name = sub_model['series_name']

    #             if series_name == "multivariate":
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df = pd.DataFrame({f'{series_name}_multivariate': anomaly_scores})
    #                 df[f"{series_name}_multivariate"] = 100 * (0.5 - df[f'{series_name}_multivariate'])
    #                 df.index = series_df.index
    #                 results[sub_model_id] = df

    #             else:
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df = pd.DataFrame({f'{series_name}': anomaly_scores})
    #                 df[f"{series_name}"] = 100 * (0.5 - df[f'{series_name}'])
    #                 df.index = series_df.index

    #                 if sub_model_id not in results:
    #                     results[sub_model_id] = df
    #                 else:
    #                     # Concatenate the results for the same sub_model_id
    #                     results[sub_model_id] = pd.concat([results[sub_model_id], df], axis=1)

    #     return results
    
####################################################################################################################
    # def transform(self, X: list, y=None):
    #     results = []

    #     for sub_model_list in X:
    #         sub_results={}
    #         for sub_model in sub_model_list:
    #             model = sub_model['model']
    #             series_df = sub_model['series_df']
    #             sub_model_id = sub_model["sub_model_id"]
    #             series_name = sub_model['series_name']

    #             if series_name == "multivariate":
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df = pd.DataFrame({f'{series_name}_multivariate': anomaly_scores})
    #                 df[f"{series_name}_multivariate"] = 100 * (0.5 - df[f'{series_name}_multivariate'])
    #                 df.index = series_df.index
    #                 sub_results["sub_model_id"] = sub_model_id
    #                 sub_results["anomaly_score"]=df
    #                 sub_results["implementation"]="multivariate"
    #                 sub_results["model"] = model.__class__.__name__

    #             else:
    #                 # univariate
    #                 anomaly_scores = model.decision_function(series_df)
    #                 df = pd.DataFrame({f'{series_name}': anomaly_scores})
    #                 df[f"{series_name}"] = 100 * (0.5 - df[f'{series_name}'])
    #                 df.index = series_df.index
    #                 # sub_results["sub_model_id"] = sub_model_id
    #                 # sub_results["model"] = model.__class__.__name__
    #                 # sub_results["implementation"]="univariate"

    #                 if sub_model_id not in results:
    #                     sub_results["anomaly_score"]=df
    #                 else:
    #                     # Concatenate the results for the same sub_model_id
    #                     sub_results["anomaly_score"] = pd.concat([results[sub_model_id], df], axis=1)
    #                 sub_results["sub_model_id"] = sub_model_id
    #                 sub_results["model"] = model.__class__.__name__
    #                 sub_results["implementation"]="univariate"
    #             results.append(sub_results)

    #     return results
    ####################################################################################################################

    def transform(self, X: list, y=None):
        results = {}

        for sub_model_list in X:
            sub_results = {}
            for sub_model in sub_model_list:
                model = sub_model['model']
                series_df = sub_model['series_df']
                sub_model_id = sub_model["sub_model_id"]
                series_name = sub_model['series_name']

                if series_name == "multivariate":
                    #Isolation forest
                    if model.__class__.__name__=="IsolationForest":


                        anomaly_scores = model.decision_function(series_df)
                        df = pd.DataFrame({f'{model.__class__.__name__}_multivariate': anomaly_scores})
                        # df[f"{model.__class__.__name__}_multivariate"] = 100 * (0.5 - df[f'{model.__class__.__name__}_multivariate'])
                        df.index = series_df.index
                        sub_results["implementation"] = "multivariate"
                        sub_results["model"] = model.__class__.__name__
                        sub_results["anomaly_score"] = df
                        results[sub_model_id] = sub_results

                    if model.__class__.__name__=="Functional":

                        # Autoencoder

                        anomaly_scores = np.mean(np.square(series_df - model.predict(series_df)), axis=1)
                        df = pd.DataFrame({'Autoencoder_multivariate': anomaly_scores})
                        # df[f"{series_name}_multivariate"] = 100 * (0.5 - df[f'{series_name}_multivariate'])
                        df.index = series_df.index
                        sub_results["implementation"] = "multivariate"
                        sub_results["model"] = model.__class__.__name__
                        sub_results["anomaly_score"] = df
                        results[sub_model_id] = sub_results

                    if model.__class__.__name__=="LocalOutlierFactor":
                        # local outlier factor  

                        anomaly_scores = model.decision_function(series_df)
                        df = pd.DataFrame({f'{model.__class__.__name__}_multivariate': anomaly_scores})
                        # df[f"{model.__class__.__name__}_multivariate"] = 100 * (0.5 - df[f'{model.__class__.__name__}_multivariate'])
                        df.index = series_df.index
                        sub_results["implementation"] = "multivariate"
                        sub_results["model"] = model.__class__.__name__
                        sub_results["anomaly_score"] = df
                        results[sub_model_id] = sub_results


                if series_name != "multivariate":
                    # univariate
                    print('The series getting executed is {}'.format(series_name))
                    anomaly_scores = model.decision_function(series_df)
                    df = pd.DataFrame({f'{series_name}': anomaly_scores})
                    # df[f"{series_name}"] = 100 * (0.5 - df[f'{series_name}'])
                    df.index = series_df.index
                    sub_results["implementation"] = "univariate"
                    sub_results["model"] = model.__class__.__name__



                    if sub_model_id not in results:
                        sub_results["anomaly_score"] = df
                        results[sub_model_id] = sub_results
                    else:
                        results[sub_model_id]["anomaly_score"] = pd.concat([results[sub_model_id]["anomaly_score"], df], axis=1)

                        

        return results  
