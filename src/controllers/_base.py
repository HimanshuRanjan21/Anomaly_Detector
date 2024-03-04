import requests  # Importing the requests library for making API requests
from ..data_ingestion import DataIngestion
from ..data_processing import DataCleaning, DataProcessing, DataPostProcessing
from ..feature_engine import FeatureEngine
from ..model_manager import ModelManager
from ..anomaly_detector import AnomalyDetector
from ..output_manager import OutputManager
from sklearn.pipeline import Pipeline
from datetime import datetime,timedelta
import logging

logger = logging.getLogger(__name__)

class User:
    def APIRequest(self, url, headers=None, params=None):
        """
        Method to make API requests.
        
        Parameters:
        - url (str): The API endpoint URL
        - headers (dict): Optional headers for the API request
        - params (dict): Optional parameters for the API request
        
        Returns:
        - response (object): The API response
        """
        # Implement API request functionality here
        response = requests.get(url, headers=headers, params=params)
        return response

class APIGateway:
    def Authenticate(self, user_credentials):
        """
        Method for user authentication.
        
        Parameters:
        - user_credentials (dict): The user credentials for authentication
        
        Returns:
        - bool: True if authenticated, False otherwise
        """
        # Implement authentication functionality here
        return True

    def RouteRequest(self, request):
        """
        Method to route the incoming API requests to the appropriate controller.
        
        Parameters:
        - request (object): The incoming API request
        
        Returns:
        - response (object): The routed API response
        """
        # Implement request routing functionality here
        return "Routed"

class Controller:
    controllers = {}

    def __init__(self, model_config: dict):
        """
        Initialize the Controller class with various components.

        Parameters:
        - model_config (dict): The configuration for the model

        Returns:
        None
        """
        self.model_config = model_config
        self.data_ingestion = DataIngestion(self.model_config)
        self.data_cleaning = DataCleaning(self.model_config)
        self.feature_engine = FeatureEngine(self.model_config)
        self.data_processing = DataProcessing(self.model_config)
        self.model_manager = ModelManager(self.model_config)
        self.anomaly_detector = AnomalyDetector(self.model_config)
        self.post_processing = DataPostProcessing(self.model_config)
        self.output_manager = OutputManager(self.model_config)

        # Create the pipeline
        # self.prediction_pipeline1 = Pipeline([
        #     ('data_cleaning', self.data_cleaning),
        #     ('feature_engineering', self.feature_engine)
        # ])

        # self.prediction_pipeline2 = Pipeline([
        #     ('data_processing', self.data_processing),
        #     ('model_manager', self.model_manager),
        #     ('anomaly_detection', self.anomaly_detector),
        #     ('post_processing', self.post_processing)
        # ])

        Controller.controllers[self.model_config["model_id"]] = self

    def run_prediction_pipeline(self, start_time: datetime, end_time: datetime):
        

        rolling_time=self.model_config["feature_engineering"]["rolling_window"]
        rolling_time=int(rolling_time[:-1])
        
        start_time=start_time-timedelta(minutes=rolling_time)

        df=self.data_ingestion.fetch_raw_data(start_time=start_time, end_time=end_time)

        df=self.data_cleaning.transform(df)

        df= self.feature_engine.transform(df)



        df=self.data_processing.transform(df)


        models_list=self.model_manager.transform(df)


        anomaly_scores=self.anomaly_detector.transform(X=models_list)


        output=self.post_processing.univariate_post_processing(results=anomaly_scores)

        final_output=self.post_processing.transformed(results=output)

        print(final_output)

        self.output_manager.save_results(final_output)
        print('code Executed')


        # Fetch raw data
        # df = self.data_ingestion.fetch_raw_data(start_time=start_time, end_time=end_time)

        # # Run 1st pipeline
        # df = self.prediction_pipeline1.fit_transform(df)

        # # Save Features
        # self.output_manager.save_features(df)

        # # Run 2nd pipeline
        # df = self.prediction_pipeline2.fit_transform(df)

        # # Post process results
        # df = self.post_processing.process_results(df)

        # # Save results
        # self.output_manager.save_results(df)
    
    def run_retraining_pipeline(self, start_time: datetime, end_time: datetime) -> None:
        # Fetch raw data
        df = self.data_ingestion.fetch_raw_data(start_time=start_time, end_time=end_time)

        # Data cleaning
        df = self.data_cleaning.transform(df)

        # Feature enfineering
        df = self.feature_engine.transform(df)
        
        self.data_processing.save_scaler(df)
        # Data processing
        df = self.data_processing.transform(df)

        # Create new models
        models_list = self.model_manager.create_new_models(df)

        # Train models
        models = self.anomaly_detector.retrain_model(models_list=models_list)

        # Save models
        self.model_manager.save_model(models_dict=models)


def main():
    # models_config_path = os.path.join("..", "..", "config", "models")

    # model_files = os.
    pass


