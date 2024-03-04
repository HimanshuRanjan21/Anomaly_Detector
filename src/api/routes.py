from flask import Blueprint
from ..controllers import Controller
from flask import jsonify
import pandas as pd
from datetime import datetime

routes = Blueprint('routes' ,__name__)
timestamp_format = "%Y-%m-%d&%H:%M:%S"

@routes.route("/write_predictions/start_time=<start_time>/end_time=<end_time>/<model_id>", methods = ['POST', 'GET'])
def write_predictions(model_id: str, start_time: str, end_time: str):
    """
    Time format : %YYYY-%mm-%dd&HH:MM:SS
    """





    # logger = logging.getLogger("route")
    try:
        if model_id not in Controller.controllers.keys():
            return None
        controller_obj = Controller.controllers[model_id]

        # Run prediction pipeline
        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, timestamp_format)
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, timestamp_format)

        controller_obj.run_prediction_pipeline(start_time=start_time, end_time=end_time)

        # Gather message
        message = jsonify({'status': "Success!"})
        message.status_code= 200
        # logger.info('Gathered static attributes')

        return message
    except Exception as e:
        message = jsonify({'Error': e})
        message.status_code= 400
        # logger.error('Error for gathering static attributes. Error: {}'.format(e))

        return message
    
@routes.route("/retrain_model/start_time=<start_time>/end_time=<end_time>/<model_id>", methods = ['POST', 'GET'])
def retrain_model(model_id: str, start_time: str, end_time: str):
    """
    Time format : %YYYY-%mm-%dd&HH:MM:SS
    """
    try:
        if model_id not in Controller.controllers.keys():
            return None
        controller_obj = Controller.controllers[model_id]

        if isinstance(start_time, str):
            start_time = datetime.strptime(start_time, timestamp_format)
        if isinstance(end_time, str):
            end_time = datetime.strptime(end_time, timestamp_format)

        # Run prediction pipeline
        controller_obj.run_retraining_pipeline(start_time=start_time, end_time=end_time)

        # Gather message
        message = jsonify({'status': "Success!"})
        message.status_code= 200
        # logger.info('Gathered static attributes')

        return message
    except Exception as e:
        message = jsonify({'Error': e})
        message.status_code= 400
        # logger.error('Error for gathering static attributes. Error: {}'.format(e))

        return message