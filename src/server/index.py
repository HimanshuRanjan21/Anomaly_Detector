from flask import Flask
from ..api.routes import routes
import os
from src.controllers import Controller
from ..utils.helpers import load_config
from src.logging.index import main


# def server():
#     # Configure logging
#     main()

#     # Read all model config
#     models_folder_path = os.path.join("config", "models")
#     files = os.listdir(models_folder_path)
#     for file in files:
#         if file.split(".")[-1]=="json":
#             model_config = load_config(os.path.join(models_folder_path, file))
#             Controller(model_config=model_config)

#     # Create flask app
#     app = Flask(__name__)

#     # Register all the routes
#     app.register_blueprint(routes)
#     return app


def server():
    # Configure logging
    main()

    # Read all model config
    models_folder_path = os.path.join("config", "new_config_with_units")
    files = os.listdir(models_folder_path)
    for file in files:
        if file.split(".")[-1]=="json":
            model_config = load_config(os.path.join(models_folder_path, file))
            print(f"{file}: The controller obj is made.")
            Controller(model_config=model_config)

    # Create flask app
    app = Flask(__name__)

    # Register all the routes
    app.register_blueprint(routes)
    return app