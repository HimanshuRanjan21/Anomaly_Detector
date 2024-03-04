import os

# Get the absolute path of the current file
current_file_path = os.path.abspath(__file__)
# Get the directory containing the current file
current_directory = os.path.dirname(current_file_path)

def main():
    """
    Configure logging settings from a configuration file.
    """
    try:
        # Configure logging
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        LOG_LEVEL = logging.DEBUG

        # Create and configure the root logger
        logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

        # Create a file handler to output logs to a file
        file_handler = logging.FileHandler(os.path.join("logs", "app_logs.log"))
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

        # Create a console handler to output logs to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.ERROR) # You can set a different log level for console
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

        # Add handlers to the root logger
        logging.getLogger().addHandler(file_handler)
        logging.getLogger().addHandler(console_handler)
    except Exception as e:
        print(f"Error configuring logging: {e}")
    # Log a debug message using init_entities logger
    log_debug_message(logger_name=__name__, message="This is a debug message")

def log_debug_message(logger_name, message):
    """
    Log a debug message using the specified logger.

    Parameters:
    - logger_name (str): Name of the logger to use.
    - message (str): Debug message to log.
    """
    try:
        logger = logging.getLogger(logger_name)
        logger.debug(message)
    except Exception as e:
        print(f"Error logging debug message with logger {logger_name}: {e}")