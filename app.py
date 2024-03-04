import sys
from src.index import application
import logging

if __name__ == '__main__':
    try:
        application().run(host = '0.0.0.0', port = 5000)
    except Exception as e:
        logging.error(f"Error occured : {e}")
        sys.exit()