import pandas as pd
from .formatters import AlertFormatter
# from ..DataManager.alerts import alrts_writr
import logging

logger = logging.getLogger("alerts")

class AlertHandler:
    def __init__(self, handler_config=None,alert_formatter=None, *args, **kwargs) -> None:
        self.config = handler_config
        self.alert_formatter = alert_formatter
        if "entity" in self.config.keys():
            self.entity = self.config["entity"]
        else:
            logger.error(f"There is no entity to initialize alert handler :: Terminating initialization")
            raise Exception(f"Terminating alert handler initialization. No entity is passed in config dict")

        if self.alert_formatter is None:
            self.formatter = AlertFormatter()

    @classmethod
    def init_handler(cls, handler_config, alert_formatter, handler_type="PythonAlertHandler"):
        _available_handlers = {
            "PythonAlertHandler" : PythonAlertHandler,
            "DBalertHandler" : DBalertHandler
        }

        return _available_handlers[handler_type](handler_config=handler_config,alert_formatter=alert_formatter, destination="SQL")

    def add_formatter(self, formatter):
        self.formatter = formatter

    def process_alert(self):
        pass

class DBalertHandler(AlertHandler):
    def __init__(self, alert_formatter=None) -> None:
        self.alert_formatter = alert_formatter
        if self.alert_formatter is None:
            self.alert_formatter = AlertFormatter()

    def process_alert(self):
        pass

class PythonAlertHandler(AlertHandler):
    _available_destinations = ["sql", "influxdb"]

    def __init__(self, destination: str, handler_config=None,alert_formatter=None) -> None:
        super().__init__(handler_config=handler_config,alert_formatter=alert_formatter)

        self.destination = destination
        if self.destination.lower() not in PythonAlertHandler._available_destinations:
            raise Exception(f"Destination defined in argument is not availbale for pushing alerts!")

    def process_alert(self, alert_timestamps: dict, dataframe=None):
        """
        alert_timestamp is a dict
        {
        timestamp : list of alert conditions met
        }
        """
        logger.debug(f"Processing alert")
        logger.debug(f"Processing alerts :: entity : {self.entity} :: Formatting alerts")
        formatted_alerts = self.formatter.format_alert(alert_timestamps, dataframe)
        logger.debug(f"Processing alerts :: entity : {self.entity} :: Formatting alerts :: alerts : {formatted_alerts}")
        
        # Push formatted alerts to SQL
        dicti={}
        if len(formatted_alerts)>0:
            
            dicti["alert_df"]=formatted_alerts,
            dicti["entity"]=self.entity
            
            logger.debug(f"Processing alerts :: entity : {self.entity} :: Writing alerts to DB")
            return(dicti)
        else:
            logger.debug(f"No alert were created!")
        return(dicti)

        # Printing instead to see the formatted alert
        # logger.debug(f"Generated alerts : {formatted_alerts}")
        # print(formatted_alerts)