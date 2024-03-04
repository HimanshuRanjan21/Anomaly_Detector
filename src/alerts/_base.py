from datetime import datetime, timedelta
import logging
import pandas as pd
from .formatters import AlertFormatter
# from .systems import tsObjectAlertSystem
from .handlers import AlertHandler
import json
import configparser
import os

# logger = logging.getLogger("alerts")

# # Create a ConfigParser instance
# config = configparser.ConfigParser()
# config.read(os.path.join('config', 'config.ini'))  # Read the config file
# alerts_status = config["alerts_status"]['status']  # Get the alert status


class AlertSystem:
    """
    Base class for handling alerts
    """
    def __init__(
            self,
            entity,
            metadata: dict,
            alert_handlers,
            alert_active: bool=True
    ) -> None:
        # logger.info(f"Initializing alert system :: entity : {entity} :: metadata : {metadata} :: alert_active : {alert_active}")
        self.entity = entity
        self.metadata = metadata
        if isinstance(self.metadata, str):
            self.metadata = json.loads(self.metadata)
        self.alert_handlers = alert_handlers
        self.alert_active = alert_active
    
    @classmethod
    def init_alert_system(cls, entity, metadata, alert_active: bool=None):
        """
        Create and initialize alerting system based on the type of object. The following types of objects are permissible
        1. SensorTag
        2. KPI
        3. LabAttribute
        4. UnitOps
        5. Flowsheet
        """
        # logger.debug(f"Initializing alert system :: entity : {entity} :: metadata : {metadata} :: creating handler and formatter config")
        # Get handler and formatter config
        handler_config = {}
        formatter_config = {}
        if "handler_config" in metadata.keys():
            handler_config = metadata["handler_config"]
        if "formatter_config" in metadata.keys():
            formatter_config = metadata["formatter_config"]
        # logger.debug(f"Initializing alert system :: handler config : {handler_config} :: formatter config : {formatter_config}")

        _available_obj_for_alerts = {
            "SensorTag" : tsObjectAlertSystem,
            "KPI" : tsObjectAlertSystem,
            "UnitOps" : None,
            "Flowsheet" : None
        }

        cls_string = entity.__class__.__name__

        # Create alert formatter
        frmttr = AlertFormatter(configuration=formatter_config)
        
        # Create alert handler
        alrt_hndlr = AlertHandler.init_handler(alert_formatter=None,handler_config=handler_config, handler_type="PythonAlertHandler")
        alrt_hndlr.add_formatter(frmttr)  # Add alert formatter

        # Check for alerts status
        if alert_active is None:
            if alerts_status=="active":
                alert_active = True
            else:
                alert_active = False
                
        return _available_obj_for_alerts[cls_string](entity=entity, metadata=metadata, alert_handlers=[alrt_hndlr], alert_active=alert_active)
        
    def run_check_for_alerts(self, *args, **kwargs):
        if not self.alert_active:
            # logger.debug(f"Alert system is not active :: Terminating process")
            return None
        
class tsObjectAlertSystem(AlertSystem):
    """
    Class for handling alerts for time series related objects
    Currently has the functionality for:
    1. SensorTag
    2. LabAttribute
    3. KPI
    """
    _available_checks = ["min_threshold", "max_threshold"]
    
    def __init__(
            self,
            entity,
            metadata: dict,
            alert_handlers,
            alert_active: bool=True,
            filter_alerts: bool=True,
            filter_time_period: timedelta=timedelta(hours=1)
    ) -> None:
        super().__init__(
            entity,
            metadata,
            alert_handlers,
            alert_active
        )
        self.filter_alerts = filter_alerts
        self.filter_time_period = filter_time_period

        self._create_checks_to_val_mapping()

        self.last_alert_raised = {cond: datetime(1970, 1, 1) for cond in self._available_checks}
        self.last_checked = {cond: datetime(1970, 1, 1) for cond in self._available_checks}

    def _create_checks_to_val_mapping(self):
        """
        Create checks and alert message mapping
        """
        self.checks_val_mapping = {}
        if "min_threshold" in self.metadata["check_to_msg_mapping"].keys():
            self.checks_val_mapping["min_threshold"] = self.metadata["min_threshold"]
        if "max_threshold" in self.metadata["check_to_msg_mapping"].keys():
            self.checks_val_mapping["max_threshold"] = self.metadata["max_threshold"]

    def run_check_for_alerts(self, df: pd.Series or pd.DataFrame, usecol: str = None):
        try:
            alerts_dict = {}
            if isinstance(df, pd.DataFrame):
                if usecol is None:
                    raise ValueError("If DataFrame is provided, usecol must be specified.")
                series = df[usecol]
            else:
                series = df

            for cond, cond_val in self.checks_val_mapping.items():
                alerts_dict[cond] = self.check_individual_conditions(ser=series, cond=cond, cond_val=cond_val)

            if self.filter_alerts:
                alerts_dict = self.filter_repeated_alerts(alert_dict=alerts_dict)

            results = {}
            for handler in self.alert_handlers:
                if isinstance(df, pd.DataFrame):
                    results[str(handler)] = handler.process_alert(alerts_dict,dataframe=df)
                else:
                    results[str(handler)] = handler.process_alert(alerts_dict)
            return results
        except Exception as e:
            raise e

    def filter_repeated_alerts(self, alert_dict: dict) -> dict:
        filtered_alerts = {}

        for cond, timestamps in alert_dict.items():
            for timestamp in timestamps:
                if timestamp >= self.last_alert_raised[cond] + self.filter_time_period:
                    self.last_alert_raised[cond] = timestamp
                    if cond not in filtered_alerts.keys():
                        filtered_alerts[cond] = []
                    filtered_alerts[cond].append(timestamp)

        return filtered_alerts

    def check_individual_conditions(self, ser: pd.Series, cond: str, cond_val: int or float):
        if cond == "min_threshold":
            return ser[ser < cond_val].index.tolist()
        elif cond == "max_threshold":
            return ser[ser > cond_val].index.tolist()
        else:
            pass
