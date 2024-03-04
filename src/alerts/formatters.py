import pandas as pd
# import logging

# logger = logging.getLogger("alerts")

class AlertFormatter:
    timestamp_format_str = "yyyy-mm-dd&HH:MM:SS"
    timestamp_format = "%Y-%m-%d&%H:%M:%S"

    def __init__(self, configuration=None) -> None:
        # logger.info(f"Initializing Alert formatter :: configuration : {configuration}")
        self.configuration = configuration

        # Set default configuration if none
        if configuration is None:
            self._set_default_configuration()
    
    def _set_default_configuration(self):
        self.configuration = {
            "tags": {
                "Equipment": "MAB",
                "Type": "Anomaly",
                "message": "MAB showing unusual behaviour"
            },
            "message_to_col_mapping": {
                "max_threshold": {
                    "type": "format_custom_message",
                    "from": "DataFrame",
                    "use_cols": ["contributor1Name", "contributor2Name", "contributor3Name"],
                    "message": "{}, {} and {}",
                    "sub_string": "{} were the major reasons"
                },
                "min_threshold": {
                    "type": "direct_message",
                    "message": "breached min th"
                }
            }
        }

    def format_alert(self, alert_dict: dict, dataframe=None) -> pd.DataFrame:

        """
        Accets a df dataframe with timestamps and checks that were triggered
        """
        # logger.debug(f"Formatting alerts :: alert dict : {alert_dict} :: ")
        # Step 1: Flatten the dictionary
        message_mapping = self.configuration["message_to_col_mapping"]
        flattened_data = [(timestamp, key) for key, timestamps in alert_dict.items() for timestamp in timestamps]

        # Step 2: Create DataFrame
        df = pd.DataFrame(flattened_data, columns=['Timestamp', "cond"])

        # Step 3: Group by Timestamp and join keys
        # df = df.groupby('Timestamp')["cond"].agg(' and '.join).reset_index()
        #        for condition in set(df["cond"]):
        for condition in set(df["cond"]):
            if message_mapping[condition]["type"]=="direct_message":
                df.loc[df["cond"]==condition,'Reason']=message_mapping[condition]["message"]


            elif message_mapping[condition]["type"]=="format_custom_message":
                columns_to_use=message_mapping[condition]["use_cols"]
                message_blueprint=message_mapping[condition]["message"]
                string_format=message_mapping[condition]["sub_string"]

                for i in df[df["cond"]==condition].index:
                    timestamp=df.iloc[i]['Timestamp']
                    datas=dataframe.loc[timestamp][columns_to_use]
                    message=message_blueprint.format(*datas)
                    message=string_format.format(message)
                    df.loc[df["Timestamp"]==timestamp,'Reason']=message
                
        
        # Add other columns from tags
        for key, val in self.configuration["tags"].items():
            df[key] = val

        # Return dataframe
        return df