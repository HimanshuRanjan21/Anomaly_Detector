from ._base import AlertSystem
from ._base import tsObjectAlertSystem
from .handlers import AlertHandler,PythonAlertHandler
from .formatters import AlertFormatter



class AnomalyAlertsSystem:
    def __init__(self, model_config: dict) -> None:
        self.model_config = model_config

    def run_alerts_system(self, results: dict):

        if 'anomalyScore' in results:
            anomaly_score=results['anomalyScore']
            anomaly_score['Total_anomaly_score']=anomaly_score.mean(axis=1)

            metadata=self.model_config ['Alerts']

            handlers=PythonAlertHandler(destination='sql',handler_config=metadata["handler_config"])
            formatter=AlertFormatter()
            handlers.add_formatter(formatter)

            ts_alert_system = tsObjectAlertSystem(
            entity=self.model_config["model_id"],
            metadata=metadata,
            alert_handlers=[handlers],
            alert_active=True)

            dici=ts_alert_system.run_check_for_alerts(df=anomaly_score['Total_anomaly_score'])
            alerts_df=dici['Handler 1']['alert_df'][0][['Timestamp', 'Contribution/Reason']]
            alerts_df.columns=['Timestamp', 'Action']

            alerts_df['Type']=f'{self.model_config["model_id"]}+{"Anomaly"}'

            # alerts_df['Contribution']=self.find_contribution(alerts_df)
            # alerts_df['Anomaly_score']=self.post_processing(alerts_df)

            alerts_df['Equipment']=self.model_config["model_id"]

            return(alerts_df)


        else:
            print("Anomaly Score missing in your Results Dictionary")

    def post_processing(self,alerts_df):
        timestamp=alerts_df['Timestamp']


    
    def find_contribution(self,alerts_df):
        pass

