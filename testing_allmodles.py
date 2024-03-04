import os
import json
from src.controllers import Controller
from datetime import datetime, timedelta
import pandas as pd

file_path = os.path.join("config","new_config_with_units", "mabCompressor.json")

with open(file_path, 'r') as f:
    data = json.load(f)



cnt = Controller(model_config=data)



start_time=datetime(2021, 5, 2,2,1) 

# start_time=start_time-timedelta(minutes=30)



cnt.run_prediction_pipeline(start_time=start_time, end_time=datetime(2021, 5, 2,2,2) )
# # print("Ingested data\n", df)

# df = cnt.data_cleaning.transform(df)


# # # # # print("cleaned_data\n", df)



# df = cnt.feature_engine.transform(df)

# # # # # cnt.data_processing.save_scaler(df)

# # # # # formatted_st = start_time.strftime('%Y-%m-%d %H:%M:%S%z')

# # # # # df=df.loc[df.index>formatted_st]
# # # # # print("features\n",df)
# # # # # print(df.isna().sum())



# df = cnt.data_processing.transform(df)

# # # print("Data processing: ", df)

# models_list = cnt.model_manager.transform(df)
# # # # print('__'*35)
# # # # print("Models list from model manager\n", models_list)
# # # # print('__'*35)
# # # # # print(len(models_list[0][0]))


# anomaly_scores = cnt.anomaly_detector.transform(X=models_list)

# print(anomaly_scores['05']["anomaly_score"])


# output = cnt.post_processing.univariate_post_processing(results=anomaly_scores)
# output = cnt.post_processing.transformed(results=output)

# cnt.output_manager.save_results(output)

# # print(':-'*35)
# print("the value counts for voting is",output["voting"].value_counts())

