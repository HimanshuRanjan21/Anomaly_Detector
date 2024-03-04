# from sklearn.base import BaseEstimator, TransformerMixin
# import pandas as pd


# class DataPostProcessing(BaseEstimator, TransformerMixin):
#     def __init__(self, model_config):
#         self.model_config = model_config

#     def univariate_post_processing(self, df: pd.DataFrame) -> pd.DataFrame:
#         anomaly_contributors = df.columns

#         # Calculate mean anomaly score
#         mean_anomaly_score = df.mean(axis=1)
        
#         # Calculate contributions in percentage
#         total_anomaly_score = mean_anomaly_score * len(anomaly_contributors)  # total anomaly scores
#         contributions = df.div(total_anomaly_score, axis='index') * 100
        
#         # output = pd.DataFrame(index=contributions.index)
#         # # Iterate over each timestamp
#         # for ind in contributions.index:
#         #     row = contributions.loc[ind]
#         #     top_largest = row.sort_values(ascending=False)

#         #     for j in range(len(top_largest)):
#         #         output.loc[ind, f"healthContributor{j+1}Name"] = top_largest.index[j]
#         #         output.loc[ind, f"healthContributor{j+1}Percentage"] = top_largest.iloc[j]

#         #     output.loc[ind, "healthContribution4thOnwardsPercentage"] = sum(top_largest.iloc[3:])

#         # Calculate ranking and contribution
#         output = pd.concat([contributions.loc[self.model_config["ML_implementation"][0]["threshold"] < mean_anomaly_score].apply(lambda row: pd.Series(row.nlargest(len(contributions.columns)).index), axis=1), contributions.loc[self.model_config["ML_implementation"][0]["threshold"] < mean_anomaly_score].apply(lambda row: row.nlargest(len(contributions.columns)), axis=1)], axis=1)
#         cols = [f"healthContributor{j+1}Name" for j in range(len(contributions.columns))] + [f"healthContributor{j+1}Percentage" for j in range(len(contributions.columns))]
#         output.columns = cols
#         output["healthContribution4thOnwardsPercentage"] = output[output.columns[-(len(output.columns)//2 - 3):]].sum(axis=1)

#         # Add anomaly score in df
#         mean_anomaly_score.name = "anomalyScore"

#         output = pd.concat([output, mean_anomaly_score], axis=1, join="outer")
        
#         # Health KPI
#         output["healthScore"] = 100 - output["anomalyScore"].rolling("7D").mean()

#         # Risk threshold
#         output["threshold"] = self.model_config["ML_implementation"][0]["threshold"]

#         # Threshold
#         output["risk_threshold"] = 0
#         output['risk_threshold'].where(output['anomalyScore'] < self.model_config["ML_implementation"][0]["risk_threshold"], output['anomalyScore'], inplace=True)
        
#         # Status
#         output["status"] = "healthy"
#         output.loc[output['anomalyScore'] > self.model_config["ML_implementation"][0]["risk_threshold"], "status"] = "at risk"
#         output.loc[output['anomalyScore'] > self.model_config["ML_implementation"][0]["threshold"], "status"] = "critical"
        
#         return output

#     def fit(self, X, y=None):
#         return self
    
#     def transform(self, X: pd.DataFrame, y=None):
        
#         return X
    



# ###### ############                        NEW CODE _him_    ######################################################################
    
# from sklearn.base import BaseEstimator, TransformerMixin
# import pandas as pd


# class DataPostProcessing(BaseEstimator, TransformerMixin):
#     def __init__(self, model_config):
#         self.model_config = model_config

#     def univariate_post_processing(self, df: pd.DataFrame) -> pd.DataFrame:
#         anomaly_contributors = df.columns

#         # Calculate mean anomaly score
#         mean_anomaly_score = df.mean(axis=1)
        
#         # Calculate contributions in percentage
#         total_anomaly_score = mean_anomaly_score * len(anomaly_contributors)  # total anomaly scores
#         contributions = df.div(total_anomaly_score, axis='index') * 100
        
#         # Calculate ranking and contribution
#         output = pd.concat([contributions.loc[self.model_config["ML_implementation"][0]["threshold"] < mean_anomaly_score].apply(lambda row: pd.Series(row.nlargest(len(contributions.columns)).index), axis=1), contributions.loc[self.model_config["ML_implementation"][0]["threshold"] < mean_anomaly_score].apply(lambda row: row.nlargest(len(contributions.columns)), axis=1)], axis=1)
#         cols = [f"healthContributor{j+1}Name" for j in range(len(contributions.columns))] + [f"healthContributor{j+1}Percentage" for j in range(len(contributions.columns))]
#         output.columns = cols
#         output["healthContribution4thOnwardsPercentage"] = output[output.columns[-(len(output.columns)//2 - 3):]].sum(axis=1)

#         # Add anomaly score in df
#         mean_anomaly_score.name = "anomalyScore"

#         output = pd.concat([output, mean_anomaly_score], axis=1, join="outer")
        
#         # Health KPI
#         output["healthScore"] = 100 - output["anomalyScore"].rolling("7D").mean()

#         # Risk threshold
#         output["threshold"] = self.model_config["ML_implementation"][0]["threshold"]

#         # Threshold
#         output["risk_threshold"] = 0
#         output['risk_threshold'].where(output['anomalyScore'] < self.model_config["ML_implementation"][0]["risk_threshold"], output['anomalyScore'], inplace=True)
        
#         # Status
#         output["status"] = "healthy"
#         output.loc[output['anomalyScore'] > self.model_config["ML_implementation"][0]["risk_threshold"], "status"] = "at risk"
#         output.loc[output['anomalyScore'] > self.model_config["ML_implementation"][0]["threshold"], "status"] = "critical"
        
#         return output

#     def fit(self, X, y=None):
#         return self
    
#     def transform(self, results: dict):
#         processed_results = {}

#         for sub_model_id, result_df in results.items():
#             if sub_model_id in self.model_config["model_specific"]:
#                 model_params = self.model_config["model_specific"][sub_model_id]
#                 output_columns = model_params["output"]

#                 # Extract specified output columns
#                 result_df = result_df[output_columns]

#                 # Check if univariate post-processing is needed
#                 if "normalizedAnomalyScore" in output_columns or "percContribution" in output_columns:
#                     result_df = self.univariate_post_processing(result_df)

#                 # Perform any other specific post-processing based on model_params
#                 # You can use result_df and model_params to perform operations

#                 # Example: Adding a new column based on post_process_param_1
#                 result_df["new_column"] = model_params["post_process_param_1"]

#                 # Add the processed results to the dictionary
#                 processed_results[sub_model_id] = result_df

#         # Combine results if specified in model_config
#         if "combine_results" in self.model_config:
#             combine_config = self.model_config["combine_results"]
#             models_to_combine = combine_config["models_to_combine"]
#             combination_method = combine_config["combination_method"]

#             if set(models_to_combine).issubset(processed_results.keys()):
#                 # Extract specified models for combination
#                 combined_df = pd.concat([processed_results[model] for model in models_to_combine], axis=1)

#                 # Perform combination based on the specified method and weights
#                 if combination_method == "weighted_average":
#                     weights = combine_config["weights"]
#                     combined_df["combined_score"] = sum(
#                         processed_results[model]["normalizedAnomalyScore"] * weight for model, weight in zip(models_to_combine, weights)
#                     )

#                 # Add the combined results to the dictionary
#                 processed_results["combined"] = combined_df

#         return processed_results
    




#### jupyter######################################################################################



from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import json
import os

# directory_path = "th_config/json_files_for_th.json"

# os.makedirs(os.path.dirname(directory_path), exist_ok=True)

# tho = {}

# if os.path.exists(directory_path):
#     with open(directory_path, "r") as file:
#         tho = json.load(file)

class DataPostProcessing(BaseEstimator, TransformerMixin):
    def __init__(self, model_config):
        self.model_config = model_config

    def univariate_post_processing(self, results):

        """
        {sub_model_id:{model,implementation,anomaly_score,results::{normalizedAnomalyScore:df,'Anomaly':df}}}
        """
        info = self.model_config["PostProcessing"]

        for sub_model_id in results:
            sub_results = results[sub_model_id]

            post_processed = {}

            data = sub_results["anomaly_score"]
            model = sub_results["model"]
            implementation = sub_results["implementation"]

            # diffrent preprocessing of data for diffrent models
            if model == 'IsolationForest':
                new_data=100*(0.5-data)

                data = pd.DataFrame(new_data, index=data.index, columns=data.columns)


            if model == 'LocalOutlierFactor':
 
                new_data =100*(0.5-data)
                data = pd.DataFrame(new_data, index=data.index, columns=data.columns)

            if model == "Functional":
                data = abs(data)
            else:
                data = data

            output_list = info["model_specific"][sub_model_id]["output"]

            if "normalizedAnomalyScore" in output_list:
                try:

                    post_processed['normalizedAnomalyScore'] = data
                except Exception as e:
                    pass

            # if "percContribution" in output_list:
            #     try:
            #         mms=data.div(data.sum(axis=1),axis=0)
            #         mms=mms*100
            #         mms.columns=mms.columns+'___percContribution'

            #         post_processed['percContribution'] = mms
            #     except Exception as e:
            #         pass

            # if "ranking" in output_list:
            #     try:
            #         ranking=data.div(data.sum(axis=1),axis=0)
            #         ranking=ranking*100
            #         health_contributors = [f'healthcontributor{i}' for i in range(1, len(data.columns) + 1)]
            #         top_contributors = ranking.idxmax(axis=1)
            #         rank = pd.DataFrame(index=data.index)
            #         for i, contributor_col in enumerate(health_contributors):
            #             rank[contributor_col] = top_contributors.apply(lambda x: f'column{x.split("_")[1]}' if "_" in x else None)
            #         post_processed['ranking'] = rank
            #     except Exception as e:
            #         pass



            if "Anomaly" in output_list:
                try:
                    
                    if "Threshold" in info["model_specific"][sub_model_id]:
                        threshold = info["model_specific"][sub_model_id]["Threshold"]

                    else:
## only th. remove this
                        mean = data.mean(axis=1).mean()
                        std = data.mean(axis=1).std()
                        threshold = mean + 3 * std

                    anomaly = data.mean(axis=1) > threshold
                    # tho[sub_model_id]=threshold
                    print(f'The Threshold for {sub_model_id}is :::{threshold}')

                    def fn(x):
                        if x == True:
                            return 1
                        else:
                            return 0

                    anomaly = anomaly.apply(fn)
                    print(anomaly)
                    anomaly = pd.DataFrame(anomaly.values, index=data.index, columns=["Anomaly"])
                    post_processed["Anomaly"] = anomaly
                


                except Exception as e:
                    print('The Exception is ',e)

            sub_results['post_processed'] = post_processed

        return results

# with open(directory_path, "w") as file:
#     json.dump(tho, file, indent=4)

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return self
    
    def transformed(self, results):
        info = self.model_config["PostProcessing"]["combine_results"]
        final_result={}

        if "voting" in info["combination_method"]:
            k=0
            for sub_model_id in results:
                if sub_model_id in info["models_to_combine"]:
                    if k==0:
                        df=results[sub_model_id]["post_processed"]["Anomaly"]
                    else:
                        df=pd.concat([df,results[sub_model_id]["post_processed"]["Anomaly"]],axis=1)
                    k=k+1
                else:
                    pass
            ff=pd.DataFrame(index=df.index)
            ff['Anomaly']=df.sum(axis=1)
            def fn(x):
                if x>=(len(df.columns)-1):
                    return 1
                else:
                    return 0
            ff['Anomaly']=ff['Anomaly'].apply(fn)
            final_result["voting"]=ff



        # if "percContribution" in info["combination_method"]:
        #     for sub_model_id in results:
        #         if results[sub_model_id]['implementation']== 'univariate':
        #             cn_df=results[sub_model_id]['post_processed']['percContribution']
        #         else:
        #             pass
            
        #     if "voting" in info["combination_method"]:
        #         contri=cn_df.mul(final_result["voting"]['Anomaly'],axis=0)
        #     else:
        #         contri=cn_df
            
        #     final_result["percContribution"]=contri



        if "anomalyScore" in info["combination_method"]:
            for sub_model_id in results:
                if results[sub_model_id]['implementation']== 'univariate':

                    df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                    print(f'{df} allocated to df')

            mean_anomaly_score = df.mean(axis=1)
            score=pd.DataFrame(mean_anomaly_score,columns=["anomalyScore"],index=df.index)

        # if "ranking" in info["combination_method"]:
        #     for sub_model_id in results:
        #         if results[sub_model_id]['implementation']== 'univariate':
        #             cn_df=results[sub_model_id]['post_processed']['ranking']
        #         else:
        #             pass
            
        #     if "voting" in info["combination_method"]:
        #         contri=cn_df.mul(final_result["voting"]['anomaly'],axis=0)
        #     else:
            
        #         contri=cn_df
            
            final_result["anomalyScore"]=score



        if "healthContributor" in info["combination_method"]:
            for sub_model_id in results:
                if results[sub_model_id]['implementation']== 'univariate':

                    df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                    print(f'{df} allocated to df')
            else:
                pass
                    
            anomaly_contributors = df.columns
            mean_anomaly_score = df.mean(axis=1)
                    
            # Calculate contributions in percentage
            total_anomaly_score = mean_anomaly_score * len(anomaly_contributors)  # total anomaly scores                contributions = df.div(total_anomaly_score, axis='index') * 100
            contributions = df.div(total_anomaly_score, axis='index') * 100    

            output = pd.concat([contributions.loc[info["risk_threshold"] < mean_anomaly_score].apply(lambda row: pd.Series(row.nlargest(len(contributions.columns)).index), axis=1), contributions.loc[info["risk_threshold"] < mean_anomaly_score].apply(lambda row: row.nlargest(len(contributions.columns)), axis=1)], axis=1)
            cols = [f"healthContributor{j+1}Name" for j in range(len(contributions.columns))] + [f"healthContributor{j+1}Percentage" for j in range(len(contributions.columns))]                
            output.columns = cols
            output["healthContribution4thOnwardsPercentage"] = output[output.columns[-(len(output.columns)//2 - 3):]].sum(axis=1)

                # Take rolling average of contribution percentages
            output[[f"healthContributor{j+1}Percentage" for j in range(len(contributions.columns))]] = output[[f"healthContributor{j+1}Percentage" for j in range(len(contributions.columns))]].rolling("60T").mean()

                # Add anomaly score in df
            # mean_anomaly_score.name = "anomalyScore"

            # output = pd.concat([output, mean_anomaly_score], axis=1, join="outer")

            final_result["healthContributor"]=output

        if "healthScore" in info["combination_method"]:
            for sub_model_id in results:
                if results[sub_model_id]['implementation']== 'univariate':
                    df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                else:
                    pass

            mean_anomaly_score = df.mean(axis=1)
            healthScore=100-mean_anomaly_score.rolling("7D").mean()
            healthScore=pd.DataFrame(healthScore,columns=["healthScore"])
            final_result["healthScore"]=healthScore

        if "risk_threshold" in info["combination_method"]:
            for sub_model_id in results:
                if results[sub_model_id]['implementation']== 'univariate':
                    df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                else:
                    pass

            mean_anomaly_score = df.mean(axis=1)
            risk = pd.DataFrame(mean_anomaly_score, columns=["risk_threshold"], index=df.index)
            risk.loc[risk["risk_threshold"]<=info["risk_threshold"],'risk_threshold'] = np.nan
            final_result["risk_threshold"]=risk



        if "critical_threshold" in info["combination_method"]:

            if "voting" in info["combination_method"]:

                for sub_model_id in results:
                    if results[sub_model_id]['implementation']== 'univariate':
                        df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                    else:
                        pass

                mean_anomaly_score = df.mean(axis=1)
                risk = pd.DataFrame(mean_anomaly_score, columns=["critical_threshold"], index=df.index)
                risk.loc[final_result["voting"]['Anomaly']!=1,"critical_threshold"]= np.nan

                final_result["critical_threshold"]=risk
        



        if "status" in info["combination_method"]:
            for sub_model_id in results:
                if results[sub_model_id]['implementation']== 'univariate':
                    df=results[sub_model_id]['post_processed']['normalizedAnomalyScore']
                else:
                    pass



            if "voting" in info["combination_method"]:

                mean_anomaly_score = df.mean(axis=1)

                status = pd.DataFrame("healthy", columns=["status"], index=df.index)
                risk_threshold = info["risk_threshold"]
                status.loc[mean_anomaly_score > risk_threshold, "status"] = "at risk"
                status.loc[final_result["voting"]['Anomaly']==1,"status"]= "critical"

              
            else:
                mean_anomaly_score = df.mean(axis=1)
                status = pd.DataFrame("healthy", columns=["status"], index=df.index)

                risk_threshold = info["risk_threshold"]
                critical_threshold = info["threshold"]

                status.loc[mean_anomaly_score > risk_threshold, "status"] = "at risk"
                status.loc[mean_anomaly_score > critical_threshold, "status"] = "critical"

            final_result["status"]=status




        


        






                


        return final_result


