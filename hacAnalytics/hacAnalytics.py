#%%
## External Dependencies ##
import math
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import numpy as np

## Internal Dependencies ##
from hacData.HACData import DataManagement

## Body ##
class Analytics(DataManagement):
    def __init__(self, facilities, measures):
        super().__init__(facilities,measures)
    
        self.runDataManagement(self.tableauDataFile)
        
        for facility in self.facilities:
            for measure in self.measures:
                if measure == "SSI":
                    procedures = ["NA", "COLO","HYST"]
                    for procedure in procedures:
                        self.run_analytics(facility,measure,procedure)
                else:
                    procedure = "NA"
                    self.run_analytics(facility,measure,procedure)
        
        
    def run_analytics(self,facility,measure,procedure):
        print(facility,measure,procedure)
        data = self.queryCleanData(facility,measure,procedure)
        self.plot_timeseries(data,["Numerator","Denominator"])


        train_df,test_df = self.split_test_train(data,0.5)
        self.exponential_smoothing(train_df,test_df,"Numerator",3)
        self.exponential_smoothing(train_df,test_df,"Denominator",3)
        
    def exponential_smoothing(self,train_df,test_df,column,season_len):
        model = ExponentialSmoothing(train_df[column].to_numpy(), trend="add", seasonal="add", seasonal_periods=season_len)
        fit = model.fit()
        pred = fit.forecast(len(test_df))
        
        error = pred - test_df[column].to_numpy()
        
        metrics = {
            "MFE":np.mean(error),
            "MAE":np.mean(abs(error)),
            "MSE":np.mean(error),
            "RMSE":np.sqrt(np.mean(error))
        }

        print("""
        \n==========Exponential Smoothing: {column}=======================
        Predictions: {pred} 
        Actual: {actual} 
        Error: {error}
        """.format(column=column,pred=pred,actual=test_df[column].to_numpy(),error=error))

        for k,v in metrics.items():
            print("{}: {}".format(k,round(v,3)))
        
        results = test_df[["Date",column]].copy()
        results["Predicted"] = pred

        self.plot_timeseries(results,[column,"Predicted"])

    def split_test_train(self,data,ratio=0.5):
        n_rows = math.floor(len(data.index)*ratio)
        if ratio == 1:
            train_df = data.copy()
            test_df = data.copy()
        else:
            train_df = data.iloc[0:n_rows]
            test_df = data.iloc[n_rows:]
        
        return train_df,test_df

    def plot_timeseries(self, data,columns):
        data.plot(figsize=(12,3),y=columns,x="Date")


## Main ##
def main():
    def getAttributes():
        measures = [
            "CAUTI",
            # "CLABSI",
            # "CDIFF",
            # "MRSA",
            # "PSI_90: Composite",
            # "SSI",
        ]

        facilities = [
            # "All Ministries",
            # "SV Anderson",
            # "SV Evansville",
            # "SV Carmel",
            # "SV Fishers",
            # "SV Heart Center",
            "SV Indianapolis",
            #"SV Kokomo"
        ]

        populationStats = {
            "PSI_90: Composite":{
                "mean":0.999,
                "std":0.1151,
                "topFive":0.8021,
                "bottomFive":1.2668
            },
            "CLABSI":{
                "mean":0.8934,
                "std":0.5913,
                "topFive":0.0,
                "bottomFive":2.191
            },
            "CAUTI":{
                "mean":0.9131,
                "std":0.5986,
                "topFive":0.0,
                "bottomFive":2.2
            },
            "MRSA":{
                "mean":0.938,
                "std":0.6447,
                "topFive":0.0,
                "bottomFive":2.3715
            },
            "CDIFF":{
                "mean":0.8955,
                "std":0.3915,
                "topFive":0.128,
                "bottomFive":1.663
            },
            "SSI":{
                "mean":0.8435,
                "std":0.5827,
                "topFive":0.0,
                "bottomFive":2.082
            }
        }
        return measures,facilities,populationStats

    m,f,ps = getAttributes()
    
    return Analytics(f,m)


if __name__ == "__main__":
    output = main()

#%%
