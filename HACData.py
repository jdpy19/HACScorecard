###
# Author: Jake Ditslear
# Company: Navion Healthcare Solutions
# Purpose: Automated processing of HAC data manually queried from NHSN. 
# Initially created: 7/19/19
###

#%%
# External Dependencies
import pandas as pd
import numpy as np
import pickle
import scipy.stats as st
import os
from os.path import join
import sys
import math
import shutil
import datetime as dt
from dateutil.relativedelta import *

class FileManagement:
    def __init__(self):
        self.path = os.getcwd()
        self.mainDirectory = join(self.path,"HACScorecardData")
        self.newDataDirectory = join(self.mainDirectory, "dataFromNHSN")
        self.processedDataDirectory = join(self.mainDirectory,"processedData")
    
        self.checkDirectory(self.mainDirectory)
        self.checkDirectory(self.newDataDirectory)
        self.checkDirectory(self.processedDataDirectory)

        self.newDataFile = "newestNHSNData" # New data being read in
        self.currentDataFile = "toDateNHSNData" # New data + previous set of data
        self.missingDataFile = "missingNHSNData" # Expected but missing data
        self.tableauDataFile = "tableauNHSNData" # toDate + Missing, run calculations

        self.targetFile = "targetData"

        self.currentMonthDirectory = self.createMonthDir()

        return super().__init__()
    
    # Directory management functions
    def checkDirectory(self,directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print("Directory already exists, did not create new folder.")

    def createMonthDir(self):
        today = dt.datetime.now()
        today = str(today.month) + "_" + str(today.year)
        monthDir = join(self.processedDataDirectory,today)
        
        self.checkDirectory(monthDir)

        return monthDir

    def moveFile(self,path,filename,dst):
        try:
            print("Trying to move {}...".format(filename))
            shutil.move(join(path,filename+".xlsx"), join(dst,filename+".xlsx"))
        except:
            print("Failure.")
        else:
            print("Success!")

    # Export functions
    def exportToExcel(self,dataframe,path,filename):
        try:
            print("Exporting {} to  Excel...".format(filename))
            dataframe.to_excel(join(path,filename + ".xlsx"))
        except:
            print("Failure.")
        else: 
            print("Success!")

    def exportToPickle(self,dataframe,path,filename):
        try:
            print("Exporting {} to Pickle...".format(filename))
            dataframe.to_pickle(join(path,filename + ".pkl"))
        except:
            print("Failure.")
        else: 
            print("Success!")

    # Import functions
    def importFromExcel(self,path,filename,sheet_name=None):
        dataframe = pd.DataFrame()

        try:
            print("Importing {} from Excel...".format(filename,path))
            if sheet_name == None:
                dataframe = pd.read_excel(join(path,filename + ".xlsx"))
            else:
                print("Sheet name not None.")
                dataframe = pd.read_excel(join(path,filename + ".xlsx"),sheet_name)
        except:
            print("Failure.")
        else: 
            print("Success!")
        
        return dataframe

    def importFromPickle(self,path,filename):
        dataframe = pd.DataFrame()
        
        try:
            print("Importing {} from Pickle...".format(filename,path))
            dataframe = pd.read_pickle(join(path,filename + ".pkl"))
        except:
            print("Failure.")
        else: 
            print("Success!")
        
        return dataframe

class DataManagement(FileManagement):
    def __init__(self,facilities,measures):
        super().__init__()

        self.facilities = facilities
        self.measures = measures

    def runDataManagement(self,df):
        self.raw_data = self.importFromExcel(self.mainDirectory,df)
        self.clean_data = self.cleanData(self.raw_data)
        self.output_data = pd.DataFrame()

    def queryCleanData(self,facility,measure,procedure):
        temp = self.clean_data.copy()
        temp["Date"] = pd.to_datetime(temp["Date"], errors="coerce")
        temp = temp.set_index(temp["Date"]).sort_index(ascending=True)
        def createMask(facility, measure, procedure):
            if procedure != "NA":
                mask = (temp["Facility"] == facility) & (temp["Measure"] == measure) & (temp["Procedure"] == procedure)
            else:
                mask = (temp["Facility"] == facility) & (temp["Measure"] == measure)

            return mask

        if facility == "All Ministries":
            temp = temp[(temp["Measure"] == measure) & (temp["Facility"] != "All Ministries")] # Want to aggregate large facilities, All Ministries raw data includes regional facilities
            temp = temp.groupby([temp.index.year,temp.index.month]).sum()
            temp.index = temp.index.set_names(["Y", "M"])
            temp.reset_index(inplace=True)
            temp["Date"] = pd.to_datetime({"year":temp.Y,"month":temp.M,"day":1}, format="%Y%m%d")
            temp["Measure"] = measure
            temp["Facility"] = "All Ministries"
            temp = temp.drop(["Y","M"],axis=1)
        else:
            temp = temp[createMask(facility,measure,procedure)]

        temp = temp.set_index(temp["Date"],drop=False)
        return temp

    def cleanData(self,df):
        df = df.copy()
        df = df.loc[df["Facility"].isin(self.facilities)]
        
        df["Numerator"] = pd.to_numeric(df["Numerator"])
        df["Numerator"] = df["Numerator"].replace(np.nan,0.0)
        
        df["Units"] = pd.to_numeric(df["Units"])
        df["Units"] = df["Units"].replace(np.nan,0.0)

        df["Denominator"] = pd.to_numeric(df["Denominator"])
        df["Denominator"] = df["Denominator"].replace(np.nan,df["Denominator"].mean())

        df = df[["Facility","Date","Numerator","Denominator","Units", "Measure","Procedure"]]
        return df

    def findDates(self,facility,measure,procedure):
        df = pd.DataFrame()
        df = self.queryCleanData(facility, measure, procedure)
        
        missing_dict = []
        if not df.empty:
            today = dt.datetime.now()
            last_month = df["Date"].max()
            n_missing = math.floor((today - last_month)/np.timedelta64(1,"M"))

            for i in range(1,n_missing):
                row = {
                    "Measure": measure,
                    "Facility": facility,
                    "Procedure": procedure,
                    "Date": last_month + relativedelta(months=+i),
                    "Numerator": 0,
                    "Denominator": df["Denominator"].mean(),
                    "Units": 0
                }
                missing_dict.append(row)
        
        return pd.DataFrame.from_dict(missing_dict)

    def findMissingData(self):
        y = pd.DataFrame()
        for facility in self.facilities:
            for measure in self.measures:
                if measure == "SSI":
                    procedures = ["NA", "COLO","HYST"]
                    for procedure in procedures:
                        x = self.findDates(facility,measure,procedure)
                        y = pd.concat([y,x],sort=True)
                        x = None
                else:
                    procedure = "NA"
                    x = self.findDates(facility,measure,procedure)
                    y = pd.concat([y,x],sort=True)
                    x = None
        self.exportToExcel(y,self.mainDirectory,self.missingDataFile)
        y = None

class CompareFiles(DataManagement):
    def __init__(self,facilities,measures):
        super().__init__(facilities,measures)
        
        self.getCompareCollate(self.currentDataFile,self.newDataFile,self.currentDataFile)
        self.runDataManagement(self.currentDataFile)
        self.findMissingData()
        self.getCompareCollate(self.currentDataFile,self.missingDataFile,self.tableauDataFile)
        self.runDataManagement(self.tableauDataFile)

    def filterHACFile(self, hacData):
        try:
            hacData = hacData[["Date","Denominator","Facility","Measure","Numerator","Procedure","Units"]]
        except:
            print("Error filtering HAC File. ")

        return hacData
    
    def compareFile(self,newDataframe,oldDataframe):
        combinedDataframe = pd.concat([newDataframe,oldDataframe],sort=True,axis=0,ignore_index=True, join="outer").drop_duplicates(subset=["Date","Facility","Measure","Procedure"]).reset_index()
        return combinedDataframe

    def getCompareCollate(self,oldFile,newFile,destination):
        newDF = self.filterHACFile(self.importFromExcel(self.mainDirectory,newFile))
        oldDF = self.filterHACFile(self.importFromExcel(self.mainDirectory,oldFile))
        outDF = pd.DataFrame()

        if (oldDF.empty) & (newDF.empty):
            print("Missing both files (Original & New).")
        elif oldDF.empty:
            outDF = newDF
            print("Original file does not exist.")
        elif newDF.empty:
            outDF = oldDF
            print("New file does not exist.")
        else:
            outDF = self.compareFile(newDF,oldDF)

        self.exportToExcel(outDF,self.mainDirectory,destination)
        self.moveFile(self.mainDirectory,newFile,self.currentMonthDirectory)

class CalculateData(CompareFiles):
    def __init__(self,popStats,facilities,measures):
        super().__init__(facilities,measures)
        self.popStats = popStats
        self.targets = self.importFromExcel(self.mainDirectory,self.targetFile,"Targets")
        self.anthemPts = self.importFromExcel(self.mainDirectory,self.targetFile,"AnthemPoints")

        self.runCalculations()

    # Calculations
    def calcAttributes(self,filteredData,period,facility,measure,procedure):
        ## Helper functions ##
        def calcSIR(df):
            df["Score"] =  df["Numerator"] / df["Denominator"]
            df["Score"] = df["Score"].replace(np.inf,0)
        
            return df

        def calcPPTD(df, period):
            # Calculate Performance Period To Date
            df["PPTD_NUM"] = 0.0
            df["PPTD_DEN"] = -1.0

            if period == "CY":
                df["PPTD_NUM"] = df.groupby(df["Date"].dt.year)["Numerator"].cumsum()
                df["PPTD_DEN"] = df.groupby(df["Date"].dt.year)["Denominator"].cumsum()
                df["CY"] = df["Date"]
            elif period == "FY":
                df["FY"] = df["Date"] + pd.DateOffset(months=-6)
                df["PPTD_NUM"] = df.groupby(df["FY"].dt.year)["Numerator"].cumsum()
                df["PPTD_DEN"] = df.groupby(df["FY"].dt.year)["Denominator"].cumsum()
                #df = df.drop(["FiscalYear"],axis=1)
            elif period == "ROLL":
                m = df.iloc[-1].Month.month # Error is occuring when Ministry == All Ministries
                df["Roll"] = df["Date"] + pd.DateOffset(months=-m)
                df["PPTD_NUM"] = df.groupby(df.Roll.dt.year)["Numerator"].cumsum()
                df["PPTD_DEN"] = df.groupby(df.Roll.dt.year)["Denominator"].cumsum()
                #df =df.drop(["Roll"],axis=1)
            else:
                print("Performance Period type not recognized.")

            df["PPTD"] = df["PPTD_NUM"] / df["PPTD_DEN"]

            return df

        def calc3MonthAVG(df):
            # Calculate 3 Month rolling average
            df["CUMSUM_NUM3"] = df["Numerator"].rolling(window=3,min_periods=1).sum()
            df["CUMSUM_DEN3"] = df["Denominator"].rolling(window=3,min_periods=1).sum()
            df["Trend_3"] = df["CUMSUM_NUM3"]/df["CUMSUM_DEN3"]
            return df

        def calcZScore(df):
            # Calculate ZScore, 
            # Cumulative ZScore,
            # Winsorized ZScore, 
            # Cumulative Winsorized ZScore, 
            # Percentile
            if df["Denominator"].last("12M").sum() >= 1:
                stats = self.popStats[measure]
                minZScore = (stats["topFive"] - stats["mean"]) / stats["std"]
                maxZScore = (stats["bottomFive"] - stats["mean"]) / stats["std"]
            
                df["cumZScore"] = (df["PPTD"] - stats["mean"]) / stats["std"]
                df["ZScore"] = (df["Score"] - stats["mean"]) / stats["std"]

                df["winCumZScore"] = df["cumZScore"].apply(lambda x: maxZScore if x > maxZScore else (minZScore if x < minZScore else x))
                df["winZScore"] = df["ZScore"].apply(lambda x: maxZScore if x > maxZScore else (minZScore if x < minZScore else x))

                df["Percentile"] = df["winCumZScore"].apply(lambda x: st.norm.cdf(x))
            else:
                df["cumZScore"] = np.nan
                df["ZScore"] = np.nan
                df["winCumZScore"] = np.nan
                df["winZScore"] = np.nan
                df["Percentile"] = np.nan

            return df

        def getTarget(facility,measure,procedure):
            target = pd.DataFrame()
            if (measure != "SSI") or procedure:
                if procedure != "NA":
                    target = self.targets[(self.targets["Facility"] == facility) & (self.targets["Measure"] == measure) & (self.targets["Procedure"] == procedure)]["VBP Target"]
                else:
                    target = self.targets[(self.targets["Facility"] == facility) & (self.targets["Measure"] == measure)]["VBP Target"]
            
            if target.empty:
                print("No target found.", facility, measure, procedure)
                return 1
            else:
               return target.tolist()[0]

        def getAnthemPoints(measure,procedure):
            points = 0

            if procedure != "NA":
                points = self.anthemPts[(self.anthemPts["Measure"] == measure) & (self.anthemPts["Procedure"] == procedure)]["Points"].tolist()[0]
            else:
                points = self.anthemPts[(self.anthemPts["Measure"] == measure)]["Points"].tolist()[0]

            return points

        def calcAchievementPoints(df,target):
            if (df["Denominator"].last("12M").sum() >= 1) & (target != -1):
                df["CumAchievementPts"] = df["PPTD"].apply(lambda x: round(9*(x-target)/(-target)+0.5))
            else:
                df["CumAchievementPts"] = np.nan
            return df
        
        def calcResidual(df,target,period):
            if df["Denominator"].last("12M").sum() >= 1:
                avgDen = df["Denominator"].mean()

                df["ProjectedDen"] = df.apply(lambda row: round((target*(row["PPTD_DEN"] + avgDen*(12-int(row[period].month)))),0),axis=1)
                df["ResidualVBP"] = df.apply(lambda row: row["ProjectedDen"] - row["PPTD_NUM"],axis=1)
            else:
                df["ResidualVBP"] = np.nan
                df["ProjectedDen"] = np.nan
            return df

        def calcAnthemPoints(df,target,points):
            df["AnthemPoints"] = df.apply(lambda row: float(points) if float(row["PPTD"]) >= target else 0.0,axis=1)

            return df

        ## Clean Data ##
        if not filteredData.empty:
            filteredData = filteredData.groupby(filteredData["Date"],as_index=False).agg(
                {
                    "Numerator":"sum",
                    "Denominator":"sum",
                    "Units":"sum",
                    "Facility":"first",
                    "Measure":"first",
                }
            )
            filteredData["Procedure"] = procedure
            filteredData = filteredData.set_index("Date",drop=False)

            ## Run Calculations ##
            filteredData = calcSIR(filteredData)
            filteredData = calcPPTD(filteredData,period)
            filteredData = calc3MonthAVG(filteredData)
            filteredData = calcZScore(filteredData)
            filteredData = calcAchievementPoints(filteredData,getTarget(facility,measure,procedure))
            filteredData = calcResidual(filteredData,getTarget(facility,measure,procedure),period)
            filteredData = calcAnthemPoints(filteredData,getTarget(facility,measure,procedure),getAnthemPoints(measure,procedure))
        return filteredData

    def queryThenCalculate(self,facility,measure,procedure,period):
        df = pd.DataFrame()
        df = self.queryCleanData(facility, measure, procedure)
        df = self.calcAttributes(df,period,facility,measure,procedure)
        return df

    def runCalculations(self):
        print("Running Calculations.")
        y = pd.DataFrame()
        for facility in self.facilities:
            for measure in self.measures:
                if measure == "SSI":
                    procedures = ["NA", "COLO","HYST"]
                    for procedure in procedures:
                        x = self.queryThenCalculate(facility, measure, procedure,"CY")
                        y = pd.concat([y,x],sort=True)
                        x = None
                else:
                    procedure = "NA"
                    x = self.queryThenCalculate(facility, measure, procedure,"CY")
                    y = pd.concat([y,x],sort=True)
                    x = None
        self.output_data = y
        self.exportToExcel(self.output_data,self.mainDirectory,self.tableauDataFile)

class ExtractNewData(DataManagement):
    def __init__(self,facilities,measures):
        super().__init__(facilities,measures)

        self.locationCodes = {
            10159: "SV Indianapolis",
            34057: "SV Indianapolis", # Women's Hospital
            16869: "SV Evansville",
            16942: "SV Kokomo",
            17843: "SV Carmel",
            17908: "SV Anderson",
            32540: "SV Fishers",
            16173: "SV Heart Center",
            16917: "Providence",
            40914: "SV Evansville",
            16552: "SV Dunn",
            21763: "SV Randolph",
            21868: "SV Salem",
            22703: "SV Clay",
            28428: "SV Mercy"
        }

        self.output_data = pd.DataFrame()

        for measure in self.measures:
            self.extractHACData(measure)

        if not self.output_data.empty:
            self.output_data = self.cleanData(self.output_data)
            self.exportToExcel(self.output_data,self.mainDirectory,self.newDataFile)
        
    # Extract Function
    def extractHACData(self, measure):
        # Helper Functions
        def createFileNames(measure):
            mName = "monthData"
            qName = "quarterData"
            
            if measure in ["CDIFF","MRSA"]:
                return [mName + measure, qName + measure]
            else:
                return [mName + measure]
        
        def filterDF(df, measure):
            if measure == "CAUTI":
                return df[pd.isnull(df["locationType"] ) & pd.isnull(df["loccdc"])]
            elif measure == "CLABSI":
                return df[pd.isnull(df["locationType"] ) & pd.isnull(df["locCDC"])]
            else:
                return df

        # Local Variables
        filenames = createFileNames(measure)
        measureAttributes = {
            "CAUTI": {
                "Numerator":"infCount",
                "Denominator":"numPred",
                "Units":"numucathdays"
            },
            "CLABSI": {
                "Numerator":"infCount",
                "Denominator":"numPred",
                "Units":"numcldays"
            },
            "CDIFF": {
                "Numerator":"CDIF_facIncHOCount",
                "Denominator":"numPred",
                "Units":"numpatdays"
            },
            "MRSA": {
                "Numerator":"MRSA_bldIncCount",
                "Denominator":"numPred",
                "Units":"numpatdays"
            },
            "SSI": {
                "Numerator":"infCountComplex30d",
                "Denominator":"numPredComplex30d",
                "Units":"procCount"
            }
        }

        attributes = measureAttributes[measure]

        # Main 
        mDF = self.importFromExcel(self.newDataDirectory,filenames[0])
        if not mDF.empty:
            mDF = filterDF(mDF,measure)

            output = pd.DataFrame()
            output["Date"] = pd.to_datetime(mDF["summaryYM"],format="%YM%m",errors="coerce")
            output["Numerator"] = mDF[attributes["Numerator"]]
            output["Units"] = mDF[attributes["Units"]]
            output["Measure"] = measure
            
            # SSI uses orgid instead of orgID
            if measure == "SSI":
                output["Procedure"] = mDF["proccode"]
                output["Facility"] = mDF["orgid"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")
            else: 
                output["Procedure"] = "NA"
                output["Facility"] = mDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")

            # IF quarterly 
            if len(filenames) > 1:
                qDF = self.importFromExcel(self.newDataDirectory,filenames[1])
                if not qDF.empty:
                    qDF = qDF[pd.notna(qDF["orgID"])]
                    output["orgID"] = mDF["orgID"]
                    output["summaryYQ"] = output.apply(lambda row: str(row["Date"].year) + "Q" + str(row["Date"].quarter),axis=1)
                    output = pd.merge(output,qDF,how="left",left_on=["orgID","summaryYQ"],right_on=["orgID","summaryYQ"])
                    output["Denominator"] = (output["Units"] / output[attributes["Units"]]) * output[attributes["Denominator"]]
                else: 
                    output["Denominator"] = np.nan
            else:
                output["Denominator"] = mDF[attributes["Denominator"]]

            output = output[["Date","Numerator","Denominator","Units","Measure","Facility","Procedure"]]
            output = output.set_index("Date",drop=False)
            self.output_data = pd.concat([self.output_data,output])
            output = None
            
            for f in filenames:
                self.moveFile(self.newDataDirectory,f,self.currentMonthDirectory)
        else: 
            print("File for {} not found.".format(measure))

def main():
    def getAttributes():
        measures = [
            "CAUTI",
            "CLABSI",
            "CDIFF",
            "MRSA",
            #"PSI_90: Composite",
            "SSI",
        ]

        facilities = [
            "All Ministries",
            "SV Anderson",
            "SV Evansville",
            "SV Carmel",
            "SV Fishers",
            "SV Heart Center",
            "SV Indianapolis",
            "SV Kokomo"
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
    
    extract = ExtractNewData(f,m)
    hac = CalculateData(ps,f,m)

    return hac

if __name__ == "__main__":
    hac= main()
#%%
