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
import shutil
import datetime as dt

class HacFileManagement:
    def __init__(self):
        self.path = os.getcwd()
        self.mainDirectory = join(self.path,"HACScorecardData")
        self.newDataDirectory = join(self.mainDirectory, "dataFromNHSN")
        self.processedDataDirectory = join(self.mainDirectory,"processedData")
    
        self.checkDirectory(self.mainDirectory)
        self.checkDirectory(self.newDataDirectory)
        self.checkDirectory(self.processedDataDirectory)

        self.newDatafile = "latestNHSNData"
        self.currentDataFile = "currentNHSNData"

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

    def moveFiles(self):
        files = os.listdir(self.newDataDirectory)
        dst = self.createMonthDir()

        for f in files:
            try:
                shutil.move(join(self.newDataDirectory,f), dst)
            except FileExistsError:
                print(FileExistsError)
    
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
    def importFromExcel(self,path,filename):
        dataframe = pd.DataFrame()

        try:
            print("Importing {} from Excel...".format(filename,path))
            dataframe = pd.read_excel(join(path,filename + ".xlsx"))
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

class CalculateHACData(HacFileManagement):
    def __init__(self,popStats,facilities,measures):
        super().__init__()

        self.popStats = popStats
        self.facilities = facilities
        self.measures = measures
        
        self.raw_data = pd.DataFrame()
        self.clean_data = pd.DataFrame()
        self.output_data = pd.DataFrame()

        self.tryToFindData()
        self.cleanRawData()
        self.runCalculations(self.facilities,self.measures)

    # Calculations
    def calcAttributes(self,filteredData,period,measure,procedure):
        filteredData = filteredData.groupby(filteredData["Date"],as_index=True).agg(
            {
                "Numerator":"sum",
                "Denominator":"sum",
                "Units":"sum",
                "Facility":"first",
                "Measure":"first",
            }
        )

        filteredData["Score"] =  filteredData["Numerator"] / filteredData["Denominator"]
        filteredData["Score"] = filteredData["Score"].replace(np.inf,0)
        filteredData["Procedure"] = procedure
        filteredData["PPTD_NUM"] = 0.0
        filteredData["PPTD_DEN"] = -1.0

        if period == "CY":
            filteredData["PPTD_NUM"] = filteredData.groupby(filteredData.index.year)["Numerator"].cumsum()
            filteredData["PPTD_DEN"] = filteredData.groupby(filteredData.index.year)["Denominator"].cumsum()
        elif period == "FY":
            filteredData["FiscalYear"] = filteredData.index + pd.DateOffset(months=-6)
            filteredData["PPTD_NUM"] = filteredData.groupby(filteredData.FiscalYear.dt.year)["Numerator"].cumsum()
            filteredData["PPTD_DEN"] = filteredData.groupby(filteredData.FiscalYear.dt.year)["Denominator"].cumsum()

            #avgDen = filteredData["Denominator"].mean()
            #filteredData["ResidualVBP"] = round((filteredData["VBP Target"]*(filteredData["PPTD_DEN"] + avgDen*(12-filteredData.FiscalYear.dt.month))) - filteredData["PPTD_NUM"],0)
            #filteredData["ProjectedDen"] = round((filteredData["VBP Target"]*(filteredData["PPTD_DEN"] + avgDen*(12-filteredData.FiscalYear.dt.month))),0)

            filteredData = filteredData.drop(["FiscalYear"],axis=1)
        elif period == "ROLL":
            m = filteredData.iloc[-1].Month.month # Error is occuring when Ministry == All Ministries
            filteredData["Roll"] = filteredData.index + pd.DateOffset(months=-m)
            filteredData["PPTD_NUM"] = filteredData.groupby(filteredData.Roll.dt.year)["Numerator"].cumsum()
            filteredData["PPTD_DEN"] = filteredData.groupby(filteredData.Roll.dt.year)["Denominator"].cumsum()
            filteredData =filteredData.drop(["Roll"],axis=1)
        else:
            print("Performance Period type not recognized.")

        filteredData["PPTD"] = filteredData["PPTD_NUM"] / filteredData["PPTD_DEN"]

        filteredData["CUMSUM_NUM3"] = filteredData["Numerator"].rolling(window=3,min_periods=1).sum()
        filteredData["CUMSUM_DEN3"] = filteredData["Denominator"].rolling(window=3,min_periods=1).sum()
        filteredData["Trend_3"] = filteredData["CUMSUM_NUM3"]/filteredData["CUMSUM_DEN3"]

        # Z-score calculations
        stats = self.popStats[measure]
        minZScore = (stats["topFive"] - stats["mean"]) / stats["std"]
        maxZScore = (stats["bottomFive"] - stats["mean"]) / stats["std"]
        
        filteredData["cumZScore"] = (filteredData["PPTD"] - stats["mean"]) / stats["std"]
        filteredData["ZScore"] = (filteredData["Score"] - stats["mean"]) / stats["std"]

        filteredData["winCumZScore"] = filteredData["cumZScore"].apply(lambda x: maxZScore if x > maxZScore else (minZScore if x < minZScore else x))
        filteredData["winZScore"] = filteredData["ZScore"].apply(lambda x: maxZScore if x > maxZScore else (minZScore if x < minZScore else x))

        filteredData["Percentile"] = filteredData["winCumZScore"].apply(lambda x: st.norm.cdf(x))
       
        
        #filteredData = filteredData.drop(["PPTD_NUM","PPTD_DEN","CUMSUM_NUM3","CUMSUM_DEN3"],axis=1)
        #filteredData = filteredData.replace(np.nan,0)
        filteredData["Date"] = filteredData.index

        return filteredData

    def runCalculations(self,measures,facilities):
        y = pd.DataFrame()
        x = pd.DataFrame()
        for facility in facilities:
            for measure in measures:
                if measure == "SSI":
                    procedures = [False, "COLO","HYST"]
                    for procedure in procedures:
                        x = self.queryCleanData(facility, measure, procedure)
                        x = self.calcAttributes(x,"FY",measure,procedure)

                        y = pd.concat([y,x],sort=True)
                        x = None
                else:
                    procedure = False
                    x = self.queryCleanData(facility, measure, procedure)
                    x = self.calcAttributes(x,"FY",measure,procedure)

                    y = pd.concat([y,x],sort=True)
                    x = None
            
        self.output_data = y
        y = None

        self.exportToExcel(self.output_data,self.mainDirectory,self.currentDataFile)

    # Cleaning
    def queryCleanData(self,facility,measure,procedure):
        temp = self.clean_data.copy()

        temp["Date"] = pd.to_datetime(temp["Date"], errors="coerce")
        temp = temp.set_index(temp["Date"]).sort_index(ascending=True)
        
        # if facility == "All Ministries":
        #     temp = temp[(temp.Measure == measure)]
        #     temp = temp.groupby([temp.index.year,temp.index.month]).sum()
        #     temp.index = temp.index.set_names(["Y", "M"])
        #     temp.reset_index(inplace=True)
        #     temp["Date"] = pd.to_datetime({"year":temp.Y,"month":temp.M,"day":1}, format="%Y%m%d")
        #     temp["Measure"] = measure
        #     temp["Facility"] = "All Ministries"
        #     temp = temp.set_index("Date",drop=False)
        #     temp = temp.drop(["Y","M"],axis=1)
        # else:

        def createMask(facility, measure, procedure):
            if procedure:
                
                mask = (temp["Facility"] == facility) & (temp["Measure"] == measure) & (temp["Procedure"] == procedure)
            else:
                mask = (temp["Facility"] == facility) & (temp["Measure"] == measure)

            return mask

        temp = temp[createMask(facility,measure,procedure)]

        return temp

    def cleanRawData(self):

        self.clean_data = self.raw_data.copy()
        self.clean_data["Facility"] = self.clean_data["Facility"].replace(np.nan,"None")

        self.clean_data = self.clean_data[self.clean_data["Facility"] != "None"]
        
        self.clean_data["Numerator"] = pd.to_numeric(self.clean_data["Numerator"])
        self.clean_data["Numerator"] = self.clean_data["Numerator"].replace(np.nan,0.0)

        self.clean_data["Denominator"] = pd.to_numeric(self.clean_data["Denominator"])
        self.clean_data["Denominator"] = self.clean_data["Denominator"].replace(np.nan,0.0)
        
        self.clean_data["Units"] = pd.to_numeric(self.clean_data["Units"])
        self.clean_data["Units"] = self.clean_data["Units"].replace(np.nan,0.0)

        self.clean_data["Date"] = self.clean_data["Date"]
        
        self.clean_data = self.clean_data[["Facility","Date","Numerator","Denominator","Units", "Measure","Procedure"]]

    # Retrieve and Store data #
    def tryToFindData(self):
        if self.raw_data.empty:
            print("Trying to retrieve pickle.")
            self.raw_data = self.importFromPickle(self.mainDirectory,self.currentDataFile)
        
        if self.raw_data.empty:
            print("Trying to retrieve Excel Data.")
            self.raw_data = self.importFromExcel(self.mainDirectory,self.currentDataFile)

            
class ExtractNewHACData(HacFileManagement):
    def __init__(self):
        super().__init__()

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

        self.extractCAUTI()
        self.extractCDIFF()
        self.extractCLABSI()
        self.extractMRSA()
        self.extractSSI()

        self.exportToExcel(self.output_data,self.mainDirectory,self.newDatafile)
        self.exportToPickle(self.output_data,self.mainDirectory,self.newDatafile)

    # Extract Functions
    def extractCAUTI(self):
        cautiDF = self.importFromExcel(self.newDataDirectory,"monthDataCAUTI")
        cautiDF = cautiDF[pd.isnull(cautiDF["locationType"] ) & pd.isnull(cautiDF["loccdc"])]
        
        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(cautiDF["summaryYM"],format="%YM%m",errors="coerce")
        output["Numerator"] = cautiDF["infCount"]
        output["Denominator"] = cautiDF["numPred"]
        output["Units"] = cautiDF["numucathdays"]
        output["Measure"] = "CAUTI"
        output["Facility"] = cautiDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output])
        output = None

    def extractCLABSI(self):
        clabsiDF = self.importFromExcel(self.newDataDirectory,"monthDataCLABSI")
        clabsiDF = clabsiDF[pd.isnull(clabsiDF["locationType"] ) & pd.isnull(clabsiDF["locCDC"])]
        
        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(clabsiDF["summaryYM"],format="%YM%m",errors="coerce")
        output["Numerator"] = clabsiDF["infCount"]
        output["Denominator"] = clabsiDF["numPred"]
        output["Units"] = clabsiDF["numcldays"]
        output["Measure"] = "CLABSI"
        output["Facility"] = clabsiDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output])
        output = None
    
    def extractCDIFF(self):
        cdiffDF = self.importFromExcel(self.newDataDirectory,"monthDataCDIFF")
        #cdiffDF = cdiffDF[pd.notna(cdiffDF["orgID"])]
        
        cdiffQuarterDF = self.importFromExcel(self.newDataDirectory,"quarterDataCDIFF")
        cdiffQuarterDF = cdiffQuarterDF[pd.notna(cdiffQuarterDF["orgID"])]

        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(cdiffDF["summaryYM"],format="%YM%m",errors="coerce")
        output["summaryYQ"] = output.apply(lambda row: str(row["Date"].year) + "Q" + str(row["Date"].quarter),axis=1)
        output["Numerator"] = cdiffDF["CDIF_facIncHOCount"]
        output["orgID"] = cdiffDF["orgID"]
        output["Units"] = cdiffDF["numpatdays"]
        output["Measure"] = "CDIFF"
        output["Facility"] = cdiffDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")

        output = pd.merge(output,cdiffQuarterDF,how="left",left_on=["orgID","summaryYQ"],right_on=["orgID","summaryYQ"])

        output["Denominator"] = (output["Units"] / output["numpatdays"]) * output["numPred"]
        
        output = output[["Date","Numerator","Denominator","Units","Measure","Facility"]]
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output])
        output = None

    def extractMRSA(self):
        mrsaDF = self.importFromExcel(self.newDataDirectory,"monthDataMRSA")
        #mrsaDF = mrsaDF[pd.notna(mrsaDF["orgID"])]
        
        mrsaQuarterDF = self.importFromExcel(self.newDataDirectory,"quarterDataMRSA")
        mrsaQuarterDF = mrsaQuarterDF[pd.notna(mrsaQuarterDF["orgID"])]

      
        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(mrsaDF["summaryYM"],format="%YM%m",errors="coerce")
        output["summaryYQ"] = output.apply(lambda row: str(row["Date"].year) + "Q" + str(row["Date"].quarter),axis=1)
        output["Numerator"] = mrsaDF["MRSA_bldIncCount"]
        output["orgID"] = mrsaDF["orgID"]
        output["Units"] = mrsaDF["numpatdays"]
        output["Measure"] = "MRSA"
        output["Facility"] = mrsaDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")

        output = pd.merge(output,mrsaQuarterDF,how="left",left_on=["orgID","summaryYQ"],right_on=["orgID","summaryYQ"])

        output["Denominator"] = (output["Units"] / output["numpatdays"]) * output["numPred"]
        
        output = output[["Date","Numerator","Denominator","Units","Measure","Facility"]]
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output])
        output = None
    
    def extractSSI(self):
        ssiDF = self.importFromExcel(self.newDataDirectory, "monthDataSSI")
        #ssiDF = ssiDF[pd.notna(ssiDF["orgid"])]
        
        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(ssiDF["summaryYM"],format="%YM%m",errors="coerce")
        output["Numerator"] = ssiDF["infCountComplex30d"]
        output["Denominator"] = ssiDF["numPredComplex30d"]
        output["Units"] = ssiDF["procCount"]
        output["Measure"] = "SSI"
        output["Facility"] = ssiDF["orgid"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")
        output["Procedure"] = ssiDF["proccode"]
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output],sort=True)
        output = None

class CompareFiles(HacFileManagement):
    def __init__(self):
        super().__init__()
        self.getCompareCollate()

    def filterHACFile(self, hacData):
        try:
            hacData = hacData[["Date","Denominator","Facility","Measure","Numerator","Procedure","Units"]]
        except:
            print("Error filtering HAC File. ")

        return hacData
    
    def compareFile(self,newDataframe,oldDataframe):
        combinedDataframe = pd.concat([newDataframe,oldDataframe],axis=0,ignore_index=True, join="outer").drop_duplicates(subset=["Date","Facility","Measure","Procedure"]).reset_index()
        return combinedDataframe

    def getCompareCollate(self):
        newDF = self.filterHACFile(self.importFromExcel(self.mainDirectory,self.newDatafile))
        oldDF = self.filterHACFile(self.importFromExcel(self.mainDirectory,self.currentDataFile))
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
        
        self.exportToExcel(outDF,self.mainDirectory,self.currentDataFile)
        self.exportToPickle(outDF,self.mainDirectory,self.currentDataFile)

def main():
    def getAttributes():
        measures = [
            "CAUTI",
            "CLABSI",
            "CDIFF",
            "MRSA",
            "PSI_90: Composite",
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
            "All Ministries",
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
    
    directory = HacFileManagement()
    extract = ExtractNewHACData()
    compare = CompareFiles()

    hac = CalculateHACData(ps,m,f)

    return hac

    #directory.moveFiles()

if __name__ == "__main__":
    hac= main()


#%%
