"""
Author: Jake Ditslear
Company: Navion Healthcare Solutions

Purpose: Automated processing of HAC data manually queried from NHSN. 

Initially created: 7/19/19
"""
#%%
# External Dependencies
import pandas as pd
import numpy as np
import pickle
import scipy.stats as st
import os
import shutil
import datetime as dt

#%%
class receiveHACData:
    def __init__(self,filename,popStats):
        self.filename = filename
        self.popStats = popStats
        
        self.raw_data = pd.DataFrame()
        self.clean_data = pd.DataFrame()
        self.output_data = pd.DataFrame()

        self.tryToFindData()
        self.cleanRawData()
    
    
    # Export Data
    def exportDataToExcel(self):
        self.output_data.to_excel(self.filename+"_output.xlsx")

    def retrieveExistingOutput(self):
        pass

    def replaceExistingWithNewData(self):
        pass

    # Calculations
    def calcAttributes(self,filteredData,period,measure):
        filteredData = filteredData.groupby(filteredData["TimeSORT"],as_index=True).agg(
            {
                "Numerator":"sum",
                "Denominator":"sum",
                "Units (Pt Days)":"sum",
                "Facility":"first",
                "Measure":"first",
                "HAC Target":"last", 
                "VBP Target":"last",
                "Anthem Target":"last",
                "STARP Target":"last"
            }
        )

        filteredData["Score"] =  filteredData["Numerator"] / filteredData["Denominator"]
        filteredData["Score"] = filteredData["Score"].replace(np.inf,0)

        filteredData["PPTD_NUM"] = 0.0
        filteredData["PPTD_DEN"] = -1.0

        if period == "CY":
            filteredData["PPTD_NUM"] = filteredData.groupby(filteredData.index.year)["Numerator"].cumsum()
            filteredData["PPTD_DEN"] = filteredData.groupby(filteredData.index.year)["Denominator"].cumsum()
        elif period == "FY":
            filteredData["FiscalYear"] = filteredData.index + pd.DateOffset(months=-6)
            filteredData["PPTD_NUM"] = filteredData.groupby(filteredData.FiscalYear.dt.year)["Numerator"].cumsum()
            filteredData["PPTD_DEN"] = filteredData.groupby(filteredData.FiscalYear.dt.year)["Denominator"].cumsum()

            avgDen = filteredData["Denominator"].mean()
            filteredData["ResidualVBP"] = round((filteredData["VBP Target"]*(filteredData["PPTD_DEN"] + avgDen*(12-filteredData.FiscalYear.dt.month))) - filteredData["PPTD_NUM"],0)
            filteredData["ProjectedDen"] = round((filteredData["VBP Target"]*(filteredData["PPTD_DEN"] + avgDen*(12-filteredData.FiscalYear.dt.month))),0)

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
        filteredData = filteredData.replace(np.nan,0)
        
        return filteredData

    def runCalculations(self,measures,facilities):
        y = pd.DataFrame()

        for facility in facilities:
            for measure in measures:
                if "SSI" in measure:
                    x0 = self.queryCleanData(facility, "SSI-COLO")
                    x1 = self.queryCleanData(facility, "SSI-HYST")
                    x = pd.concat([x0,x1],sort=True)
                    x["Measure"] = "SSI"
                    x = self.calcAttributes(x,"FY",measure)

                    y = pd.concat([y,x],sort=True)
                    x = None
                    x0 = None
                    x1 = None
                else: 
                    x = self.queryCleanData(facility, measure)
                    x = self.calcAttributes(x,"FY",measure)

                    y = pd.concat([y,x],sort=True)
                    x = None
            
        self.output_data = y
        y = None

    # Cleaning
    def queryCleanData(self,facility,measure):
        temp = self.clean_data.copy()

        temp["TimeSORT"] = pd.to_datetime(temp["TimeSORT"], errors="coerce")
        temp = temp.set_index(temp["TimeSORT"]).sort_index(ascending=True)
        
        if facility == "All Ministries":
            temp = temp[(temp.Measure == measure)]
            temp = temp.groupby([temp.index.year,temp.index.month]).sum()
            temp.index = temp.index.set_names(["Y", "M"])
            temp.reset_index(inplace=True)
            temp["TimeSORT"] = pd.to_datetime({"year":temp.Y,"month":temp.M,"day":1}, format="%Y%m%d")
            temp["Measure"] = measure
            temp["Facility"] = "All Ministries"
            temp = temp.set_index("TimeSORT",drop=False)
            temp = temp.drop(["Y","M"],axis=1)
        else:
            temp = temp[(temp["Facility"] == facility) & (temp["Measure"] == measure)]

        return temp

    def cleanRawData(self):
        self.clean_data = self.raw_data.copy()
        self.clean_data["Facility"] = self.clean_data["Facility"].replace(np.nan,"None")

        self.clean_data = self.clean_data[self.clean_data["Facility"] != "All Ministries"]
        self.clean_data = self.clean_data[self.clean_data["Facility"] != "None"]
        
        self.clean_data["Numerator"] = pd.to_numeric(self.clean_data["Numerator"])
        self.clean_data["Numerator"] = self.clean_data["Numerator"].replace(np.nan,0.0)

        self.clean_data["Denominator"] = pd.to_numeric(self.clean_data["Denominator"])
        self.clean_data["Denominator"] = self.clean_data["Denominator"].replace(np.nan,0.0)
        
        self.clean_data["Units (Pt Days)"] = pd.to_numeric(self.clean_data["Units (Pt Days)"])
        self.clean_data["Units (Pt Days)"] = self.clean_data["Units (Pt Days)"].replace(np.nan,0.0)

        self.clean_data["TimeSORT"] = pd.to_datetime(self.clean_data["TimeSORT"])
        
        self.clean_data = self.clean_data[["Facility","Unit","TimeSORT","Numerator","Denominator","Units (Pt Days)", "Measure","HAC Target", "VBP Target","Anthem Target","STARP Target"]]
    
    # Retrieve and Store data #
    def tryToFindData(self):
        if self.raw_data.empty:
            print("Trying to retrieve pickle.")
            self.getDataFromPickle()
        
        if self.raw_data.empty:
            print("Trying to retrieve Excel Data.")
            self.getDataFromExcel()
            self.storeDataToPickle()

    def getDataFromPickle(self):
        try: 
            self.raw_data = pd.read_pickle(self.filename + ".pkl")
            print("Retrieved data from pickle.\n")
        except: 
            print("Could not find pickle.\n")
    
    def storeDataToPickle(self):
        try:
            self.raw_data.to_pickle(self.filename + ".pkl")
            print("Stored raw data to pickle.\n")
        except:
            print("Could not store pickle.\n")

    def getDataFromExcel(self):
        try:
            self.raw_data = pd.read_excel(self.filename + ".xlsx")
            print("Retrieved data from Excel.")
        except:
            print("Could not get data from Excel.\n")

#%%
class hacFileManagement:
    def __init__(self):
        self.path = os.getcwd()
        self.mainDirectory = self.path + "\HACScorecardData"
        self.newDataDirectory = self.mainDirectory + "\dataFromNHSN"
        self.processedDataDirectory = self.mainDirectory + "\processedData"
    
        self.checkDirectory(self.mainDirectory)
        self.checkDirectory(self.newDataDirectory)
        self.checkDirectory(self.processedDataDirectory)
    
    # Functions
    def checkDirectory(self,directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            print(FileExistsError)

    def createMonthDir(self):
        today = dt.datetime.now()
        today = str(today.month) + "_" + str(today.year)
        monthDir = self.processedDataDirectory + "\\" + today
        
        self.checkDirectory(monthDir)

        return monthDir

    def moveFiles(self):
        files = os.listdir(self.newDataDirectory)
        dst = self.createMonthDir()

        for f in files:
            try:
                shutil.move(self.newDataDirectory + "\\" + f, dst)
            except FileExistsError:
                print(FileExistsError)
#%%
class extractNewHACData:
    def __init__(self,directory):
        self.directory = directory
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

        self.storeExcel()
        self.storePickle()

    # Ouput Functions
    def storeExcel(self):
        today = dt.datetime.now()
        today = str(today.month) + "_" + str(today.year)

        self.output_data.to_excel(self.directory + "\\exctractedNHSNData"+ today + ".xlsx")
    
    def storePickle(self):
        today = dt.datetime.now()
        today = str(today.month) + "_" + str(today.year)
        
        try:
            self.output_data.to_pickle(self.directory + "\\exctractedNHSNData"+ today + ".pkl")
            print("Stored raw data to pickle.\n")
        except:
            print("Could not store pickle.\n")

    # Extract Functions

    def extractCAUTI(self):
        cautiDF = pd.read_excel(self.directory + "\\"+ "monthDataCAUTI.xlsx")
        cautiDF = cautiDF[pd.isnull(cautiDF["locationType"] ) & pd.isnull(cautiDF["loccdc"])]
        
        output = pd.DataFrame()
        output["Date"] = pd.to_datetime(cautiDF["Unnamed: 0"],format="%YM%m",errors="coerce")
        output["Numerator"] = cautiDF["infCount"]
        output["Denominator"] = cautiDF["numPred"]
        output["Units"] = cautiDF["numucathdays"]
        output["Measure"] = "CAUTI"
        output["Facility"] = cautiDF["orgID"].apply(lambda x: self.locationCodes[int(x)] if pd.notna(x) else "All Ministries")
        output = output.set_index("Date")

        self.output_data = pd.concat([self.output_data,output])
        output = None

    def extractCLABSI(self):
        clabsiDF = pd.read_excel(self.directory + "\\"+ "monthDataCLABSI.xlsx")
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
        cdiffDF = pd.read_excel(self.directory + "\\"+ "monthDataCDIFF.xlsx")
        #cdiffDF = cdiffDF[pd.notna(cdiffDF["orgID"])]
        
        cdiffQuarterDF = pd.read_excel(self.directory + "\\"+ "quarterDataCDIFF.xlsx")
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
        mrsaDF = pd.read_excel(self.directory + "\\"+ "monthDataMRSA.xlsx")
        #mrsaDF = mrsaDF[pd.notna(mrsaDF["orgID"])]
        
        mrsaQuarterDF = pd.read_excel(self.directory + "\\"+ "quarterDataMRSA.xlsx")
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
        ssiDF = pd.read_excel(self.directory + "\\"+ "monthDataSSI.xlsx")
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

#%%
def main():
    filename = "hacSourceDataJune"
    measures = [
        "CAUTI",
        "CLABSI",
        "C-Diff",
        "MRSA",
        "PSI_90: Composite",
        "SSI",
    ]

    facilities = [
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
        "C-Diff":{
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

    hac = receiveHACData(filename,populationStats)
    hac.runCalculations(measures,facilities)
    hac.exportDataToExcel()
    return hac

def testFileManagement():
    test = hacFileManagement()
    #test.moveFiles()

    extract = extractNewHACData(test.newDataDirectory)

if __name__ == "__main__":
    #output = main()
    test = testFileManagement()
    
#%%