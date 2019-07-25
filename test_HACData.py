#%%
import pandas as pd
import os

def compareExcelFiles(file1, file2):
    df1 = pd.read_excel(file1)
    df2 = pd.read_excel(file2)

    print(df1[df1 != df2])


def main():
    path = os.getcwd()
    f1 = os.path.join(path,"HACScorecardData\\tableauNHSNData_test.xlsx")
    f2 = os.path.join(path,"HACScorecardData\\tableauNHSNData.xlsx")
    
    compareExcelFiles(f1,f2)

if __name__ == "__main__":
    main()

#%%
