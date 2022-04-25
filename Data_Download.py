# IMPORTING NECESSARY PACKAGES AND LIBRARIES
import requests
import calendar
from datetime import date
import os
from zipfile import ZipFile
import pandas as pd

pc_username = os.getlogin()

# CREATING A FOLDER TO STORE THE DATA
if not os.path.exists(f"C:/Users/{pc_username}/Desktop/My Daily Data"):
        daily_data_path = os.mkdir(f"C:/Users/{pc_username}/Desktop/My Daily Data")

# GATTING DATA FROM THE PROVIED LINKS
def NSE_Deliverables():
        r = requests.get(NSE_Deliverables_url,headers=headers)
        if r.status_code == 200:
                with open(f'C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverable_Data.csv','wb') as f:
                        f.write(r.content)
                        print("NSE Deliverables data download Complete")
        else:
                print(f"NSE Deliverables data download failed (Please change your system date or Check whether the data is \navailable in the site for {today}")


def NSE_Bhavcopy():
        r = requests.get(NSE_Bhavcopy_url,headers=headers)
        if r.status_code == 200:
                with open(f'C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Bhavcopy.zip','wb') as f:
                        f.write(r.content)
                        print("NSE Bhavcopy download Complete")
        else:
                print(f"NSE Bhavcopy download failed (Please change your system date or Check whether the data is \navailable in the site for {today}")


def icharts_data():
        r = requests.get(icharts_url,headers=headers)
        if r.status_code == 200:
                with open(f'C:/Users/{pc_username}/Desktop/My Daily Data/Technical_Data.csv','wb') as f:
                        f.write(r.content)
                        print("Technical data download Complete")
        else:
                print("Something went wrong")


def BSE_Bhavcopy():
        r = requests.get(BSE_Bhavcopy_url,headers=headers)
        if r.status_code == 200:
                with open(f'C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Bhavcopy.zip','wb') as f:
                        f.write(r.content)
                        print("BSE Bhavcopy download Complete")
        else:
                print(f"BSE Bhavcopy download failed (Please change your system date or Check whether the data is \navailable in the site for {today}")


def BSE_Deliverables():
        r = requests.get(BSE_Deliverables_url,headers=headers)
        if r.status_code == 200:
                with open(f'C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Deliverables.zip','wb') as f:
                        f.write(r.content)
                        print("BSE Deliverables data download Complete")
        else:
                print(f"BSE Deliverables data download failed (Please change your system date or Check whether the data is \navailable in the site for {today}")


headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0', 'referer':'https://www1.nseindia.com/'}


# PARSING THE DATE DETAILS FOR MAKING DYNAMIC URL'S:

today = str(date.today())
split = today.split('-')
year = split[0]
year_in_2_digit = year[2:]
month = split[1]


month_in_letters = calendar.month_name[int(month)]
MON = month_in_letters.upper()[:3]
day = split[2]

print(today)
print()

# PARSING COMPLETE

# MAKING DYNAMIC URL'S:

NSE_Deliverables_url = f"https://www1.nseindia.com/archives/equities/mto/MTO_{day}{month}{year}.DAT"
NSE_Bhavcopy_url = f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/{MON}/cm{day}{MON}{year}bhav.csv.zip"
icharts_url = 'https://main.icharts.in/includes/screener/EODScan.php?export=1'
BSE_Deliverables_url = f"https://www.bseindia.com/BSEDATA/gross/{year}/SCBSEALL{day}{month}.zip"
BSE_Bhavcopy_url = f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISINCODE_{day}{month}{year_in_2_digit}.zip"

# MAKING DYNAMIC URL'S COMPLETE


# EXECUTING THE DOWNLOAD PROGRAM

print("Starting download...")
print()

NSE_Bhavcopy()
NSE_Deliverables()
BSE_Bhavcopy()
BSE_Deliverables()
icharts_data()

print()
# EXECUTION COMPLETE


# EXTRACTING AND DELETING THE ZIP FILES

def extract_all():
        print("Extracting downloaded files...")
        os.chdir("C:/Users/mdnis/Desktop/My Daily Data")

        dirlist = os.listdir()

        for file in os.walk("C:/Users/mdnis/Desktop/My Daily Data"):
                for file in dirlist:
                        if file.endswith(".zip"):
                                with ZipFile(file, 'r') as zipobj:
                                        zipobj.extractall()
                                os.remove(file)

extract_all()
print("Extraction completed successfully.")
print()

# FUNCTION TO CLEAN ALL THE UNWANTED HEADERS FROM NSE DELIVERABLES DATA AND ADD NECESSARY ONES

def clean_NSE_Deliverables():

        if os.path.exists(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverable_Data.csv"):
                print("Cleaning NSE Deliverables Data...")
                rawNSEDeliverablesfile = open(f'C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverable_Data.csv','r')
                lines = rawNSEDeliverablesfile.readlines()
                rawNSEDeliverablesfile.close()
                del lines[:4]

                new_NSE_Deliverables_Data = open(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables1.csv",'w')
                for line in lines:
                        new_NSE_Deliverables_Data.write(line)

                new_NSE_Deliverables_Data.close()

                os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverable_Data.csv")

        if os.path.exists(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables1.csv"):

                df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables1.csv",names=["Record Type","Sr No","Name of Security","Series","Volume","Delivery Volume","Deliver Percentage"])

                df = df.drop(['Record Type','Sr No'],axis = 1)

                df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables.csv",index = False)

                os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables1.csv")
                print("Cleaning Complete.")
                print()

# CLEANING NSE DELIVERABLES FUNCTION MADE SUCCESSFULLY


# FUNCTION TO CLEAN BSE DELIVERABLES

def clean_BSE_Deliverables():

                if os.path.exists(f"C:/Users/{pc_username}/Desktop/My Daily Data/SCBSEALL{day}{month}.TXT"):
                        print("Cleaning BSE Deliverables Data...")
                        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/SCBSEALL{day}{month}.TXT",delimiter='|')
                        df = df.drop(['DATE'],axis = 1)

                        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Deliverables.csv",index = False)

                        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/SCBSEALL{day}{month}.TXT")
                        print("Cleaning Complete.")
                        print()
                        print()

# RENAMING ALL THE DOWNLOADED FILES FOR SAKE OF CONVENIENCE

def rename_NSE_Bhavcopy():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/cm{day}{MON}{year}bhav.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Bhavcopy_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/cm{day}{MON}{year}bhav.csv")
        print(f"Renaming to NSE_Bhavcopy_{day}{MON}{year} Complete.")

def rename_BSE_Bhavcopy():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/EQ_ISINCODE_{day}{month}{year_in_2_digit}.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Bhavcopy_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/EQ_ISINCODE_{day}{month}{year_in_2_digit}.csv")
        print(f"Renaming to BSE_Bhavcopy_{day}{MON}{year}.csv Complete.")


def rename_NSE_Deliverables():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/NSE_Deliverables.csv")
        print(f"Renaming to NSE_Deliverables_{day}{MON}{year}.csv Complete.")


def rename_BSE_Deliverables():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Deliverables.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Deliverables_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/BSE_Deliverables.csv")
        print(f"Renaming to BSE_Deliverables_{day}{MON}{year}.csv Complete.")


def rename_Technical_Data():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/Technical_Data.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/Technical_Data_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/Technical_Data.csv")
        print(f"Renaming to Technical_Data_{day}{MON}{year}.csv Complete.")


def rename_Index_Data():
        df = pd.read_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/Index_Data.csv")
        df.to_csv(f"C:/Users/{pc_username}/Desktop/My Daily Data/Index_Data_{day}{MON}{year}.csv", index=False)
        os.remove(f"C:/Users/{pc_username}/Desktop/My Daily Data/Index_Data.csv")
        print(f"Renaming to Index_Data_{day}{MON}{year}.csv Complete.")


# CALLING ALL THE FUNCTIONS
clean_NSE_Deliverables()
clean_BSE_Deliverables()
rename_NSE_Bhavcopy()
rename_BSE_Bhavcopy()
rename_NSE_Deliverables()
rename_BSE_Deliverables()
rename_Technical_Data()
rename_Index_Data()

import Data_Import










exit_key = input("Press Enter to exit")








