# Import Libraries
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
from datetime import date
import calendar
import os
import csv
import shutil

# Pc username
pc_username = os.getlogin()

# Credentials to database connection
hostname = "localhost"
dbname = "market_data"
uname = "root"
pwd = "0007"

# Parsing the date details to work dynamically
today = str(date.today())
split = today.split('-')
# Year Details
year = split[0]
year_in_twodigits = year[2:]
# Month Details
month = split[1]
MON = calendar.month_name[int(month)].upper()[:3]
# Day Details
day = split[2]


# tech = pd.read_csv(r"C:\Users\mdnis\Desktop\Daily Data\Technical_Data.csv")

#                Cleaning Functions
# ---------------------------------------------------------

# Cleaning nse_bhav
def clean_NSE_Bhav():
    if os.path.exists(rf"C:\Users\{pc_username}\Desktop\My Daily Data\NSE_Bhavcopy_{day}{MON}{year}.csv"):
        if check_file_filled(rf"C:\Users\mdnis\Desktop\My Daily Data\NSE_Bhavcopy_{day}{MON}{year}.csv"):
            nse_bhav = pd.read_csv(rf"C:\Users\{pc_username}\Desktop\My Daily Data\NSE_Bhavcopy_{day}{MON}{year}.csv")
            nse_bhav = nse_bhav.drop('Unnamed: 13', axis=1)
            nse_bhav['TIMESTAMP'] = pd.to_datetime(nse_bhav['TIMESTAMP'])
            nse_bhav['TIMESTAMP'] = nse_bhav['TIMESTAMP'].astype('datetime64[ns]')
            nse_bhav['SERIES'] = nse_bhav.SERIES.fillna('RPL')
            return nse_bhav



# Cleaning nse_deli
def clean_NSE_Deli():
    if os.path.exists(rf"C:\Users\{pc_username}\Desktop\My Daily Data\NSE_Deliverables_{day}{MON}{year}.csv"):
        if check_file_filled(rf"C:\Users\mdnis\Desktop\My Daily Data\NSE_Deliverables_{day}{MON}{year}.csv"):
            nse_deli = pd.read_csv(rf"C:\Users\{pc_username}\Desktop\My Daily Data\NSE_Deliverables_{day}{MON}{year}.csv",
                                   names=['SYMBOL', 'SERIES', 'VOLUME', 'DELIVERY_VOLUME', 'DELIVERY_PERCENTAGE'])
            nse_deli = nse_deli.drop(0, axis=0)
            nse_deli['TIMESTAMP'] = dt.datetime.today().strftime('%Y-%m-%d')
            nse_deli['TIMESTAMP'] = nse_deli['TIMESTAMP'].astype('datetime64[ns]')
            nse_deli['SERIES'] = nse_deli.SERIES.fillna('RPL')
            return nse_deli

# Cleaning bse_bhav
def clean_BSE_Bhav():
    if os.path.exists(rf"C:\Users\{pc_username}\Desktop\My Daily Data\BSE_Bhavcopy_{day}{MON}{year}.csv"):
        if check_file_filled(rf"C:\Users\mdnis\Desktop\My Daily Data\BSE_Bhavcopy_{day}{MON}{year}.csv"):
            bse_bhav = pd.read_csv(rf"C:\Users\{pc_username}\Desktop\My Daily Data\BSE_Bhavcopy_{day}{MON}{year}.csv",
                                   names=['SCCODE', 'SYMBOL', 'SCGROUP', 'TYPE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'LAST',
                                          'PREVCLOSE', 'TOTALTRADES', 'TOTTRDQTY', 'TOTTRDVAL', 'TDCLOINDI', 'ISIN',
                                          'TIMESTAMP', 'FILLER2', 'FILLER3'], header=None)
            bse_bhav = bse_bhav.drop(0, axis=0)
            bse_bhav['TIMESTAMP'] = pd.to_datetime(bse_bhav['TIMESTAMP']).dt.date
            bse_bhav = bse_bhav.drop(['TDCLOINDI', 'FILLER2', 'FILLER3'], axis=1)
            bse_bhav['TIMESTAMP'] = bse_bhav['TIMESTAMP'].astype('datetime64[ns]')
            return bse_bhav

# Cleaning bse_deli
def clean_BSE_Deli():
    if os.path.exists(rf"C:\Users\{pc_username}\Desktop\My Daily Data\BSE_Deliverables_{day}{MON}{year}.csv"):
        if check_file_filled(rf"C:\Users\mdnis\Desktop\My Daily Data\BSE_Deliverables_{day}{MON}{year}.csv"):
            bse_deli = pd.read_csv(rf"C:\Users\{pc_username}\Desktop\My Daily Data\BSE_Deliverables_{day}{MON}{year}.csv",
                                   names=['SCCODE', 'DELIVERY_VOLUME', 'DELIVERY_VALUE', 'VOLUME', 'TOTTRDVAL',
                                          'DELIVERY_PERCENTAGE'])
            bse_deli = bse_deli.drop(0, axis=0)
            bse_deli['TIMESTAMP'] = dt.datetime.today().strftime('%Y-%m-%d')
            bse_deli['TIMESTAMP'] = bse_deli['TIMESTAMP'].astype('datetime64[ns]')
            return bse_deli

# Appending Dataframes to respective MySQL Tables
def append_dataframes():
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
    clean_NSE_Bhav().to_sql('tbl01_nse_bhavcopy', engine, index=False, if_exists='append')
    print("NSE Bhavcopy Cleaned and Appended Successfuly")
    clean_NSE_Deli().to_sql('tbl01_nse_deliverables', engine, index=False, if_exists='append')
    print("NSE Deliverables Cleaned and Appended Successfuly")
    clean_BSE_Bhav().to_sql('tbl01_bse_bhavcopy', engine, index=False, if_exists='append')
    print("BSE Bhavcopy Cleaned and Appended Successfuly")
    clean_BSE_Deli().to_sql('tbl01_bse_deliverables', engine, index=False,if_exists='append')
    print("BSE Deliverables Cleaned and Appended Successfuly")


# Function to check if a file is empty or not
def check_file_filled(path):
    with open(path) as csvfile:
        reader = csv.reader(csvfile)
        for row in csvfile:
            if len(row[0:50]) == 50:
                return True
            else:
                return False


# Creating list of file names
table_names = ['BSE_Deliverables', 'BSE_Bhavcopy', 'NSE_Deliverables', 'NSE_Bhavcopy']
# Checking conditions before running functions
flag = 0
for i in range(len(table_names)):
    if os.path.exists(rf"C:\Users\mdnis\Desktop\My Daily Data\{table_names[i]}_{day}{MON}{year}.csv"):
        if check_file_filled(rf"C:\Users\mdnis\Desktop\My Daily Data\{table_names[i]}_{day}{MON}{year}.csv"):
            flag += 1
        else:
            print(f"{table_names[i]}_{day}{MON}{year}.csv is EMPTY")

    else:
        print(f"{table_names[i]}_{day}{MON}{year}.csv File not Found")


# RUNNING PART
# ------------------------------------------------------------------------------------------------------
clean_NSE_Bhav()
clean_NSE_Deli()
clean_BSE_Bhav()
clean_BSE_Deli()

# Check value of flag is = length of table_names list and if yes, append
if flag == len(table_names):
    append_dataframes()

shutil.rmtree(f"C:/Users/{pc_username}/Desktop/My Daily Data")


import Daily_Insert_Append