# Importing required packages
import mysql.connector

# Connecting to mysql database
db_con = mysql.connector.connect(host='localhost', user='root', password='0007')

# Creating a cursor to execute queries
cursor = db_con.cursor()

# Selecting market_data database
cursor.execute("USE market_data;")

# INSERTION START
# -----------------------------------------------------------------------------------------------------------------------

# Insert join of NSE_Bhavcopy and NSE_Deliverables to NSE_Combined Table
nse_combined_append = "INSERT INTO market_data.nse_combined_clone select a.SYMBOL, a.SERIES, a.OPEN, a.HIGH, a.LOW, a.CLOSE, a.LAST, a.PREVCLOSE, b.VOLUME, a.TOTTRDVAL/10000000 as TURNOVER, b.DELIVERY_VOLUME,b.DELIVERY_PERCENTAGE,round((a.TOTTRDVAL/a.TOTALTRADES),2) as AVG_TRADEWORTH_RS, round((b.VOLUME/a.TOTALTRADES),2) as AVG_QTYPERTRADE, round((a.TOTTRDVAL/b.VOLUME),2) as AVG_PRICE,round(((a.TOTTRDVAL/b.VOLUME)*b.DELIVERY_VOLUME),2)/10000000 as DELIVERY_TURNOVER,a.TIMESTAMP, TOTALTRADES, ISIN from tbl01_nse_bhavcopy a CROSS JOIN tbl01_nse_deliverables b on a.SYMBOL = b.SYMBOL AND (a.TIMESTAMP = b.TIMESTAMP)AND (a.SERIES = b.SERIES);"
cursor.execute(nse_combined_append)
db_con.commit()
print("Successfully Appended to NSE_Combined Table")

# Insert join of BSE_Bhavcopy and BSE_Deliverables to NSE_Combined Table
bse_combined_append = "INSERT INTO market_data.bse_combined_clone select a.SCCODE, a.SYMBOL,a.SCGROUP,a.TYPE,a.OPEN, a.HIGH, a.LOW, a.CLOSE,a.LAST, a.PREVCLOSE,a.TOTALTRADES ,b.VOLUME, a.TOTTRDVAL/10000000 as TURNOVER, b.DELIVERY_VOLUME,round((b.DELIVERY_VALUE),2)/10000000 as DELIVERY_TURNOVER,b.DELIVERY_PERCENTAGE,round((a.TOTTRDVAL/a.TOTALTRADES),2) as AVG_TRADEWORTH_RS, round((b.VOLUME/a.TOTALTRADES),2) as AVG_QTYPERTRADE, round((a.TOTTRDVAL/b.VOLUME),2) as AVG_PRICE,a.ISIN,a.TIMESTAMP from tbl01_bse_bhavcopy a CROSS JOIN tbl01_bse_deliverables b on (a.SCCODE = b.SCCODE) AND  a.TIMESTAMP = b.TIMESTAMP;"
cursor.execute(bse_combined_append)
db_con.commit()
print("Successfully Appended to NSE_Combined Table")


# Insert join of NSE_Combined and BSE_Combined to NSE_BSE_Combined Table
nse_bse_combined = "INSERT INTO market_data.nse_bse_combined_clone select b.SCCODE, a.SYMBOL, a.SERIES, a.OPEN, a.HIGH, a.LOW, a.CLOSE, a.LAST, a.PREVCLOSE, a.VOLUME + b.VOLUME as NSE_BSE_VOLUME, a.TOTALTRADES + b.TOTALTRADES as NSE_BSE_TOTALTRADES, round((a.TURNOVER + b.TURNOVER),2) as NSE_BSE_TURNOVER, round(a.DELIVERY_TURNOVER + b.DELIVERY_TURNOVER,2) as NSE_BSE_DELIVERY_TURNOVER, a.DELIVERY_VOLUME + b.DELIVERY_VOLUME as NSE_BSE_DELIVERY_VOLUME, round(((a.DELIVERY_VOLUME + b.DELIVERY_VOLUME)/(a.VOLUME + b.VOLUME))*100,2) NSE_BSE_DELIVERY_PERCENTAGE, round((((a.TURNOVER) + (b.TURNOVER))/((a.TOTALTRADES) + (b.TOTALTRADES)))*10000000,2) as NSE_BSE_AVG_TRADEWORTH, round(((a.VOLUME + b.VOLUME)/(a.TOTALTRADES + b.TOTALTRADES)),2) as NSE_BSE_AVG_QTYPERTRADE, round(((a.TURNOVER+b.TURNOVER)/(a.VOLUME+b.VOLUME))*10000000,2) as NSE_BSE_AVG_PRICE, a.TIMESTAMP, a.ISIN from nse_combined_clone a CROSS JOIN bse_combined_clone b on (a.ISIN = b.ISIN) AND (a.TIMESTAMP = b.TIMESTAMP);"
cursor.execute(nse_bse_combined)
db_con.commit()
print("Successfully Appended to NSE_BSE_Combined Table")

