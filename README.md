# NSE_BSE_Data_Pipeline_For_SQL

This is a small part of a freelance project i have worked on.

The clinet provided 5 links which is the download links of four files released EOD from the National Stock Exchange and Bombay Stock Exchange namely,
1. NSE Bhavcopy
2. NSE Deliverables
3. BSE Bhavcopy
4. BSE Deliverables

## The client's requirement:
* Client wanted to create a database to store and analyze Stockmarket data. 
* The data from all the 5 links should be updated automatically when a single python script is ran.
* After inserting the data to 5 tables, it should perform some joining ooperations and append the join data to some other tables.
* Important note : Some days data wont be updated and in those days automatically previousdays data will be available and somedays data will be uploaded as empty files.
  This should be overcome

## Work Flow.
##### 1. Data Download and Renaming (Data_Download.py).
* The 4 links the client provided was made dynamic by adding and updating the date daily automatically.
* Used requests to get the data from the links and written everything to a .csv file and some are downloaded as .zip.
* After that, the zipped files are extracted and the zips are deleted.
* The files are renamed for the sake of convenience.

##### 2. Data Cleaning and Importing (Data_Import).
* The data files are converted to pandas dataframes.
* The schema along with the date column in each dataframes are normalized so that, it matches the schema and structure of mySQL Table.
* There could be days when the data is not uploaded or empty files are uploaded in the website. To tackle this, before appending the data to mySQL database, first check  
  if the date in files are same as current days date and check if data is present inside the files.
* Used sql alchemy to create an engine and append the data to tables easily.
* After inserting the data, the folder containig  datafiles are deleted.

##### 3. Data Joining and Data Append (Data_Insert_Append.py)
* The Bhavcopy and Deliverables tables of NSE and BSE are joined together using SQL Queries.
* The joined data is inserted into an existing table using SQL Queries.
* Analytics and Visualizatons done (confidential).
