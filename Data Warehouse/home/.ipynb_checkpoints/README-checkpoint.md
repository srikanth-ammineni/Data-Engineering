## Summary of the project
A fictional startup called Sparikify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The purpose of this project is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Project Datasets

Project datasets include two datasets that reside in S3. Below are the S3 links for each:

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data


## Staging tables
* **staging_events** - Entire log_data dump from S3
* **staging_songs** - Entire song_data dump from S3 

### Fact Table
* **songplays** - records in log data associated with song plays i.e. records with page NextSong

### Dimension Table
* **users** - users in the app
* **songplays** - songs in music database
* **songplays** - artists in music database
* **songplays** - timestamps of records in songplays broken down into specific units


## Files/Folders available in the repo

* **dwh.cgf** - Config file containing all the required cluster details, IAM roles etc
* **create_tables.py** - Python script to drop and create tables
* **sql_queries.py** - Python script containing all the SQL queries required
* **etl .py** - Python script to load the whole datasets
* **launch-redshift.ipynb** - Jupyter notebook to create and launch redshift cluster using python sdk

## Steps and Commands to run the pipeline from console

1. Configure dwh.cfg file
2. Run the Jupyter notebook 'launch-redshift.ipynb' to create a redshift cluster
2. Run the python file create_tables.py to create all the required tables
    `python create_tables.py` 
3. Run the python file etl.py to load the data from s3 to staging tables and then insert to fact and dimension tables
    `python etl.py` 

