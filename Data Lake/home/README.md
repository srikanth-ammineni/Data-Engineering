## Summary of the project
A fictional music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project is all about building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Datasets

Project datasets include two datasets that reside in S3. Below are the S3 links for each:

Song data: s3://udacity-dend/song_data <br/>
Log data: s3://udacity-dend/log_data 


### Fact Table
* **songplays** - records in log data associated with song plays i.e. records with page NextSong

### Dimension Table
* **users** - users in the app
* **songplays** - songs in music database
* **songplays** - artists in music database
* **songplays** - timestamps of records in songplays broken down into specific units


## Files/Folders available in the repo

* **dl.cgf** - Config file containing access key for AWS
* **etl .py** - Python script to fetch data from s3, process and load it back to s3 using spark. 

## Steps and Commands to run the pipeline from console

* Update dl.cfg with the aws access keys.
* if runnig locally <br/>
    `python etl.py` 
* if running on a EMR cluster <br/>
    `spark-submit etl.py --master yarn` 

