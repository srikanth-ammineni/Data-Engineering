import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import TimestampType
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias('name'),"artist_location","artist_latitude","artist_longitude").drop_duplicates()

    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table=df.select(col('userId').alias('user_id'),
                          col('firstName').alias('first_name'),
                          col('lastName').alias('last_name'),
                          col('gender'),col('level'))
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time/", mode="overwrite", partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table=df.join(song_df,df.song == song_df.title)\
    .select(monotonically_increasing_id().alias('songplay_id'),col("start_time"),col("userId").alias("user_id"),"level","song_id","artist_id", col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))


    # write songplays table to parquet files partitioned by year and month
    songplays_table=songplays_table.withColumn('year',year("start_time")) \
                                .withColumn('month',month("start_time"))

    # write songplays table to s3 in parquet
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    sc = spark.sparkContext


    """
    # Include this if spark submit throws an error saying "No space available in local directories"
    hdpConf = sc._jsc.hadoopConfiguration()
    user = os.getenv("USER")
    hdpConf.set("hadoop.security.credential.provider.path", "jceks://hdfs/user/{}/awskeyfile.jceks".format(user))
    hdpConf.set("fs.s3a.fast.upload", "true")
    hdpConf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    """
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend-123/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
