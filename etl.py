import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear, date_format,
                                   to_timestamp, from_unixtime)
import logging
from logging import getLogger, basicConfig
logger = getLogger("etl")
logger.setLevel(logging.INFO)
log_handler = logging.FileHandler(filename="etl.log")
log_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s :%(message)s"))
logger.addHandler(log_handler)
config = configparser.ConfigParser()
config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session"""

    logger.info("creat spark session")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Create songs and artsts tables

    This function does the following procedure:
    - Load song-data dataset.
    - Create songs table and save it as parquet
    - Create artists table and save it as parquet
    """
    # get filepath to song data file
    song_data = input_data + "/song-data/A/*/*/"

    # read song data file
    logger.info("load song-data")
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    logger.info("create songs")
    songs_table = spark.sql("""
    SELECT 
        song_id
        , title
        , artist_id
        , year
        , duration
    FROM song_data
    """)

    # write songs table to parquet files partitioned by year and artist
    logger.info("save songs")
    songs_table.write.mode('overwrite').partitionBy(
        "year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    logger.info("create artists")
    artists_table = spark.sql("""
    SELECT 
        artist_id
        , artist_name as name
        , artist_location as location
        , artist_latitude as latitude
        , artist_longitude as longitude
    FROM song_data
    """)

    # write artists table to parquet files
    logger.info("save artists")
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """Create users, time and songplays tables

    This function does the following procedure:
    - Load log-data, song-data datasets
    - Create users table and save it as parquet
    - Create time table and save it as parquet
    - Create songplays table and save it as parquet
    """

    # get filepath to log data file
    log_data = input_data + "/log-data/*/*/"

    # read log data file
    logger.info("load log-data")
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where('page = "NextSong"')

    df.createOrReplaceTempView("log_data")

    # extract columns for users table
    logger.info("create users")
    users_table = spark.sql("""
    SELECT 
        userId as user_id
        , firstName as first_name
        , lastName as last_name
        , gender
        , level    
    FROM log_data
    """)

    # write users table to parquet files
    logger.info("save users")
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    @udf
    def get_timestamp_sec(ts):
        return ts//1000
    # create datetime column from original timestamp column
    df = df.withColumn("timestamp", from_unixtime(
        get_timestamp_sec(col("ts"))))

    # recreate log_data view
    df.createOrReplaceTempView("log_data")

    # extract columns to create time table
    logger.info("create time")
    time_table = spark.sql("""
    SELECT 
        ts as start_time
        , timestamp
        , cast(date_format(timestamp, "HH") as INTEGER) as hour
        , cast(date_format(timestamp, "dd") as INTEGER) as day
        , weekofyear(timestamp) as week 
        , month(timestamp) as month
        , year(timestamp) as year
        , dayofweek(timestamp) as weekday
    FROM log_data
    """)

    # write time table to parquet files partitioned by year and month
    logger.info("save time")
    time_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data + "time")

    # read in song data to use for songplays tablea
    # song_data = input_data + "/song-data/*/*/*/"
    # song_df = spark.read.json(song_data)
    # song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table
    logger.info("create songplays")
    songplays_table = spark.sql("""
    SELECT 
        ts as start_time
        , month(timestamp) as month
        , year(timestamp) as year
        , log_data.userId as user_id
        , log_data.level
        , song_data.song_id
        , song_data.artist_id
        , log_data.sessionId as session_id
        , log_data.location
        , log_data.userAgent as user_agent
    FROM log_data
    JOIN song_data ON 
        log_data.artist = song_data.artist_name
        and log_data.song = song_data.title
        and log_data.length = song_data.duration
    """)

    # write songplays table to parquet files partitioned by year and month
    logger.info("save songplays")
    songplays_table.write.mode('overwrite').partitionBy(
        "year", "month").parquet(output_data + "songplays")


def main():
    """Transform json dataset to dimension tables"""

    spark = create_spark_session()

    input_data = config['S3']['INPUT_DATA']
    output_data = config['S3']['OUTPUT_DATA']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
