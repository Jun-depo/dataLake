import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Create a SparkSession to use Spark"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """ 
    
    This function uses Spark to create song_data dataframe from input song_data, extract
    information to create songs and artists table, and save tables in distributed parquet format.   
    
    """
    
    # get filepath to song_data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", 
                             "year","duration"]).dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 
                                                               "songs.parquet")

    # extract columns to create artists table
    artists_table = df.select([df.artist_id, 
                               df.artist_name.alias("name"), 
                               df.artist_location.alias("location"), 
                               df.artist_latitude.alias("latitude"), 
                               df.artist_longitude.alias("longitude")]) \
                               .dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    
    """ 
    
    This function uses Spark to create log_data dataframe from input log_data, extract columns 
    to create users, time and songplays tables, and save them in distributed parquet format.   
    
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.select("*").where(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select([df.userId.alias("user_id"),
                             df.firstName.alias("first_name"), 
                             df.lastName.alias("last_name"), 
                             "gender", "level"]).dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")

    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(x / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    
    # extract columns to create time table
    time_table = df.select(df.datetime.alias("start_time"),
                          hour("datetime").alias("hour"),
                          dayofmonth("datetime").alias("day"),
                          weekofyear("datetime").alias("week"),
                          month("datetime").alias("month"),
                          year("datetime").alias("year"), 
                          date_format("datetime", 'F').alias("weekday")) \
                          .dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 
                                                          "time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [df.song==song_df.title,
                            df.artist==song_df.artist_name,
                            df.length==song_df.duration])
    
    songplays_table = songplays_table.withColumn("songplay_id", 
                                   monotonically_increasing_id())
    
    songplays_table = songplays_table.select([songplays_table.songplay_id, 
                    songplays_table.datetime.alias("start_time"), 
                    songplays_table.userId.alias("user_id"), 
                    songplays_table.level, songplays_table.song_id,  
                    songplays_table.artist_id,
                    songplays_table.sessionId.alias("session_id"), 
                    songplays_table.location,songplays_table.userAgent]) \
                    .where((songplays_table.userId != "") &
                           (songplays_table.userId.isNotNull())) \
                    .dropDuplicates(["start_time"])
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.repartition(year("start_time"), 
         month("start_time")).write.partitionBy("start_time") \
        .parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jun-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

