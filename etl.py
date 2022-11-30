import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


os.environ['AWS_ACCESS_KEY_ID']= "<AWS_ACCESS_KEY_ID>" 
os.environ['AWS_SECRET_ACCESS_KEY']= "<AWS_SECRET_KEY>"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        ['song_id', 'title', 'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'),
                              partitionBy=['year', 'artist_id'], mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id').alias('artist_id'), \
                              col('artist_name').alias('name'), \
                              col('artist_location').alias('location'), \
                              col('artist_latitude').alias('latitude'), \
                              col('artist_longitude').alias('longitude'))

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(
        ['userId', 'firstName', 'lastName', 'gender', 'level'])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000).isoformat())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts), mode='overwrite')

    # extract columns to create time table
    time_table = df.select('datetime') \
            .withColumn('start_time', df.datetime) \
            .withColumn('hour', hour('datetime')) \
            .withColumn('day', dayofmonth('datetime')) \
            .withColumn('week', weekofyear('datetime')) \
            .withColumn('month', month('datetime')) \
            .withColumn('year', year('datetime')) \
            .withColumn('weekday', dayofweek('datetime'))

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time'),
                             partitionBy=['year', 'month'], mode='overwrite')

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # inner join log_data (df) with song_data (song_df) based on artisit name
    joined_df = df.join(song_df, df.artist == song_df.artist_name, 'inner')

    # add songplay_id as auto increment column to joined_df also year and month
    joined_df = joined_df.withColumn('songplay_id', monotonically_increasing_id()) \
                         .withColumn('year', year('datetime')) \
                         .withColumn('month', month('datetime'))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = joined_df.select(('songplay_id'), \
            col('timestamp').alias('start_time'), \
            col('userId').alias('user_id'), \
            col('level'), \
            col('artist_id'), \
            col('location'), \
            col('userAgent').alias('user_agent'), \
            col('year'), \
            col('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'),
                                  partitionBy=['year', 'month'], mode='overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://songsoutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
