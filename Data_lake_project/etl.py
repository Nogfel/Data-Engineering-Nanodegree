import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Creates a Spark Session'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Reads data from the song .json files and stores it in
    a Spark dataframe. Next, is performed some data wrangling
    in order to generate the tables: `songs` and `artists`. 
    both tables are exported to parquet format in the end.'''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
    WITH songs_duplicate AS (
        SELECT
            CAST(song_id AS STRING) AS song_id, 
            CAST(title AS STRING) AS title, 
            CAST(artist_id AS STRING) AS artist_id, 
            CAST(year AS INT) AS year, 
            CAST(duration AS FLOAT) AS duration,
            ROW_NUMBER() OVER(PARTITION BY song_id ORDER BY song_id DESC) AS rank
        FROM staging_songs)
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM songs_duplicate
    WHERE rank = 1
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = spark.sql("""
    WITH artists_duplicates AS (
    SELECT
        CAST(artist_id AS STRING) AS artist_id, 
        CAST(artist_name AS STRING) AS name, 
        CAST(artist_location AS STRING) AS location, 
        CAST(artist_latitude AS FLOAT) AS lattitude, 
        CAST(artist_longitude AS FLOAT) AS longitude,
        ROW_NUMBER() OVER(PARTITION BY artist_id ORDER BY artist_id DESC) AS rank
    FROM staging_songs
    )
    SELECT
        artist_id,
        name,
        location,
        lattitude,
        longitude
    FROM artists_duplicates
    WHERE rank = 1
    """)
    
    # write artists table to parquet files
    artists_table = artists_table.write.partitionBy('artist_id').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    '''Reads data from log .json files, stores it in a Spark
    dataframe and generates the tables: `users`, `time` and
    songplays. After each table is generated, it is exported
    to a parquet format.'''
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')\
            .select('userId', 'firstName', 'lastName', 'level', 'gender', 'song', 'artist',
                    'sessionId', 'location', 'userAgent', 'ts')
    
    df.createOrReplaceTempView("staging_events")

    # extract columns for users table    
    users_table = spark.sql("""
    WITH uniq_staging_events AS (
        SELECT 
            userid, 
            firstName,
            lastName,
            gender,
            level,
            ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events
                WHERE userid IS NOT NULL
    )
    SELECT 
        CAST(userid AS STRING) AS userid,
        CAST(firstName AS STRING) AS first_name, 
        CAST(lastName AS STRING) AS last_name, 
        CAST(gender AS STRING) AS gender,
        CAST(level AS STRING) AS level
        FROM uniq_staging_events
    WHERE rank = 1;
    """)
    
    # write users table to parquet files
    users_table = users_table.write.partitionBy('userId').parquet(os.path.join(output_data, 'user'))

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
    WITH time AS (
        SELECT DISTINCT
            ts,
            TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') AS start_time
        FROM staging_events
    )
    SELECT
        CAST(ts AS INT) AS ts,
        CAST(start_time AS TIMESTAMP) AS start_time,
        CAST(EXTRACT(HOUR FROM start_time) AS INT) AS hour,
        CAST(EXTRACT(DAY FROM start_time) AS INT) AS day,
        CAST(EXTRACT(WEEK FROM start_time) AS INT) AS week,
        CAST(EXTRACT(MONTH FROM start_time) AS INT) AS month,
        CAST(EXTRACT(YEAR FROM start_time) AS INT) AS year,
        CAST(EXTRACT(DAYOFWEEK FROM start_time) AS INT) AS weekday
    FROM time
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    # song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    WITH base AS (
    SELECT
        TIMESTAMP 'epoch' + (ev.ts/1000 * INTERVAL '1 second') AS start_time, 
        ev.userId AS user_id, 
        ev.level, 
        so.song_id, 
        so.artist_id, 
        ev.sessionId AS session_id, 
        ev.location, 
        ev.userAgent AS user_agent
    FROM staging_events ev
    LEFT JOIN staging_songs so ON so.title = ev.song
                                AND so.artist_name = ev.artist
    )
    SELECT
        CAST(start_time AS TIMESTAMP) AS start_time,
        CAST(user_id  AS STRING) AS user_id,
        CAST(song_id AS STRING) AS song_id,
        CAST(artist_id AS STRING) AS artist_id,
        CAST(session_id AS INT) AS session_id,
        CAST(EXTRACT(MONTH FROM start_time) AS INT) AS month,
        CAST(EXTRACT(YEAR FROM start_time) AS INT) AS year
    FROM base
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplay'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend-nog/"
    # input_data = "/home/felipe/Documentos/Udacity/Data Engineering Nanodegree/Data_lake_project/data/"
    output_data = "s3a://sparkyfy-lake-nog/"
    # output_data = "/home/felipe/Documentos/Udacity/Data Engineering Nanodegree/Data_lake_project/lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
