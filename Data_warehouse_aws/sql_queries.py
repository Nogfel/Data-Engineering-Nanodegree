import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession SMALLINT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId VARCHAR
)
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE songplay (
    songplay_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id VARCHAR NOT NULL DISTKEY,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY SORTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR
)
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
)
""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    lattitude DECIMAL,
    longitude DECIMAL
)
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json'
    region 'us-west-2';
""").format(ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
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
INNER JOIN staging_songs so ON so.title = ev.song
                            AND so.artist_name = ev.artist
                            AND so.duration = ev.length
WHERE ev.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
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
    userid,
    firstName AS first_name, 
    lastName AS last_name, 
    gender, 
    level    
    FROM uniq_staging_events
WHERE rank = 1;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT
    artist_id, 
    artist_name AS name, 
    artist_location AS location, 
    artist_latitude AS lattitude, 
    artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time, 
    extract(hour FROM start_time) AS hour, 
    extract(day FROM start_time) AS day, 
    extract(week FROM start_time) AS week, 
    extract(month FROM start_time) AS month, 
    extract(year FROM start_time) AS year, 
    extract(weekday FROM start_time) AS weekday
FROM songplay
""")

# ANALYSIS
qty_users_by_gender = '''
SELECT 
    gender,
    COUNT(user_id) AS count 
FROM users
GROUP BY 1;
'''

duplicate_users = '''
SELECT 
    user_id, 
    COUNT(*) as count 
FROM users 
GROUP BY user_id 
ORDER BY count DESC LIMIT 10;
'''

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytical_queries = [qty_users_by_gender, duplicate_users]