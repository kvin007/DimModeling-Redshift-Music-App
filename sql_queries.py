import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLES

ARN = config.get("IAM_ROLE", "ARN")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events
(
    artist varchar(max),
    auth varchar(25),
    firstName varchar(100),
    gender CHAR(2),
    itemInSession int,
    lastName varchar(100),
    length float,
    level varchar(25),
    location varchar(max),
    method varchar(10),
    page varchar(30),
    registration bigint,
    sessionId bigint,
    song varchar(max),
    status int,
    ts bigint,
    userAgent varchar(max),
    userId bigint
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    artist_id varchar(18),
    artist_latitude float,
    artist_location varchar(max),
    artist_longitude float,
    artist_name varchar(max),
    duration float,
    num_songs int,
    song_id varchar(18),
    title varchar(max),
    year int
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id int identity(0,1) primary key,
    start_time timestamp not null,
    user_id bigint not null,
    level varchar(25),
    song_id varchar(18),
    artist_id varchar(18),
    session_id bigint,
    location varchar(max),
    user_agent varchar(max)
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id bigint primary key,
    first_name varchar(100),
    last_name varchar(100),
    gender char(2),
    level varchar(25)
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(18) primary key,
    title varchar(max) not null,
    artist_id varchar(18),
    year int,
    duration float not null
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(18) primary key,
    name varchar(max) not null,
    location varchar(max),
    latitude float,
    longitude float
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp primary key,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto'
;

""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
userId,
level,
s.song_id,
s.artist_id,
sessionId,
location,
userAgent
FROM staging_events e
LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT userId, firstName, lastName, gender, level
FROM (
  SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS last_user_event, userId, firstName, lastName, gender, level
  FROM staging_events
  WHERE page = 'NextSong'
)
WHERE last_user_event = 1
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
DATE_PART(hour, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
DATE_PART(day, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
DATE_PART(week, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
DATE_PART(month, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
DATE_PART(year, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
DATE_PART(weekday, TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]