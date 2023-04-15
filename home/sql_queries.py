import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
                                    artist          VARCHAR,
                                    auth            VARCHAR,
                                    first_name      VARCHAR,
                                    gender          VARCHAR,
                                    item_in_session INT,
                                    last_name       VARCHAR,
                                    length          FLOAT,
                                    level           VARCHAR,
                                    location        VARCHAR,
                                    method          VARCHAR,
                                    page            VARCHAR,
                                    registration    FLOAT,
                                    session_id      INT,
                                    song            VARCHAR,
                                    status          INT,
                                    ts              BIGINT,
                                    user_agent      VARCHAR,
                                    user_id         VARCHAR
                                )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
                                    artist_id         VARCHAR,
                                    artist_name       VARCHAR,
                                    artist_latitude   FLOAT,
                                    artist_longitude  FLOAT,
                                    artist_location   TEXT,
                                    num_songs         INT,
                                    song_id           VARCHAR,
                                    title             VARCHAR,
                                    duration          NUMERIC NOT NULL,
                                    year              INT
                                )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
                                    songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                                    start_time  TIMESTAMP NOT NULL sortkey, 
                                    user_id     INT NOT NULL distkey,
                                    level       VARCHAR, 
                                    song_id     VARCHAR,
                                    artist_id   VARCHAR, 
                                    session_id  INT,
                                    location    VARCHAR, 
                                    user_agent  VARCHAR
                                    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
                                  user_id     INT PRIMARY KEY distkey,
                                  first_name  VARCHAR,
                                  last_name   VARCHAR,
                                  gender      VARCHAR,
                                  level       VARCHAR
                                )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
                                  song_id    VARCHAR PRIMARY KEY distkey,
                                  title      VARCHAR NOT NULL,
                                  artist_id  VARCHAR,
                                  year       INT,
                                  duration   NUMERIC NOT NULL
                                )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
                                    artist_id  VARCHAR PRIMARY KEY sortkey,
                                    name       VARCHAR NOT NULL,
                                    location   VARCHAR,
                                    latitude   FLOAT,
                                    longitude  FLOAT
                                    )diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
                                  start_time   TIMESTAMP PRIMARY KEY sortkey,
                                  hour         INT,
                                  day          INT,
                                  week         INT,
                                  month        INT,
                                  year         INT distkey,
                                  weekday      VARCHAR
                                )
""")

# STAGING TABLES

staging_events_copy = ("""
                        COPY  staging_events
                              from {}
                              iam_role {}
                              json {}
                              region 'us-west-2'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
                        COPY staging_songs
                            FROM {}
                            iam_role {}
                            json 'auto'
                            region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
            INSERT INTO
                users(user_id, first_name, last_name, gender, level)
            SELECT DISTINCT
                user_id, first_name, last_name, gender, level
            FROM
                staging_events
            WHERE
                user_id IS NOT NULL
""")

song_table_insert = ("""
            INSERT INTO
                songs(song_id, title, artist_id, year, duration)
            SELECT DISTINCT
                song_id, title, artist_id, year, duration
            FROM
                staging_songs
            WHERE
                song_id IS NOT NULL
""")

artist_table_insert = ("""
             INSERT INTO
                artists(artist_id, name, location, latitude, longitude)
             SELECT DISTINCT
                artist_id, artist_name,artist_location,artist_latitude,artist_longitude
            FROM
                staging_songs
            WHERE
                artist_id IS NOT NULL
""")

time_table_insert = ("""
            INSERT INTO
                time(start_time, hour, day, week, month, year, weekday)
            SELECT DISTINCT
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
                EXTRACT (HOUR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS hour,
                EXTRACT (DAY FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS day,
                EXTRACT (WEEK FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS week,
                EXTRACT (MONTH FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS month,
                EXTRACT (YEAR FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS year,
                EXTRACT (DOW FROM TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS weekday
            FROM
                staging_events
            WHERE
                start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
