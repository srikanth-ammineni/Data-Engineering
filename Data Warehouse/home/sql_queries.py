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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR,
                                                                            auth VARCHAR,
                                                                            firstName VARCHAR,
                                                                            gender VARCHAR,
                                                                            itemInSession int,
                                                                            lastName VARCHAR, 
                                                                            length float, 
                                                                            level VARCHAR, 
                                                                            location VARCHAR, 
                                                                            method VARCHAR,
                                                                            page VARCHAR,
                                                                            registration VARCHAR, 
                                                                            sessionId bigint, 
                                                                            song VARCHAR, 
                                                                            status int,
                                                                            ts timestamp, 
                                                                            userAgent VARCHAR, 
                                                                            userId bigint) """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (song_id VARCHAR PRIMARY KEY, 
                                                                           artist_id VARCHAR, 
                                                                           artist_latitude float,
                                                                           artist_longitude float,
                                                                           artist_location VARCHAR,
                                                                           artist_name VARCHAR(500),
                                                                           duration float, 
                                                                           num_songs bigint, 
                                                                           title VARCHAR, 
                                                                           year int)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id bigint IDENTITY PRIMARY KEY, 
                                                                  start_time timestamp REFERENCES time(start_time) sortkey, 
                                                                  user_id bigint NOT NULL REFERENCES users(user_id), 
                                                                  level varchar,
                                                                  song_id varchar REFERENCES songs(song_id), 
                                                                  artist_id varchar REFERENCES artists(artist_id) distkey,
                                                                  session_id bigint,
                                                                  location varchar,
                                                                  user_agent varchar);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id bigint PRIMARY KEY, 
                                                          first_name varchar NOT NULL, 
                                                          last_name varchar NOT NULL, 
                                                          gender varchar, 
                                                          level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY distkey, 
                                                          title varchar NOT NULL, 
                                                          artist_id varchar NOT NULL, 
                                                          year int, 
                                                          duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY distkey, 
                                                              name VARCHAR(500) NOT NULL, 
                                                              location varchar, 
                                                              latitude float, 
                                                              longitude float);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp PRIMARY KEY sortkey, 
                                                         hour int, 
                                                         day int, 
                                                         week int, 
                                                         month int,
                                                         year int,
                                                         weekday int);""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events from 's3://udacity-dend/log_data'
                          CREDENTIALS 'aws_iam_role={}'
                          TIMEFORMAT as 'epochmillisecs'
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                          JSON 's3://udacity-dend/log_json_path.json'
                          COMPUPDATE OFF REGION 'us-west-2';""").format(config.get('IAM_ROLE', 'ARN'))


staging_songs_copy = ("""COPY staging_songs from 's3://udacity-dend/song-data'
                          CREDENTIALS 'aws_iam_role={}'
                          TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                          JSON 'auto'
                          COMPUPDATE OFF REGION 'us-west-2';""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        SELECT  DISTINCT(e.ts)  AS start_time, 
                                e.userId        AS user_id, 
                                e.level         AS level, 
                                s.song_id       AS song_id, 
                                s.artist_id     AS artist_id, 
                                e.sessionId     AS session_id, 
                                e.location      AS location, 
                                e.userAgent     AS user_agent
                        FROM staging_events e, staging_songs  s   
                        WHERE e.song = s.title AND e.artist = s.artist_name AND e.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId  AS user_id,
            firstName  AS first_name,
            lastName  AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT song_id AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert =  ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT artist_id AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT start_time               AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
