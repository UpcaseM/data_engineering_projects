# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Fact Table
# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""

CREATE TABLE songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR(10),
    song_id VARCHAR(20),
    artist_id VARCHAR(20),
    session_id VARCHAR(10),
    location VARCHAR(50),
    user_agent VARCHAR
);

""")

# Dimension Tables
# users - users in the app
# user_id, first_name, last_name, gender, level
user_table_create = ("""

CREATE TABLE users (
    user_id INT NOT NULL PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    gender VARCHAR(10),
    level VARCHAR(10)
);

""")

# songs - songs in music database
# song_id, title, artist_id, year, duration
song_table_create = ("""

CREATE TABLE songs(
    song_id VARCHAR(20) NOT NULL PRIMARY KEY,
    title VARCHAR(100),
    artist_id VARCHAR(20),
    year INTEGER,
    duration FLOAT
);

""")

# artists - artists in music database
# artist_id, name, location, latitude, longitude
artist_table_create = ("""

CREATE TABLE artists(
    artist_id VARCHAR(20) NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(50),
    latitude VARCHAR(50),
    longitude VARCHAR(50)
);

""")

# time - timestamps of records in songplays broken down into specific units
# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
    
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);

""")

# INSERT RECORDS

songplay_table_insert = ("""

INSERT INTO songplays (
                         start_time, 
                         user_id, 
                         level, 
                         song_id, 
                         artist_id, 
                         session_id,
                         location,
                         user_agent) 
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)

""")


user_table_insert = ("""

INSERT INTO users (user_id, 
                   first_name, 
                   last_name, 
                   gender, 
                   level) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
    DO UPDATE
    SET level = excluded.level
    WHERE users.level = 'free' AND excluded.level = 'paid'
""")

    
song_table_insert = ("""

INSERT INTO songs (song_id, 
                   title, 
                   artist_id, 
                   year, 
                   duration) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
""")

    
artist_table_insert = ("""

INSERT INTO artists (artist_id, 
                     name, 
                     location, 
                     latitude, 
                     longitude) 
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) 
    DO NOTHING;
""")


time_table_insert = ("""

INSERT INTO time (start_time, 
                         hour, 
                         day, 
                         week, 
                         month,
                         year,
                         weekday) 
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""

SELECT
songs.song_id,
artists.artist_id
FROM songs
    JOIN artists ON songs.artist_id = artists.artist_id
WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration =%s

""")
# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]