# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay"

user_table_drop = "DROP TABLE IF EXISTS users"

song_table_drop = "DROP TABLE IF EXISTS songs"

artist_table_drop = "DROP TABLE IF EXISTS artists" 

time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

song_table_create = ("""CREATE TABLE "songs" (
                    "id" SERIAL PRIMARY KEY,
                    "song_id" varchar UNIQUE NOT NULL,
                    "title" varchar NOT NULL,
                    "artist_id" varchar,
                    "year" int,
                    "duration" decimal NOT NULL);""")

artist_table_create = ("""CREATE TABLE "artists" (
                    "id" SERIAL PRIMARY KEY,
                    "artist_id" varchar UNIQUE NOT NULL,
                    "name" varchar NOT NULL,
                    "location" varchar,
                    "latitude" DOUBLE PRECISION,
                    "longitude" DOUBLE PRECISION);""")

time_table_create = ("""CREATE TABLE "time" (
                    "id" SERIAL PRIMARY KEY,
                    "start_time" timestamp UNIQUE NOT NULL,
                    "hour" time,
                    "day" int,
                    "week" int,
                    "month" int,
                    "year" int,
                    "weekday" int,
                    "dayname" varchar);""")

user_table_create = ("""CREATE TABLE "users" (
                    "id" SERIAL PRIMARY KEY,
                    "user_id" int UNIQUE NOT NULL,
                    "first_name" varchar,
                    "last_name" varchar,
                    "gender" varchar,
                    "level" varchar);""")

songplay_table_create = ("""CREATE TABLE "songsplay" (
                    "id" SERIAL PRIMARY KEY,
                    "start_time" timestamp NOT NULL,
                    "user_id" int NOT NULL,
                    "level" varchar,
                    "song_id" varchar,
                    "artist_id" varchar,
                    "session_id" varchar,
                    "location" varchar,
                    "user_agent" varchar);""")


# INSERT RECORDS

songplay_table_insert = """INSERT INTO songsplay 
                        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                        ON CONFLICT DO NOTHING;"""
                                                

song_table_insert = """INSERT INTO songs
                    (song_id, title, artist_id, year, duration) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (song_id) DO NOTHING;"""
                    
artist_table_insert = """INSERT INTO artists 
                      (artist_id, name, location, latitude, longitude) 
                      values (%s, %s, %s, %s, %s)
                      ON CONFLICT (artist_id) DO NOTHING;"""

time_table_insert = """INSERT INTO time 
                       (start_time, hour, day, week, month, year, weekday, dayname) 
                       values (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (start_time) DO NOTHING"""   

user_table_insert = """INSERT INTO users 
                      (user_id, first_name, last_name, gender, level) 
                      values (%s, %s, %s, %s, %s)
                      ON CONFLICT (user_id) DO NOTHING"""
                       


# FIND SONGS

song_select = song_select = """SELECT  songs.song_id, artists.artist_id FROM songs 
                inner join artists on songs.artist_id = artists.artist_id
                 WHERE  songs.duration = %s
                   and  songs.title like %s ESCAPE ''
                   and  artists.name like %s ESCAPE '' """

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

