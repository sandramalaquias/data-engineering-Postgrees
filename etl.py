"""
    Description: This script is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.
"""

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Description: This function is responsible for read each "json song file" from directory,
    and then split each record into songs and artists table, making the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        filepath: song data file path.
    
    Returns:
        None
    """
    # open/read song file 
    df = pd.read_json(filepath, lines=True)

    # select / transform columns according to song table column type
    song_data = df[['song_id','title','artist_id', 'year', 'duration']].values
    song_data = [x if isinstance(x, str) else x.tolist() for x in song_data]  
    song_data = [x if str(x) != 'nan' else None for x in song_data]
    
    # select rows with not null values in according to songs tables constrains and insert song record
    for row in song_data:
        if not(row[0] == None) and not(row[1] == None):
            cur.execute(song_table_insert, row)
        
    # select / transform columns according to artists table column type
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values)
    artist_data = [x if isinstance(x, str) else x.tolist() for x in artist_data]
    artist_data = [x if str(x) != 'nan' else None for x in artist_data]
    
    # select rows with not null values in according to artists tables constrains and insert artist record
    for row in artist_data:
        if not(row[0] == None)  and  not(row[1] == None):
            cur.execute(artist_table_insert, row)


def process_log_file(cur, filepath):
    """Description: This function is responsible for read each "json log file" from directory,
    and then split each record into time, users and songs_play table, making the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        filepath: log data file path.
    
    Returns:
        None
    """
    # open/read log file
    df_log = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action, select the time column, and create a dict for time
    # the timestamp is in milisconds
    df_time = pd.to_datetime(df_log[df_log['page'] =='NextSong']['ts'], unit='ms')
    df_time = df_time[:20]
    dict_time = {}
    dict_time['timestamp'] = df_time
    dict_time['hour'] = pd.DatetimeIndex(df_time).time
    dict_time['day'] = pd.DatetimeIndex(df_time).day
    dict_time['week'] = pd.DatetimeIndex(df_time).week
    dict_time['month'] = pd.DatetimeIndex(df_time).month
    dict_time['year'] = pd.DatetimeIndex(df_time).year
    dict_time['weekday'] = df_time.dt.dayofweek + 1
    dict_time['day_name'] = df_time.dt.day_name()
    time_df = pd.DataFrame.from_dict(dict_time)
    
    # insert time record - no filter is need for constrains
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # select columns to load user table
    user_df = df_log[['userId','firstName', 'lastName', 'gender', 'level']]
    
    # Select rows with numeric values in according to users table constrains and insert records
    for i, row in user_df.iterrows():
        if str(row.userId).isnumeric(): 
            cur.execute(user_table_insert, row)

    # insert songplay records
    # get songid and artistid from song and artist tables
    
    for index, row in df_log.iterrows():       
        cur.execute(song_select, (row.length, row.song, row.artist))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
    
        time = pd.to_datetime(row.ts, unit='ms')
    
    # Select rows with numeric values in according to song_play table constrains and insert songplay record    
        if str(row.userId).isnumeric()  and str(row.ts).isnumeric():
            songplay_data = (time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent,)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data or song data file path.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
        
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: This function is responsible to connect a database sparkify and 
    split the functions to corresponding filepath """
   
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    print ("Process ended")


if __name__ == "__main__":
    main()