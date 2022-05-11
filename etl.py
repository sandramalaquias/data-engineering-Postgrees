import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id','title','artist_id', 'year', 'duration']].values
    song_data = [x if isinstance(x, str) else x.tolist() for x in song_data]  
    song_data = [x if str(x) != 'nan' else None for x in song_data]
    
    for row in song_data:
        if not(row[0] == None) and not(row[1] == None):
            cur.execute(song_table_insert, row)
        
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values)
    artist_data = [x if isinstance(x, str) else x.tolist() for x in artist_data]
    artist_data = [x if str(x) != 'nan' else None for x in artist_data]
    for row in artist_data:
        if not(row[0] == None)  and  not(row[1] == None):
            cur.execute(artist_table_insert, row)


def process_log_file(cur, filepath):
    # open log file
    df_log = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action and create a dict for time    
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
    
    # insert time record
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df_log[['userId','firstName', 'lastName', 'gender', 'level']]
    
    # insert user records
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
    
    # insert songplay record    
        if str(row.userId).isnumeric()  and str(row.ts).isnumeric():
            songplay_data = (time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent,)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    print ("Fim de processamento")


if __name__ == "__main__":
    main()