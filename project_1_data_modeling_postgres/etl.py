import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import psycopg2.extras as pe


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ = 'series')

    # insert song record
    song_data = df.loc[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.loc[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines= True)

    # filter by NextSong action
    df = df[df['page']== 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df.loc[:, 'ts'], unit = 'ms').astype(str)
    t = pd.to_datetime(df.loc[:, 'ts'])
    
    # insert time data records
    time_data = (t.astype(str), t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    pe.execute_batch(cur, time_table_insert, time_df.values)

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    pe.execute_batch(cur, user_table_insert, user_df.values)

    # insert songplay records
    for index, row in df.iterrows():
        
        lst_data =[]
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, 
                                  row.artist,
                                  row.length
                                 ))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.ts, 
            row.userId, 
            row.level, 
            songid, 
            artistid, 
            row.sessionId, 
            row.location, 
            row.userAgent
        )
        lst_data.append(songplay_data)
    pe.execute_batch(cur, songplay_table_insert, lst_data)


def process_data(cur, conn, filepath, func, verbose = True):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    if verbose:
        print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        if verbose:
            print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
    print('ETL process completed!')

if __name__ == "__main__":
    main()
    