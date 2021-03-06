import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath):
    """
    ETL function for song files

    Args:
        cur (psycopg2.connection.cursor): psycopg2 cursor object
        filepath(str): path of a singular file
    

    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # obtain list of relevant columns
    columns = df[["song_id", "title", "artist_id", "year", "duration"]].values[0]
    song_data = list(columns) 

    # insert song record
    cur.execute(song_table_insert, song_data)
    
    # obtain list of relevant columns
    columns = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0]
    artist_data = list(columns)

    # insert artist record
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    ETL function for process files

    Args:
        cur(psycopg2.connection.cursor): psycopg2 cursor object
        filepath(str): path of a singular file
    
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # obtain column values accessing 'dt' object
    time_data = [t.dt.to_pydatetime(), t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values,
                 t.dt.month.values, t.dt.year.values,  t.dt.dayofweek.values] 
    column_labels = ["timestamp", "hour", "day", "week_of_year", "month", "year", "weekday"]
    time_dict = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(time_dict)

    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (datetime.fromtimestamp(row.ts/1000), row.userId, row.level, 
                         songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Calls ETL functions for song files and subsequently for log files

    Args:
        cur(psycopg2.connection.cursor): psycopg2 cursor object
        conn(psycopg2.connection): psycopg2 connection objects
        filepath(str): path of a singular file
        func(obj): function to call with the other params
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # First process song files
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    # Then process log files
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()