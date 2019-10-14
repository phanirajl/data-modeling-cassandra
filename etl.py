import pandas as pd
import os

from create_tables import insertfromdataframe
from create_tables import keyspace_connection
import create_tables

def process_files(folder):
    df = pd.DataFrame()
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith(".csv"):
                 df_new = pd.read_csv(os.path.join(root, file))
                 df = df.append(df_new, ignore_index=True)
    return df

def process_data(IPCluster, keyspace, folder):

    df = process_files(folder)

    # Part 1: Give me the artist, song title and song’s length in the music app history that was
    # heard during sessionId = 338, and itemInSession = 4

    # SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, SONG TEXT, LENGTH TEXT
    SESSION_SONGS = df[['sessionId', 'itemInSession', 'artist', 'song', 'length']].copy()
    insertfromdataframe(IPCluster, keyspace, "SESSION_SONGS", SESSION_SONGS)

    # Part 2: Give me only the following: name of artist, song (sorted by itemInSession) and user(first and last
    # name) for userid = 10, sessionid = 182

    # SESSION_USERS(USERID INT, SESSIONID INT, ITEMINSESSION INT, ARTIST TEXT, SONG TEXT, FIRSTNAME TEXT, LASTNAME TEXT
    SESSION_USERS = df[['userId', 'sessionId', 'itemInSession', 'artist', 'song', 'firstName', 'lastName']].copy()
    insertfromdataframe(IPCluster, keyspace, "SESSION_USERS", SESSION_USERS)

    # Part 3: Give me every user name(first and last) in my music
    # app history who listened to the song ‘All Hands Against His Own’

    # SONG_USERS(SONG TEXT, USERID INT, FIRSTNAME TEXT, LASTNAME TEXT

    SONG_USERS = df[['song', 'userId', 'firstName', 'lastName']].copy()
    insertfromdataframe(IPCluster, keyspace, "SONG_USERS", SONG_USERS)


def main():
    IPCluster, keyspace = "127.0.0.1","sparkifydb"
    create_tables.main( IPCluster, keyspace)

    process_data(IPCluster, keyspace, "./event_data/")

if __name__ == "__main__":
    main()
