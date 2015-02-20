'''
Fetches tweets from archive directories and adds them to the tweetdatabase.
'''

import os
import time
import json
from ermdb import db_open, db_close, db_get, db_put, db_exists
from ermdb import db_add_map
from tweet_common import read_tweet_file
from ermcfg import ARCHIVE_DIR, TWEET_DB_FILENAME, ARCHIVE_LOAD_INTERVAL


def archive_list(archive_dir=ARCHIVE_DIR):
    '''
    List of zip files (names) in a directory
    '''
    files = os.listdir(archive_dir)
    return [f for f in files if f.endswith('.zip')]


def archive_is_new(tweetdb, filelist):
    '''
    Returns new archive filenames in a given list
    '''
    return [f for f in filelist if not db_get(tweetdb, f)]


def db_load_tweets(tweetdb, filepath):
    '''
    Loads a zipfile containing a line-delimited json file of tweets and adds
    them to the tweet database
    '''

    tweets = read_tweet_file(filepath)
    tweets = {str(tweet['id_str']): json.dumps(tweet) for tweet in tweets}
    db_add_map(tweetdb, tweets)


def db_load_archives(tweetdb, archives, archive_dir=ARCHIVE_DIR):
    archives.sort()
    for archive_name in archives:
        print('Loading %s' % (archive_name,))
        # file path from name
        archive_file = os.path.join(archive_dir, archive_name)
        # load file
        db_load_tweets(tweetdb, archive_file)
        # mark file as loaded
        db_put(tweetdb, archive_name, 't')
        # @TODO remove this (fix for my work laptop)
        time.sleep(0.2)


def db_init(archive_dir=ARCHIVE_DIR, tweet_db_filename=TWEET_DB_FILENAME):
    '''
    Handles the creation and initial loading of tweets from the archive files
    '''
    db = db_open(tweet_db_filename, create=True)

    archives = archive_list(archive_dir)
    db_load_archives(db, archives)
    db.reader_check()


def run_service(tweetdb, archive_dir=ARCHIVE_DIR):
    while True:
        # Check for new archive files
        archives = archive_list(archive_dir)
        archives = archive_is_new(tweetdb, archives)
        if len(archives) != 0:
            # Add new files
            db_load_archives(tweetdb, archives, archive_dir)
        else:
            print('No new files')

        tweetdb.reader_check()
        # Sleep
        time.sleep(ARCHIVE_LOAD_INTERVAL)


def main():

    if not db_exists(TWEET_DB_FILENAME):
        db_init()

    try:
        db = db_open(TWEET_DB_FILENAME, create=False)
    except:
        print("Failed to open db")
        return 0

    try:
        run_service(db)
    finally:
        print('Closing DBs')
        db_close(db)


if __name__ == '__main__':
    main()
