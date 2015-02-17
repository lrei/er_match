'''
Fetches tweets from archive directories and adds them to the tweetdatabase.
'''

import os
import time
import json
import kyotocabinet as kc
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
    return [f for f in filelist if not tweetdb.get(f)]


def db_open(dbfile=TWEET_DB_FILENAME, create=False):
    '''
    Opens the databe. If create=False, throws exeption if it does not exist
    '''

    db = kc.DB()
    if create:
        if not db.open(dbfile, kc.DB.OWRITER | kc.DB.OCREATE):
            print("Failed to create")
            return None
    elif not db.open(dbfile, kc.DB.OWRITER):
        print("Failed to open")
        return None
    else:
        print('created')

    return db


def db_load_tweets(tweetdb, filepath):
    '''
    Loads a zipfile containing a line-delimited json file of tweets and adds
    them to the tweet database
    '''

    tweets = read_tweet_file(filepath)
    for tweet in tweets:
        tweetdb.add(tweet['id_str'], json.dumps(tweet))


def db_load_archives(tweetdb, archives, archive_dir=ARCHIVE_DIR):
    for archive_name in archives:
        print('Loading %s' % (archive_name,))
        # file path from name
        archive_file = os.path.join(archive_dir, archive_name)
        # load file
        db_load_tweets(tweetdb, archive_file)
        # mark file as loaded
        tweetdb.add(archive_name, 't')


def db_init(archive_dir=ARCHIVE_DIR, tweet_db_filename=TWEET_DB_FILENAME):
    '''
    Handles the creation and initial loading of tweets from the archive files
    '''
    db = db_open(tweet_db_filename, create=True)

    archives = archive_list(archive_dir)
    db_load_archives(db, archives)


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
        # Sleep
        time.sleep(ARCHIVE_LOAD_INTERVAL)


def main():

    db = db_open()

    if db is None:
        db_init()
        db = db_open()

    if db is None:
        print("Failed to open db")
        return 0

    try:
        run_service(db)
    except:
        db.close()


if __name__ == '__main__':
    main()
