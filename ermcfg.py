'''
Config/Settings for ER Tweet Matching Service
'''

from datetime import date


## ER SERVICE
# Log ER Requests?
ER_LOG = False
# Database file that stores event registry url-eventid map (Hash)
ER_URL_DB_FILENAME = 'er.url.db.kch'
# Database file that stores date-eventid mappings (Hash)
ER_DATE_DB_FILENAME = 'er.date.db.kch'
# Database file that stores eventid-centroid (english) mappings (Hash)
ER_CENTROID_EN_DB_FILENAME = 'er.centroid.en.db.kch'
# Start fetching Event Registry Events from this date
START_DATETIME = date(2015, 1, 1)
# Days of Event date to fetch at once in batch mode
BATCH_INTERVAL = 1
# Socket timeout (seconds)
SOCKET_TIMEOUT = 120.0
# Wait time between requests after a timneout
REQUEST_SLEEP = 60
# Number of events-articles to fetch per request
ARTICLES_BATCH_SIZE = 20
#
EVENTS_BATCH_SIZE = 20

## TWEET SERVICE
# Database that stores tweets and the archive files that have been read
TWEET_DB_FILENAME = 'tweets.db.kch'
# The directory twitter observatories
#ARCHIVE_DIR = '../tweets'
ARCHIVE_DIR = '/home/rei/DATA/symphony/tweets/'
# Determines how often an archive directory is checked (seconds)
ARCHIVE_LOAD_INTERVAL = 60 * 2

## MATCH SERVICE
# How many days after an event can it be matched to a tweet
MATCH_DAYS_BEFORE = 3
