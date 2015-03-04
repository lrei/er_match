'''
Config/Settings for ER Tweet Matching Service
'''

from datetime import date


# GLOBAL
# Maximum size a database can grow to (should be a multiple of 10MB)
MAX_DB_SIZE = 10485760 * 1000 * 100     # 10MB -> 10GB -> 1TB

## ER SERVICE
# Credentials
ER_USER = 'luis.rei@ijs.si'
ER_PASS = ''
# Log ER Requests?
ER_LOG = False
# Seconds to wait between consecutive page requests
ER_WAIT_BETWEEN_REQUESTS = 0.5
# Database file that stores event registry url-eventid map
ER_URL_DB_FILENAME = 'er.en.url.db'
ER_URL_DB_FILENAME = '/media/rei/data/er_match/er.en.url.db'
ER_URL_DB_FILENAME = '/media/storage/DATA/symphony/er_match/er.en.url.db'
# Database file that stores date-eventid mappings
ER_DATE_DB_FILENAME = 'er.en.date.db'
ER_DATE_DB_FILENAME = '/media/rei/data/er_match/er.en.date.db'
# Database file that stores eventid-centroid (english) mappings (Hash)
ER_CENTROID_EN_DB_FILENAME = 'er.en.centroid.db'
ER_CENTROID_EN_DB_FILENAME = '/media/rei/data/er_match/er.en.centroid.db'
ER_CENTROID_EN_DB_FILENAME = '/media/storage/DATA/symphony/er_match/er.en.centroid.db'
# Database that stores the days that have been fetched
ER_STATUS_DB_FILENAME = 'er.status.db'
ER_STATUS_DB_FILENAME = '/media/rei/data/er_match/er.status.db'
# Start fetching (archive mode) Event Registry Events from this date
START_DATE = date(2014, 11, 1)
# End fetching (archive mode) at date
#END_DATETIME = datetime.date.today()
END_DATE = date(2015, 1, 31)
# Days of Event date to fetch at once in batch mode
BATCH_INTERVAL = 1
# Socket timeout (seconds)
SOCKET_TIMEOUT = 120.0
# Wait time between requests after a timneout
REQUEST_SLEEP = 60
# Number of events-articles to fetch per request
ARTICLES_BATCH_SIZE = 200
# Number of URLs to request per Page
URLS_PER_PAGE = 200     # 200 should be the maximum set by the server
# Number of Events to ask for the URL in online mode
EVENTS_BATCH_SIZE = 40

## TWEET SERVICE
# Database that stores tweets and the archive files that have been read
TWEET_DB_FILENAME = 'tweets.db'
TWEET_DB_FILENAME = '/media/rei/data/er_match/tweets.db'
TWEET_DB_FILENAME = '/media/storage/DATA/symphony/er_match/tweets.db'
# Database that stores the loaded archive file names
ARCHIVE_DB_FILENAME = 'archives.db'
ARCHIVE_DB_FILENAME = '/media/rei/data/er_match/archives.db'
# The directory of twitter observatory archive files (crawler archives)
#ARCHIVE_DIR = '../tweets'
ARCHIVE_DIR = '/media/rei/data/er_match/tweets/'
ARCHIVE_DIR = '/media/storage/DATA/symphony/er_match/tweets/'
# Determines how often an archive directory is checked (seconds)
ARCHIVE_LOAD_INTERVAL = 60 * 2

## MATCH SERVICE
# How many days after an event can it be matched to a tweet
MATCH_DAYS_BEFORE = 3
MATCH_URL_DB_FILENAME = 'match.url.db'
MATCH_URL_DB_FILENAME = '/media/rei/data/er_match/match.url.db'
MATCH_URL_DB_FILENAME = '/media/storage/DATA/symphony/er_match/match.url.db'
