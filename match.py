import kyotocabinet as kc
from tweet_common import read_tweets_from_dir
from tweet_common import tweets_remove_fields, tweets_filter_with_urls

ARCHIVEDIR = '../tweets'
ERDBFILENAME = 'er.db.kch'
MATCHEDBFILENAME = 'matched.db.kch'


def match_url(db, tweet):
    for url in tweet['urls']:
        eventid = db.get(url)
        if eventid is not None:
            return eventid

    return None


def match_tweet_archive(archive_dir=ARCHIVEDIR, er_db_file=ERDBFILENAME,
                        matched_db_file=MATCHEDBFILENAME):
    erdb = kc.DB()
    if not erdb.open(er_db_file, kc.DB.OREADER):
        print("Failed to open url db")
        return None

    matched_db = kc.DB()
    if not matched_db.open(matched_db_file, kc.DB.OWRITER | kc.DB.OCREATE):
        print("Failed to open/create matched db")
        return None

    count = 0
    for tweets in read_tweets_from_dir(archive_dir):
        tweets = tweets_remove_fields(tweets)
        tweets = tweets_filter_with_urls(tweets)
        for tweet in tweets:
            eventid = match_url(erdb, tweet)
            if eventid is not None:
                matched_db.add(tweet['id_str'], eventid)
                count += 1

    print('Matched %d tweets' % (count, ))
