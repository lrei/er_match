'''
Matches tweets based on URLs
'''

import json
from ermdb import db_open, db_close, db_get, db_put
from ermcfg import TWEET_DB_FILENAME, MATCH_URL_DB_FILENAME, ER_URL_DB_FILENAME


def match_url_tweet(url_db, tweet_json_str):
    try:
        tweet = json.loads(tweet_json_str)
    except:
        return None

    # discard tweets without urls
    if 'urls' not in tweet:
        return None
    elif len(tweet['urls']) == 0:
        return None

    # try to match urls
    for hashed_url in tweet['urls']:
        event_id = db_get(url_db, hashed_url)
        if event_id is not None:
            return event_id

    # No match found
    return None


def match_urls_in_tweetdb(tweet_db_filename=TWEET_DB_FILENAME,
                          url_db_filename=ER_URL_DB_FILENAME,
                          match_url_db_filename=MATCH_URL_DB_FILENAME):
    '''Match all tweets in the database.
    Note: Currently does not take dates into consideration.
    '''

    tweet_db = db_open(tweet_db_filename, readonly=True)
    url_db = db_open(url_db_filename, readonly=True)
    match_url_db = db_open(match_url_db_filename, readonly=False, create=True)

    # Stats
    s_tweets = 0
    s_matched = 0

    '''Paralelizing this should be:
        1 - Read a chunk of tweets from DB
        2 - Parallel Match into dictionary
        3 - db_add_map() the dictionary
    '''

    with tweet_db.begin(write=False) as txn:
        cursor = txn.cursor()
        for tweet_id, value in cursor:
            s_tweets += 1

            event_id = match_url_tweet(url_db, value)

            # if match found, write to db
            if event_id is not None:
                db_put(match_url_db, tweet_id, event_id)
                s_matched += 1

            if s_tweets % 10000 == 0:
                print('matched = %d / %d' % (s_matched, s_tweets))

    # Finally, display stats
    print('Matched = %d / %d' % (s_matched, s_tweets))

    # close dbs
    db_close(tweet_db)
    db_close(url_db)
    db_close(match_url_db)


def main():
    match_urls_in_tweetdb()


if __name__ == '__main__':
    main()
