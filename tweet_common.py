import os
import zipfile
import json
import mmh3
from datetime import datetime, date


def url_fix(url):
    '''disabled: basic url normalization, removes query parameters'''
    #import urlparse
    #parts = urlparse.urlparse(url)
    #return parts.netloc + parts.path
    return url


def read_tweet_file(filepath):
    '''Read a zipped tweet file (symphony/twitter observatory archive format)
    '''
    tweets = []
    with zipfile.ZipFile(filepath, 'r') as zf:
        name = zf.namelist()[0]
        for line in zf.open(name):
            try:
                tweet_json = json.loads(line)
            except:
                continue
            tweets.append(tweet_json)
    return tweets


def read_tweets_from_dir(dirpath='./tweets'):
    '''Read a directory of zipped tweet files (symphony/twitter observatory
       archive format)'''
    tweets = []
    files = os.listdir(dirpath)
    for tweet_file in files:
        filepath = os.path.join(dirpath, tweet_file)
        file_tweets = read_tweet_file(filepath)
        tweets += file_tweets

    return tweets


def read_tweets_from_dir_gen(dirpath='../tweets'):
    '''Generator version for reading one file at a time'''
    files = os.listdir(dirpath)
    for tweet_file in files:
        filepath = os.path.join(dirpath, tweet_file)
        yield read_tweet_file(filepath)


def tweets_filter_with_urls(tweets):
    '''Returns only tweets with URLs'''
    filtered_tweets = []

    for tweet in tweets:
        if (u'urls' not in tweet['entities']
                or len(tweet['entities']['urls'])) == 0:
            continue

        filtered_tweets.append(tweet)

    return filtered_tweets


def tweets_remove_fields(tweets, allowed=[u'id_str', u'text', u'created_at']):
    '''Removes all non-allowed fields from tweet.
       Adds a URL field with fixed, hashed and string-escaped expanded_urls
    '''

    for tweet in tweets:
        tweet_urls = tweet['entities']['urls']
        tweet_urls = [x[u'expanded_url'] for x in tweet_urls]
        tweet_urls = [url_fix(x) for x in tweet_urls]
        try:
            tweet_urls = [mmh3.hash_bytes(x) for x in tweet_urls]
        except:
            # discard url
            print('bad url (non-ascii) - urls for tweet discarded')
            tweet_urls = []
        tweet_urls = [x.encode('string-escape') for x in tweet_urls]
        tweet[u'urls'] = tweet_urls

        for key in tweet.keys():
            if key not in allowed + [u'urls']:
                del tweet[key]


def tweet_day(tweet):
    return datetime.strptime((tweet["created_at"]).strip('"'),
                             "%a %b %d %H:%M:%S +0000 %Y").date()


def tweet_archive_min_date(dirpath='../tweets'):
    min_date = date.today()

    for tweets in read_tweets_from_dir_gen(dirpath='../tweets'):
        for tweet in tweets:
            if 'created_at' in tweet:
                try:
                    tday = tweet_day(tweet)
                except:
                    continue
                if tday < min_date:
                    min_date = tday

    return min_date
