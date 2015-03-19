from __future__ import division
import math
import json
import itertools
import cPickle as pickle
from multiprocessing import Pool, cpu_count
from collections import namedtuple
from text_normalize import normalize_text, n_grams
from similarity import jaccard_sim, cosine_sim, page_sim
from ermdb import db_open, db_close, db_get
from ermcfg import TWEET_DB_FILENAME, MATCH_URL_DB_FILENAME
from ermcfg import ER_CENTROID_EN_DB_FILENAME


# Ngrams
Ngrams = namedtuple('Ngrams', ['uni', 'bi', 'tri', 'quad'])


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def get_related():
    '''Read training set from matched URLs DB'''
    related = {}

    url_match_db = db_open(MATCH_URL_DB_FILENAME, readonly=True, create=False)
    with url_match_db.begin(write=False) as txn:
        cursor = txn.cursor()
        for tweet_id, event_id in cursor:
            related[tweet_id] = event_id
    db_close(url_match_db)
    return related


def get_data_raw(related, event_mult=8):
    '''Load Tweet text, Article Title and Article Body'''
    tweets = {}
    titles = {}
    bodies = {}
    missing_articles = 0

    print('\nLoading from DBs')
    tweet_db = db_open(TWEET_DB_FILENAME, create=False, readonly=True)
    article_db = db_open(ER_CENTROID_EN_DB_FILENAME, create=False,
                         readonly=True)

    for tweet_id in related:
        tweet_val = db_get(tweet_db, tweet_id)
        tweet = json.loads(tweet_val)[u'text']
        tweets[tweet_id] = tweet

        # get article if necessary
        event_id = related[tweet_id]

        if event_id in titles:
            tweets[tweet_id] = tweet
            continue

        article = db_get(article_db, event_id)
        if article is None:
            missing_articles += 1
            continue

        article = json.loads(article)
        titles[event_id] = article['title']
        bodies[event_id] = article['body']

    total_articles = len(titles)
    max_articles = total_articles * event_mult

    print('Missing articles: %d' % (missing_articles,))
    print('Matched articles: %d' % (total_articles,))
    print('Matched tweeets: %d' % (len(tweets),))

    # Load ALL articles (even unmatched articles)
    with article_db.begin(write=False) as txn:
        with txn.cursor() as curs:
            for event_id, article in curs:
                if event_id in titles:
                    continue
                else:
                    article = json.loads(article)
                    titles[event_id] = article['title']
                    bodies[event_id] = article['body']
                    total_articles += 1
                    if total_articles >= max_articles:
                        break

    # close the dbs
    db_close(tweet_db)
    db_close(article_db)

    print('Matched tweeets: %d' % (len(tweets),))
    print('Total articles loaded: %d' % (len(titles), ))

    return (tweets, titles, bodies)


def generate_article_ngrams(article_data):
    event_id, title_text, body_text = article_data
    title_text = normalize_text(title_text)

    title_ngrams = Ngrams(n_grams(title_text, 1), n_grams(title_text, 2),
                          n_grams(title_text, 3), n_grams(title_text, 4))

    body_text = normalize_text(body_text)
    body_ngrams = Ngrams(n_grams(body_text, 1), n_grams(body_text, 2),
                         n_grams(body_text, 3), n_grams(body_text, 4))

    return (event_id, title_ngrams, body_ngrams)


def generate_tweet_ngrams(tweet_data, min_tokens=4):
    tweet_id, tweet_text = tweet_data
    tweet_text = normalize_text(tweet_text)
    if len(tweet_text.split()) < min_tokens:
        return None

    tweet_ngrams = Ngrams(n_grams(tweet_text, 1), n_grams(tweet_text, 2),
                          n_grams(tweet_text, 3), n_grams(tweet_text, 4))

    return (tweet_id, tweet_ngrams)


def get_ngrams(related, tweets, titles, bodies):
    '''
    Output is the ngrams for articles and tweets
    '''
    pool = Pool(cpu_count())

    #
    # Articles
    #
    print('\nNormalizing Article text and Generating Article Ngrams')
    chunk_size = 1000
    event_ids = titles.keys()
    total_chunks = math.ceil(len(event_ids) / chunk_size)
    n_chunks = 0

    for chunk in grouper(chunk_size, event_ids):
        data = [(event_id, titles[event_id], bodies[event_id])
                for event_id in chunk]
        results = pool.map(generate_article_ngrams, data)
        for x in results:
            event_id, title, body = x
            titles[event_id] = title
            bodies[event_id] = body
        n_chunks += 1
        p = (n_chunks / total_chunks) * 100
        print('\r%f%%' % (p,))

    #
    # Tweets
    #
    tweets_new = dict()
    print('\nNormalizing Tweet Text and Generating Tweet Ngrams')
    chunk_size = 1000
    tweet_ids = tweets.keys()
    total_chunks = math.ceil(len(tweet_ids) / chunk_size)
    n_chunks = 0

    for chunk in grouper(chunk_size, tweet_ids):
        data = [(tweet_id, tweets[tweet_id]) for tweet_id in chunk]
        results = pool.map(generate_tweet_ngrams, data)
        for x in results:
            if x is not None:
                tweet_id, tweet_ngrams = x
                tweets_new[tweet_id] = tweet_ngrams
        n_chunks += 1
        p = (n_chunks / total_chunks) * 100
        print('\r%f%%' % (p,))

    # Update tweets and Related
    tweets = tweets_new
    for tweet_id in list(related.keys()):
        if tweet_id not in tweets:
            del related[tweet_id]

    print('New Matched Size = %d' % (len(related),))

    return (tweets, titles, bodies)


def calc_sims(tweet_grams, title_grams, body_grams, n):
    sims = []

    # title to tweet similarity
    sims.append(jaccard_sim(tweet_grams, title_grams))

    # tweet to body sims
    sims.append(page_sim(body_grams, tweet_grams))
    sims.append(jaccard_sim(tweet_grams, body_grams))
    sims.append(cosine_sim(tweet_grams, body_grams))

    return sims


def extract_features(tweet_ngrams, title_ngrams, body_ngrams, n=4):
    x = []
    n -= 1  # 0 based indexing

    while n >= 0:
        s = calc_sims(tweet_ngrams[n], title_ngrams[n], body_ngrams[n], n)
        x.extend(s)
        n -= 1

    return x


def extract_features_map(ex_data):
    '''Version for generating datasets, ignores examples with no similarity'''
    tweet_ngrams, title_ngrams, body_ngrams = ex_data
    x = extract_features(tweet_ngrams, title_ngrams, body_ngrams)
    if sum(x) == 0:
        return None
    return x


def gen_dataset_pos(related, tweets, titles, bodies):
    '''Positive Examples'''

    print('\nGenerating Positive Examples')
    x_list = []

    pool = Pool(cpu_count())
    tweet_ids = related.keys()
    chunk_size = 1000
    total_chunks = math.ceil(len(tweet_ids) / chunk_size)
    n_chunks = 0

    for chunk in grouper(chunk_size, tweet_ids):
        data = [(tweets[tweet_id], titles[related[tweet_id]],
                 bodies[related[tweet_id]]) for tweet_id in chunk]
        results = pool.map(extract_features_map, data)
        for x in results:
            if x is not None:
                x_list.append(x)
        n_chunks += 1
        p = (n_chunks / total_chunks) * 100
        print('\r%f%%' % (p,))

    return x_list


def gen_dataset_neg(related, tweets, titles, bodies, npos):
    '''Negative Examples'''
    print('\nGenerating Negative Examples')
    x_list = []

    nprocs = cpu_count()
    pool = Pool(nprocs)

    tweet_ids = related.keys()
    event_ids = titles.keys()
    rset = set(related.items())
    all_combinations = itertools.product(tweet_ids, event_ids)

    chunk_size = 10000
    nneg = 0

    npos -= 1   # remove 1 for the 0 vector example

    for chunk in grouper(chunk_size, all_combinations):
        f_chunk = (p for p in chunk if p not in rset)
        # generate examples
        data = [(tweets[tweet_id], titles[event_id],
                 bodies[event_id]) for (tweet_id, event_id) in f_chunk]
        if len(data) == 0:
            continue
        # test examples
        print('testing')
        results = pool.map(extract_features_map, data)
        # add valid examples
        for x in results:
            if x is not None:
                x_list.append(x)
        nneg = len(x_list)
        # print stats
        print('\r%f%% (%d / %d)' % ((nneg / npos) * 100, nneg, npos))
        if nneg > npos:
            break

    # all zero example
    n_feats = len(x_list[0])
    print('Number of Features = %d' % (n_feats,))
    x_list.insert(0, [0] * n_feats)

    # trim
    x_list = x_list[0:nneg]
    return x_list


def main():
    ''' '''
    p_proto = pickle.HIGHEST_PROTOCOL
    related = get_related()

    #
    # Raw Data
    #
    try:
        # Try to load from serialized files
        tweets = pickle.load(open('pickled/tweets.raw', 'rb'))
        titles = pickle.load(open('pickled/titles.raw', 'rb'))
        bodies = pickle.load(open('pickled/bodies.raw', 'rb'))
        print('Loaded Raw Files')
    except:
        # else load from dbs
        tweets, titles, bodies = get_data_raw(related)
        pickle.dump(tweets, open('pickled/tweets.raw', 'wb'), p_proto)
        pickle.dump(titles, open('pickled/titles.raw', 'wb'), p_proto)
        pickle.dump(bodies, open('pickled/bodies.raw', 'wb'), p_proto)

    #
    # Ngrams
    # This is bypassed because Python's Pickle requires huge ammounts of memory
    # (double?) which is fundamentally retarded and unusable
    '''
    try:
        tweets = pickle.load(open('pickled/tweets.ngrams', 'rb'))
        titles = pickle.load(open('pickled/titles.ngrams', 'rb'))
        bodies = pickle.load(open('pickled/bodies.ngrams', 'rb'))
        print('Loaded N-Gram Files')
    except:
        tweets, titles, bodies = get_ngrams(related, tweets, titles, bodies)
        pickle.dump(bodies, open('pickled/bodies.ngrams', 'wb'), p_proto)
        pickle.dump(titles, open('pickled/titles.ngrams', 'wb'), p_proto)
        pickle.dump(tweets, open('pickled/tweets.ngrams', 'wb'), p_proto)
    '''
    tweets, titles, bodies = get_ngrams(related, tweets, titles, bodies)

    #
    # Positive Training Set
    #
    try:
        pos = pickle.load(open('pickled/pos.feats', 'rb'))
        print('Loaded Positve Training Set')
    except:
        pos = gen_dataset_pos(related, tweets, titles, bodies)
        pickle.dump(pos, open('pickled/pos.feats', 'wb'), p_proto)

    print('Total Pos Sim = %f' % (sum([sum(x) for x in pos]),))
    p_size = len(pos)
    print('Pos Size = %d' % (p_size,))
    del pos

    #
    # Negative Training Set
    #
    try:
        neg = pickle.load(open('pickled/neg.feats', 'rb'))
        print('Loaded Negative Training Set')
    except:
        neg = gen_dataset_neg(related, tweets, titles, bodies, p_size)
        pickle.dump(neg, open('pickled/neg.feats', 'wb'), p_proto)

    print('Total Neg Sim = %f' % (sum([sum(x) for x in neg]),))

if __name__ == '__main__':
    main()
