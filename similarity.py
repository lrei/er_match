'''Module for text similarity '''

from __future__ import division
import math
from collections import Counter


def jaccard_sim(set_1, set_2):
    '''Compute Jacard Similarity'''
    # handle lists and other set-convertible objects
    if not isinstance(set_1, set):
        set_1 = set(set_1)
    if not isinstance(set_2, set):
        set_2 = set(set_2)

    # calculate and return
    n = len(set_1.intersection(set_2))
    # handle empty insersection
    if n == 0:
        return 0.0
    return n / float(len(set_1) + len(set_2) - n)


def cosine_sim(words1, words2):
    vec1 = Counter(words1)
    vec2 = Counter(words2)

    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
    sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def page_sim(page_set, tweet_set):
    '''A similarity measure that accounts for difference in size betweet a page
    (article body) and a tweet
    '''
    # handle lists and other set-convertible objects
    if not isinstance(page_set, set):
        page_set = set(page_set)
    if not isinstance(tweet_set, set):
        tweet_set = set(tweet_set)

    n = len(tweet_set)
    i = len(page_set.intersection(tweet_set))

    return i * math.log(n + 1)
