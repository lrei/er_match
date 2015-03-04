'''Text preprocessing'''

import string
import unicodedata
import re
import nltk
from nltk.util import ngrams


def n_grams(txt, n):
    return list(ngrams(txt.split(), n))


def remove_accents(txt):
    ''' Removes accents and punctuation/special then converts to lowercase'''
    ascii = string.ascii_letters + string.digits
    new_txt = []
    for data in txt.split():
        t = ''.join(x for x in unicodedata.normalize(
            'NFKD', data) if x in ascii).lower()
        if t != ' ':
            new_txt.append(t)
    return ' '.join(new_txt)


def remove_numbers(text):
    text = re.sub('^[0-9]+', '', text)
    return text


def remove_urls(text):
    url_re = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    text = url_re.sub(' ', text)
    return text


def remove_stopwords(text, stopwords):
    text = text.encode('utf-8')
    filtered_text = [w for w in text.split() if not w in stopwords]
    return " ".join(filtered_text)


def normalize_text(txt, lang='english'):
    # 1 - remove urls
    txt = remove_urls(txt)

    # 2 - remove accents/special chars/punct and lower text
    txt = remove_accents(txt)

    # 3 - remove stopwords
    stopwords = nltk.corpus.stopwords.words(lang)
    txt = remove_stopwords(txt, stopwords)

    return txt
