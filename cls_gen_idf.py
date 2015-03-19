#!/env/python

from __future__ import division
import sys
import cPickle as pickle
from math import log
from collections import Counter, defaultdict
from itertools import islice, chain
from multiprocessing import Pool, cpu_count
from text_normalize import normalize_text, n_grams


# Config
MAX_N = 3
MIN_N = 1
ngram_range = range(MIN_N, MAX_N)
min_df = 10
total_docs = 1000000     # 1M
chunk_size = 10000       # 10K


def to_chunks(iterable, chunk_size):
    it = iter(iterable)
    while True:
        first = next(it)
        rest = islice(it, chunk_size - 1)
        yield chain((first,), rest)


def process_line(text_line):
    text_line = normalize_text(text_line.decode('utf8'))
    result = []

    for n in ngram_range:
        ngrams_line = list(set(n_grams(text_line, n)))
        result.extend(ngrams_line)

    return result


def main():
    #
    # Init
    #
    counter = Counter()
    ndocs = 0
    p = 0
    fname = sys.argv[1]
    pool = Pool(cpu_count())

    #
    # Read-Count Loop
    #
    with open(fname, 'r') as f:
        # read chunk_size lines
        for chunk in to_chunks(f, chunk_size):
            # generate ngrams (sets as lists)
            results = pool.map(process_line, chunk)
            # update counts
            for res in results:
                ndocs += 1
                counter.update(res)
            # display progess
            last_p = p
            p = int((ndocs / total_docs) * 100)
            if p != last_p:
                print('%d%% (%d docs)' % (p, ndocs))
            # check end condition
            if ndocs >= total_docs:
                break

    #
    # Create defaultdict and Calculate IDF
    #
    print('Building IDF dict')

    # for OOV words, the IDF weight will be log(ndocs) - i.e. the highest score
    dcounter = defaultdict(int)
    dcounter[None] = log(ndocs)

    for x in list(counter.keys()):
        # Enforce mininum DF by ignoring terms that are below the threshold
        if counter[x] >= min_df:
            # Convert to IDF
            dcounter[x] = log(ndocs / (1 + counter[x]))

        # delete counter entries as we go in a bid to save memory
        del counter[x]

    print('V = %d' % (len(dcounter),))

    del counter  # delete the old counter object

    # Save
    print('Saving')
    p_proto = pickle.HIGHEST_PROTOCOL
    pickle.dump(dcounter, open('pickled/idf.counter', 'wb'), p_proto)


if __name__ == '__main__':
    main()
