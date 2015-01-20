import rocksdb
from datetime import date, timedelta
from tweet_common import tweet_archive_min_date
from er_common import er_get_urls


DBFILENAME = 'er.db'
ARCHIVEDIR = '../tweets'
FETCH_DAYS_BEFORE = 1


def db_open(dbfile=DBFILENAME, create=False, bulk=False):
    '''Opens the databe. If create=False, throws exeption if it does not exist
    '''

    opts = rocksdb.Options()
    opts.create_if_missing = create
    opts.max_open_files = 300000
    opts.write_buffer_size = 67108864  # 64MB
    opts.max_write_buffer_number = 2
    opts.target_file_size_base = 67108864  # 64MB
    #opts.block_cache = rocksdb.LRUCache(1 * (1024 ** 3))  # 1GB
    #opts.block_cache_compressed = rocksdb.LRUCache(256 * (1024 ** 2))  # 256MB
    opts.disable_data_sync = bulk

    db = rocksdb.DB(dbfile, opts)
    return db


def add_urlmap(db, urlmap):
    '''Adds a URL -> EventID dictionary to the DB
    '''
    batch = rocksdb.WriteBatch()
    for key, value in urlmap.items():
        batch.put(key, value)
    db.write(batch)


def db_init():
    db = db_open(create=True)
    date_min = tweet_archive_min_date(ARCHIVEDIR)
    date_max = date.today()

    batch_interval = timedelta(days=FETCH_DAYS_BEFORE)
    date_fetch_start = date_min - batch_interval
    date_fetch_end = date_min

    while date_fetch_start < date_max:
        print('fetching between %s and %s' % (date_fetch_start.str(),
                                              date_fetch_end.str()))

        urlmap = er_get_urls(date_fetch_start, date_fetch_end)
        print('adding')
        add_urlmap(db, urlmap)
        date_fetch_start = date_fetch_end
        date_fetch_end = date_fetch_end + batch_interval


def main():

    try:
        db = db_open()
    except:
        db_init()

    db = db_open()

if __name__ == '__main__':
    main()
