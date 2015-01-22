import kyotocabinet as kc
from datetime import date, timedelta
from tweet_common import tweet_archive_min_date
from er_common import er_get_urls


DBFILENAME = 'er.db.kch'
ARCHIVEDIR = '../tweets'
FETCH_DAYS_BEFORE = 1


def db_open(dbfile=DBFILENAME, create=False, bulk=False):
    '''Opens the databe. If create=False, throws exeption if it does not exist
    '''

    db = kc.DB()
    if create:
        if not db.open(dbfile, kc.DB.OWRITER | kc.DB.OCREATE):
            print("Failed to create")
            return None
    elif not db.open(dbfile, kc.DB.OWRITER):
        print("Failed to open")
        return None

    return db


def add_urlmap(db, urlmap):
    '''Adds a URL -> EventID dictionary to the DB
    '''
    for key, value in urlmap.items():
        db.add(key, value)


def db_init():
    db = db_open(create=True)
    date_min = tweet_archive_min_date(ARCHIVEDIR)
    date_max = date.today()

    batch_interval = timedelta(days=FETCH_DAYS_BEFORE)
    date_fetch_start = date_min - batch_interval
    date_fetch_end = date_min

    while date_fetch_start <= date_max:
        print('fetching between %s and %s' % (str(date_fetch_start),
                                              str(date_fetch_end)))

        urlmap = er_get_urls(date_fetch_start, date_fetch_end)
        print('adding')
        add_urlmap(db, urlmap)
        print('done')
        date_fetch_start = date_fetch_end
        date_fetch_end = date_fetch_end + batch_interval


def run_service(db):
    #urlmap = er_get_new()
    pass


def main():

    db = db_open()

    if db is None:
        db_init()
        db = db_open()

    if db is None:
        print("Failed to open db")
        return 0

    run_service(db)

    db.close()


if __name__ == '__main__':
    main()
