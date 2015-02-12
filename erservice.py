import kyotocabinet as kc
import datetime

from er_common import er_get_urls_for_day, er_get_events_article, er_get_latest

from ermcfg import ER_URL_DB_FILENAME, ER_DATE_DB_FILENAME
from ermcfg import START_DATETIME
from ermcfg import ER_CENTROID_EN_DB_FILENAME


def db_open(dbfile, create=False):
    '''
    Opens a database for writing.
    If create=False, throws exeption if it does not exist.
    '''

    db = kc.DB()
    if create:
        if not db.open(dbfile, kc.DB.OWRITER | kc.DB.OCREATE):
            print("Failed to create")
            return None
    elif not db.open(dbfile, kc.DB.OWRITER):
        print("Failed to open")
        return None
    else:
        print('created: %s' % (dbfile,))

    return db


def db_open_read(dbfile):
    db = kc.DB()
    if not db.open(dbfile, kc.DB.OREADER):
        print("Failed to open database for reading")
        return None


def add_map(db, kvmap):
    '''
    Adds a Key -> Value dictionary to the DB
    '''
    for key, value in kvmap.items():
        db.add(key, value)


def add_list(db, key, vlist):
    '''
    Adds Key -> List to db. vlist must be a list of strings (e.g. id_str)
    '''
    db.add(key, ' '.join(vlist))


def dbs_init(urldb_filename=ER_URL_DB_FILENAME,
             datedb_filename=ER_DATE_DB_FILENAME,
             endb_filename=ER_CENTROID_EN_DB_FILENAME):
    '''
    Initializes all event databases
    '''
    # open databases
    urldb = db_open(urldb_filename, create=True)
    datedb = db_open(datedb_filename, create=True)
    endb = db_open(endb_filename, create=True)
    if urldb is None or datedb is None or endb is None:
        return None

    # start and end fetch dates
    date_fetch = START_DATETIME
    date_max = datetime.date.today()
    one_day = datetime.timedelta(days=1)

    while date_fetch <= date_max:
        print('fetching for %s' % (str(date_fetch),))
        urlmap = er_get_urls_for_day(date_fetch)
        event_ids = urlmap.values()

        # increment date, handle day change, check for empty day (possible?)
        date_fetch = date_fetch + one_day
        date_max = datetime.datetime.today()
        if(len(event_ids) == 0):
            continue

        print('\tadd url map')
        add_map(urldb, urlmap)
        print('\tdone url map')

        print('\tadd date map')
        add_list(date_fetch.isoformat(), event_ids)
        print('\tdone date map')

        print('\tfetching article map: english')
        artmap = er_get_events_article(event_ids, lang='eng')

        print('\tadding article map: english')
        add_map(endb, artmap)

    print('init done')

    # close databases
    urldb.close()
    datedb.close()
    endb.close()


def append_list_map(db, listmap):
    for key in listmap:
        dbval = db.get(key)
        dblist = dbval.split()
        # check if same list
        if dblist == listmap[key]:
            continue  # yup, do nothing
        # extend
        vlist = dblist + listmap[key]
        # uniquify
        vlist = list(set(vlist))
        add_list(db, key, vlist)


def run_service(urldb, datedb, endb):
    for urlmap, artmap, datemap in er_get_latest(lang='eng'):
        add_map(urldb, urlmap)
        add_map(endb, artmap)
        append_list_map(datedb, datemap)


def main():
    urldb = db_open(ER_URL_DB_FILENAME, create=False)
    datedb = db_open(ER_DATE_DB_FILENAME, create=False)
    endb = db_open(ER_CENTROID_EN_DB_FILENAME, create=False)
    if urldb is None or datedb is None or endb is None:
        dbs_init()
        urldb = db_open(ER_URL_DB_FILENAME, create=False)
        datedb = db_open(ER_DATE_DB_FILENAME, create=False)
        endb = db_open(ER_CENTROID_EN_DB_FILENAME, create=False)

    if urldb is None or datedb is None or endb is None:
        print("Failed to open dbs")
        return -1

    try:
        run_service(urldb, datedb, endb)
    except:
        urldb.close()
        datedb.close()
        endb.close()


if __name__ == '__main__':
    main()
