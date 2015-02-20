import sys
import datetime

import mmh3

from ermdb import db_open, db_close, db_get, db_put, db_exists, db_check
from ermdb import db_add_map, db_add_list, db_append_list_map
from er_common import er_get_urls_for_day, er_get_events_article, er_get_latest

from ermcfg import ER_URL_DB_FILENAME, ER_DATE_DB_FILENAME
from ermcfg import START_DATE, END_DATE
from ermcfg import ER_CENTROID_EN_DB_FILENAME


def convert_urlmap(urlmap):
        return {mmh3.hash_bytes(x): str(urlmap[x]) for x in urlmap}


def convert_artmap(artmap):
        return {str(x): artmap[x].encode('utf8') for x in artmap}


def convert_ids(idlist):
        return [str(x) for x in idlist]


def dbs_init(start_date=START_DATE,
             urldb_filename=ER_URL_DB_FILENAME,
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
    date_fetch = start_date
    date_max = END_DATE
    one_day = datetime.timedelta(days=1)

    while date_fetch <= date_max:
        print('Fetching ER data for %s' % (str(date_fetch),))
        urlmap = er_get_urls_for_day(date_fetch)
        event_ids = urlmap.values()
        n_events = len(event_ids)
        print('-fetched %d events with urls' % (n_events,))

        # check for empty day (possible?)
        if(n_events == 0):
            date_fetch = date_fetch + one_day
            continue

        # Hash and convert to string
        print('-add url map')
        urlmap = convert_urlmap(urlmap)
        db_add_map(urldb, urlmap)
        print('-done url map')

        # Get Event->Article map
        print('-fetching article map: english')
        artmap = er_get_events_article(event_ids, lang='eng')

        # Add Event->Article map
        n_centroids = len(artmap)
        print('-adding article map: english - %d events' % (n_centroids, ))
        if n_centroids > 0:
            artmap = convert_artmap(artmap)
            db_add_map(endb, artmap)

        # List of Date -> EventIds
        print('-add date map - %d events' % (n_events,))
        event_ids = convert_ids(event_ids)
        db_add_list(datedb, date_fetch.isoformat(), event_ids)

        print('-done date map')
        db_put(datedb, 'last_date', str(date_fetch))

        print('-Finished fecthing ER data for %s' % (str(date_fetch), ))
        date_fetch = date_fetch + one_day

    print('init done')

    # close databases
    db_close([urldb, datedb, endb])


def run_service(urldb, datedb, endb):
    for urlmap, artmap, datemap in er_get_latest(lang='eng'):
        db_add_map(urldb, convert_urlmap(urlmap))
        db_add_map(endb, convert_artmap(artmap))
        db_append_list_map(datedb, datemap)

    db_put(datedb, 'last_date', str(datetime.date.now()))
    db_check([urldb, datedb, endb])


def main():
    exists = db_exists([ER_URL_DB_FILENAME, ER_DATE_DB_FILENAME,
                        ER_CENTROID_EN_DB_FILENAME])

    if not exists:
        # Batch mode from scratch
        dbs_init()

    exists = db_exists([ER_URL_DB_FILENAME, ER_DATE_DB_FILENAME,
                        ER_CENTROID_EN_DB_FILENAME])
    if not exists:
        print("Failed to open dbs")
        return -1

    urldb = db_open(ER_URL_DB_FILENAME, create=False)
    datedb = db_open(ER_DATE_DB_FILENAME, create=False)
    endb = db_open(ER_CENTROID_EN_DB_FILENAME, create=False)

    # Resume Batch Mode
    if 'resume' in sys.argv:
        last_date_in_db = db_get(datedb, 'last_date')
        if last_date_in_db is None:
            raise ValueError('Invalid last date in database')
        print('Resuming from %s' % (last_date_in_db, ))
        # Convert to datetime.date, there is a better way to do this but f-it
        date_vals = last_date_in_db.split('-')
        date_vals = [int(x) for x in date_vals]
        last_date_in_db = datetime.date(*date_vals)
        # Move to next day
        last_date_in_db += datetime.timedelta(days=1)
        # Go
        try:
            dbs_init(start_date=last_date_in_db)
        except:
            db_close([urldb, datedb, endb])
            raise

    try:
        run_service(urldb, datedb, endb)
    except:
        db_close([urldb, datedb, endb])


if __name__ == '__main__':
    main()
