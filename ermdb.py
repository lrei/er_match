'''
Lightweight database abstraction layer
'''

import os
import lmdb
from ermcfg import MAX_DB_SIZE


def db_open(dbfile, create=False, readonly=False):
    '''
    Opens the databe. If create=False, exception
    '''

    try:
        db = lmdb.open(dbfile, map_size=MAX_DB_SIZE, metasync=True, sync=True,
                       readonly=readonly, create=create)
    except:
        raise

    print("Opened db at %s" % (dbfile,))

    return db


def db_exists(dbfiles):
    '''Checks if a database file/dir exists or a list of files all exist'''

    if type(dbfiles) is list:
        for dbfile in dbfiles:
            if not os.path.exists(dbfile):
                return False
        return True
    else:
        return os.path.exists(dbfiles)


def db_put(db, key, val):
    if type(key) is unicode:
        key = str(key)
    if type(val) is unicode:
        val = str(val)

    with db.begin(write=True) as txn:
        txn.put(key, val)


def db_get(db, key):
    if type(key) is unicode:
        key = str(key)
    with db.begin(write=False) as txn:
        return txn.get(key)


def db_close(dbs):
    '''Close a DB or list of DBs'''
    if type(dbs) is list:
        for db in dbs:
            if db is not None:
                db.close()
        print('DBs closed')
    else:
        dbs.close()


def db_add_map(db, kvmap):
    '''
    Adds a Key -> Value dictionary to the DB
    '''
    with db.begin(write=True) as txn:
        for key, val in kvmap.items():
            txn.put(key, val)


def db_add_list(db, key, vlist):
    '''
    Adds Key -> List to db. vlist must be a list of strings (e.g. id_str)
    '''
    with db.begin(write=True) as txn:
        txn.put(key, ' '.join(vlist))


def db_append_list_map(db, listmap):
    '''
    '''
    with db.begin(write=True) as txn:
        for key in listmap:
            dbval = txn.get(key)
            if not dbval:
                dblist = []
            else:
                dblist = dbval.split()

            # check if same list
            if dblist == listmap[key]:
                continue  # yup, do nothing

            # extend
            vlist = dblist + listmap[key]
            # uniquify
            vlist = list(set(vlist))
            db_add_list(db, key, vlist)


def db_check(dbs):
    if type(dbs) is list:
        for db in dbs:
            db.reader_check()
    else:
        db.reader_check()
