# testdbapi.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test object database using Berkeley DB."""


if __name__ == '__main__':

    from os.path import abspath

    # bsddb removed from Python 3.n
    try:
        from bsddb3.db import DB_CREATE, DB_INIT_MPOOL
    except ImportError:
        from bsddb.db import DB_CREATE, DB_INIT_MPOOL

    from basesup.api.constants import PRIMARY, SECONDARY

    from basesup.dbapi import DBapi

    dbnames = {
        'games':{
            PRIMARY:None,
            SECONDARY:{
                'source':None,
                'positions':'POSITION',
                'Event':None,
                'Site':None,
                'Date':None,
                'Round':None,
                'White':None,
                'Black':None,
                'Result':None,
                },
            },
        }

    dbtype = {}
    
    environment = {'flags':DB_CREATE | DB_INIT_MPOOL,
                   'gbytes':0,
                   'bytes':65536000,
                   }

    defer = {'games':{'positions':30000000}}

    db = DBapi(abspath('testfilename'), dbnames, dbtype, environment, defer)

    print 'db = ', db
    for i in db.__dict__:
        print
        print i, db.__dict__[i]
    print
    print '"main" keys'
    for i in db.__dict__['_main']:
        print
        print i
        print db.__dict__['_main'][i].__dict__
