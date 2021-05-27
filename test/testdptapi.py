# testdptapi.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test provision DPT file access in non-deferred update mode."""


if __name__ == '__main__':

    from basesup.dptapi import DPTapiError, DPTapi

    from basesup.api.constants import (
        DDNAME, FILE, FILEDESC, PRIMARY, FIELDS, SECONDARY,
        FLT, INV, UAE, ORD, ONM, SPT,
        BSIZE, BRECPPG, BRESERVE, BREUSE,
        DSIZE, DRESERVE, DPGSRES,
        FILEORG,
        )

    dbspec = {
        'games':{
            DDNAME:'GAMES',
            FILE:'games.dpt',
            FILEDESC:{
                BSIZE:50,
                BRECPPG:10,
                DSIZE:50,
                FILEORG:36,
                },
            PRIMARY:'SCORE',
            FIELDS:{
                'SCORE':{
                    FLT:False,
                    INV:False,
                    UAE:False,
                    ORD:False,
                    ONM:False,
                    SPT:50,
                    },
                },
            },
        'positions':{
            DDNAME:'POSITION',
            FILE:'positions.dpt',
            FILEDESC:{
                BSIZE:50,
                BRECPPG:10,
                DSIZE:50,
                FILEORG:36,
                },
            SECONDARY:{
                'piecesquare':'PIECESQUARE',
                },
            FIELDS:{
                'Positions':None,
                'PIECESQUARE':{},
                },
            DEFER:{
                'piecesquare':1000000,
                },
            },
        'gash':{
            DDNAME:'GASH',
            FILE:'kk.dpt',
            FILEDESC:{
                BSIZE:50,
                BRECPPG:10,
                DSIZE:50,
                FILEORG:36,
                },
            FIELDS:{
                'Gash':None,
                #'SCORE':None,
                },
            },
        }
    dbfolder = 'D:/Roger/rmdbclasses/test/dpttest'

    try:
        dpt = DPTapi(dbspec, dbfolder)
        for f in dpt._dptfiles:
            print f
            print dpt._dptfiles[f].__dict__
    except DPTapiError, e:
        print e
