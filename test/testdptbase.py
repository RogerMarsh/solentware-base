# testdptbase.py
# Copyright (c) 2007 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test provision DPT file access using bsddb style methods."""


if __name__ == '__main__':

    from basesup.api.constants import (
        FLT, INV, UAE, ORD, ONM, SPT,
        BSIZE, BRECPPG, BRESERVE, BREUSE,
        DSIZE, DRESERVE, DPGSRES,
        FILEORG, DEFAULT, EO, RRN, SUPPORTED_FILEORGS,
        MANDATORY_FILEATTS, FILEATTS,
        PRIMARY_FIELDATTS, SECONDARY_FIELDATTS,
        DDNAME, FILE, FILEDESC, FOLDER, FIELDS,
        PRIMARY, SECONDARY, DEFER,
        BTOD_FACTOR, DEFAULT_RECORDS,
        DPT_DEFER_FOLDER, DPT_DU_SEQNUM, DPT_SYS_FOLDER,
        )

    from basesup.dptbase import DPTbaseError, DPTbase

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
        dpt = DPTbase(dbspec, dbfolder)
        for f in dpt._dptfiles:
            print f
            print dpt._dptfiles[f].__dict__
    except DPTbaseError, e:
        print e
