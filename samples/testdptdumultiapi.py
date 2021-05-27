# testdptdumultiapi.py
# Copyright (c) 2007, 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Test provision DPT file access in multi-step deferred update mode."""


if __name__ == '__main__':

    from basesup.dptdumultiapi import DPTdumultiapiError, DPTdumultiapi

    from basesup.api.constants import (
        FLT, INV, UAE, ORD, ONM, SPT,
        BSIZE, BRECPPG, BRESERVE, BREUSE,
        DSIZE, DRESERVE, DPGSRES,
        FILEORG, DEFAULT, EO, RRN, SUPPORTED_FILEORGS,
        DDNAME, FILE, FILEDESC, FOLDER, FIELDS,
        PRIMARY, SECONDARY, DEFER,
        DPT_DEFER_FOLDER,
        TAPEA, TAPEN,
        )

    dbspec = {
        'games': {
            DDNAME: 'GAMES',
            FILE: 'games.dpt',
            FILEDESC: {
                BSIZE: 50,
                BRECPPG: 10,
                DSIZE: 50,
                FILEORG: 36,
                },
            PRIMARY: 'SCORE',
            FIELDS: {
                'SCORE': {
                    FLT: False,
                    INV: False,
                    UAE: False,
                    ORD: False,
                    ONM: False,
                    SPT: 50,
                    },
                },
            },
        'positions': {
            DDNAME: 'POSITION',
            FILE: 'positions.dpt',
            FILEDESC: {
                BSIZE: 50,
                BRECPPG: 10,
                DSIZE: 50,
                FILEORG: 36,
                },
            SECONDARY: {
                'piecesquare': 'PIECESQUARE',
                },
            FIELDS: {
                'Positions': None,
                'PIECESQUARE': {},
                },
            DEFER: {
                'piecesquare': 1000000,
                },
            },
        'gash': {
            DDNAME: 'GASH',
            FILE: 'kk.dpt',
            FILEDESC: {
                BSIZE: 50,
                BRECPPG: 10,
                DSIZE: 50,
                FILEORG: 36,
                },
            FIELDS: {
                'Gash': None,
                #'SCORE':None,
                },
            },
        }
    dbfolder = 'D:/Roger/rmdbclasses/test/dpttest'

    try:
        dptapi = DPTdumultiapi(dbspec, dbfolder)
        for f in dptapi.get_dptfiles():
            print(f)
            print(dptapi.get_dptfiles()[f].__dict__)
    except DPTdumultiapiError as e:
        print(e)
