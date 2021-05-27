# constants_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""constants tests"""

import unittest

from .. import constants


class ConstantsFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test__assumptions(self):
        """"""
        msg = 'Failure of this test invalidates all other tests'
        ae = self.assertEqual
        ae(constants.BSDDB_MODULE, 'bsddb')
        ae(constants.BSDDB3_MODULE, 'bsddb3')
        ae(constants.DPT_MODULE, 'dptdb.dptapi')
        ae(constants.SQLITE3_MODULE, 'sqlite3')
        ae(constants.APSW_MODULE, 'apsw')
        ae(constants.SQLITE_VALUE_COLUMN, 'Value')
        ae(constants.SQLITE_SEGMENT_COLUMN, 'Segment')
        ae(constants.SQLITE_COUNT_COLUMN, 'RecordCount')
        ae(constants.SQLITE_RECORDS_COLUMN, 'RecordNumbers')
        ae(constants.ACCESS_METHOD, 'access_method')
        ae(constants.BTREE, 'btree')
        ae(constants.HASH, 'hash')
        ae(constants.RECNO, 'recno')
        ae(constants.BLOB, 'blob')
        ae(constants.FLT, 'float')
        ae(constants.INV, 'invisible')
        ae(constants.UAE, 'update_at_end')
        ae(constants.ORD, 'ordered')
        ae(constants.ONM, 'ordnum')
        ae(constants.SPT, 'splitpct')
        ae(constants.BSIZE, 'bsize')
        ae(constants.BRECPPG, 'brecppg')
        ae(constants.BRESERVE, 'breserve')
        ae(constants.BREUSE, 'breuse')
        ae(constants.DSIZE, 'dsize')
        ae(constants.DRESERVE, 'dreserve')
        ae(constants.DPGSRES, 'dpgsres')
        ae(constants.FILEORG, 'fileorg')
        ae(constants.DEFAULT, -1)
        ae(constants.EO, 0)
        ae(constants.RRN, 36)
        ae(constants.SUPPORTED_FILEORGS, (0, 36))
        ae(constants.MANDATORY_FILEATTS, {
            'bsize': (int, type(None)),
            'brecppg': int,
            'dsize': (int, type(None)),
            'fileorg': int,
            })
        ae(constants.SECONDARY_FIELDATTS, {
            'float': False,
            'invisible': True,
            'update_at_end': False,
            'ordered': True,
            'ordnum': False,
            'splitpct': 50,
            'access_method': 'btree', # HASH is the other supported value.
            })
        ae(constants.PRIMARY_FIELDATTS, {
            'float': False,
            'invisible': False,
            'update_at_end': False,
            'ordered': False,
            'ordnum': False,
            'splitpct': 50,
            'access_method': 'recno', # Only supported value.
            })
        ae(constants.DB_FIELDATTS, {'access_method'})
        ae(constants.DPT_FIELDATTS,
           {'float', 'invisible', 'update_at_end',
            'ordered', 'ordnum', 'splitpct'})
        ae(constants.SQLITE3_FIELDATTS, {'float'})
        ae(constants.FILEATTS, {
            'bsize': None,
            'brecppg': None,
            'breserve': -1,
            'breuse': -1,
            'dsize': None,
            'dreserve': -1,
            'dpgsres': -1,
            'fileorg': None,
            })
        ae(constants.DDNAME, 'ddname')
        ae(constants.FILE, 'file')
        ae(constants.FILEDESC, 'filedesc')
        ae(constants.FOLDER, 'folder')
        ae(constants.FIELDS, 'fields')
        ae(constants.PRIMARY, 'primary')
        ae(constants.SECONDARY, 'secondary')
        ae(constants.DPT_DEFER_FOLDER, 'dptdefer')
        ae(constants.DB_DEFER_FOLDER, 'dbdefer')
        ae(constants.SECONDARY_FOLDER, 'dbsecondary')
        ae(constants.DPT_DU_SEQNUM, 'Seqnum')
        ae(constants.DPT_SYS_FOLDER, 'dptsys')
        ae(constants.DPT_SYSDU_FOLDER, 'dptsysdu')
        ae(constants.TAPEA, 'TAPEA')
        ae(constants.TAPEN, 'TAPEN')
        ae(constants.DEFER, 'defer')
        ae(constants.USERECORDIDENTITY, 'userecordidentity')
        ae(constants.RECORDIDENTITY, 'RecordIdentity')
        ae(constants.RECORDIDENTITYINVISIBLE, 'RecordIdentityInvisible')
        ae(constants.IDENTITY, 'identity')
        ae(constants.BTOD_FACTOR, 'btod_factor')
        ae(constants.BTOD_CONSTANT, 'btod_constant')
        ae(constants.DEFAULT_RECORDS, 'default_records')
        ae(constants.DEFAULT_INCREASE_FACTOR, 'default_increase_factor')
        ae(constants.TABLE_B_SIZE, 8160)
        ae(constants.DEFAULT_INITIAL_NUMBER_OF_RECORDS, 200)
        ae(constants.INDEXPREFIX, 'ix')
        ae(constants.SEGMENTPREFIX, 'sg')
        ae(constants.TABLEPREFIX, 't')
        ae(constants.DPT_PRIMARY_FIELD_LENGTH, 'dptprimaryfieldlength')
        ae(constants.SAFE_DPT_FIELD_LENGTH, 63)
        ae(constants.DPT_PATTERN_CHARS,
           {c: ''.join(('!', c)) for c in '*+!#/,)(/-='})
        ae(constants.LENGTH_SEGMENT_BITARRAY_REFERENCE, 11)
        ae(constants.LENGTH_SEGMENT_LIST_REFERENCE, 10)
        ae(constants.LENGTH_SEGMENT_RECORD_REFERENCE, 6)
        ae(constants.SUBFILE_DELIMITER, '_')
        ae(len([d for d in dir(constants) if not d.endswith('__')]), 75)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(ConstantsFunctions))
