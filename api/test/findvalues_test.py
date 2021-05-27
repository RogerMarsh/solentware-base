# findvalues_test.py
# Copyright 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""findvalues tests"""

import unittest
import os
from ast import literal_eval
import re

from ._filespec import (GAMES_FILE_DEF,
                        GAME_FIELD_DEF,
                        EVENT_FIELD_DEF,
                        SITE_FIELD_DEF,
                        DATE_FIELD_DEF,
                        ROUND_FIELD_DEF,
                        WHITE_FIELD_DEF,
                        BLACK_FIELD_DEF,
                        RESULT_FIELD_DEF,
                        SEVEN_TAG_ROSTER,
                        NAME_FIELD_DEF,
                        samplefilespec,
                        SampleNameValue,
                        SampleNameRecord,
                        )

# Use the first database engine found in order:
# DPT, bsddb3, apsw, sqlite3.
recnumbase = 1
try:
    from ._filespec import DPTDatabase as DatabaseEngine
    recnumbase = 0
except ImportError:
    try:
        from ._filespec import Bsddb3Database as DatabaseEngine
    except ImportError:
        try:
            from ._filespec import Sqlite3apswDatabase as DatabaseEngine
        except ImportError:
            from ._filespec import Sqlite3Database as DatabaseEngine

from .. import wherevalues
from .. import findvalues


class FindValuesTC(unittest.TestCase):

    def setUp(self):
        self.findtest_database = os.path.expanduser(
            os.path.join('~', '_findtest_database'))
        self.filespec = samplefilespec()
        self.findtest_record = SampleNameRecord()
        # Usually data[index] = function(data[GAME_FIELD_DEF]) where index is
        # one of the *_FIELD_DEF items.
        # For testing just set some arbitrary plausible values.
        self.testdata = (
            {GAME_FIELD_DEF:'gamedata1',
             EVENT_FIELD_DEF:'eventdata2',
             SITE_FIELD_DEF:'sitedata3',
             DATE_FIELD_DEF:'datedata1',
             ROUND_FIELD_DEF:'rounddata2',
             WHITE_FIELD_DEF:'whitedata3',
             BLACK_FIELD_DEF:'blackdata0',
             RESULT_FIELD_DEF:'resultdata0',
             },
            {GAME_FIELD_DEF:'gamedata2',
             EVENT_FIELD_DEF:'eventdata3',
             SITE_FIELD_DEF:'sitedata1',
             DATE_FIELD_DEF:'datedata3',
             ROUND_FIELD_DEF:'rounddata1',
             WHITE_FIELD_DEF:'whitedata2',
             BLACK_FIELD_DEF:'blackdata0',
             RESULT_FIELD_DEF:'resultdata0',
             },
            {GAME_FIELD_DEF:'gamedata3',
             EVENT_FIELD_DEF:'eventdata1',
             SITE_FIELD_DEF:'sitedata2',
             DATE_FIELD_DEF:'datedata1',
             ROUND_FIELD_DEF:'rounddata2',
             WHITE_FIELD_DEF:'whitedata3',
             BLACK_FIELD_DEF:'blackdata0',
             RESULT_FIELD_DEF:'resultdata0',
             },
            )
        self.trdata = {GAME_FIELD_DEF:'game',
                       BLACK_FIELD_DEF:'black',
                       RESULT_FIELD_DEF:'result',
                       }
        self.ftd = DatabaseEngine(self.findtest_database)
        if self.ftd.open_context():
            for r in self.testdata:
                self.ftd.start_transaction()
                self.findtest_record.load_record((None, repr(r)))
                self.findtest_record.put_record(self.ftd, GAMES_FILE_DEF)
                self.ftd.commit()
            rd = {k:''.join((v, str(4).zfill(4)))
                  for k, v in self.trdata.items()}
            self.ftd.start_transaction()
            for r in range(10):
                self.findtest_record.load_record((None, repr(rd)))
                self.findtest_record.put_record(self.ftd, GAMES_FILE_DEF)
            self.ftd.commit()

    def tearDown(self):
        self.ftd.delete_database()

    def check_index(self, database, field, expected_records):
        # No need for sorting bsddb answer with bitmapped record numbers
        # because records are returned in bit number order which is same as
        # number order.
        c = database.database_cursor(GAMES_FILE_DEF, field)
        r = c.first()
        for er in expected_records:
            self.assertEqual(r, er)
            r = c.next()
        else:
            self.assertEqual(r, None)

    def check_table(self, database, expected_records):
        c = database.database_cursor(GAMES_FILE_DEF, GAMES_FILE_DEF)
        r = c.first()
        for er in expected_records:
            self.assertEqual((r[0], literal_eval(r[1])), er)
            r = c.next()
        else:
            self.assertEqual(r, None)

    def check_name_index(self,
                         expected_values,
                         above_value=None,
                         below_value=None,
                         from_value=None,
                         to_value=None,
                         like=True,
                         like_pattern=None,
                         in_=True,
                         in__set=None):
        fv = findvalues.FindValues(self.ftd, GAMES_FILE_DEF)
        vc = wherevalues.ValuesClause()
        vc.valid_phrase = True
        vc.field = NAME_FIELD_DEF
        vc.above_value=above_value
        vc.below_value=below_value
        vc.from_value=from_value
        vc.to_value=to_value
        vc.like=like
        vc.like_pattern=like_pattern
        vc.in_=in_
        vc.in__set=in__set
        vc.result = {v for v in self.ftd.get_database_instance(
            GAMES_FILE_DEF, vc.field).find_values(vc)}
        self.assertEqual(vc.result, expected_values)

    def test____assumptions(self):
        """"""
        msg = 'Failure of this test invalidates all other tests'
        ftd = self.ftd
        self.check_index(
            ftd,
            RESULT_FIELD_DEF,
            [('result0004', 3 + recnumbase),
             ('result0004', 4 + recnumbase),
             ('result0004', 5 + recnumbase),
             ('result0004', 6 + recnumbase),
             ('result0004', 7 + recnumbase),
             ('result0004', 8 + recnumbase),
             ('result0004', 9 + recnumbase),
             ('result0004', 10 + recnumbase),
             ('result0004', 11 + recnumbase),
             ('result0004', 12 + recnumbase),
             ('resultdata0', 0 + recnumbase),
             ('resultdata0', 1 + recnumbase),
             ('resultdata0', 2 + recnumbase),
                     ])
        self.check_index(
            ftd,
            BLACK_FIELD_DEF,
            [('black0004', 3 + recnumbase),
             ('black0004', 4 + recnumbase),
             ('black0004', 5 + recnumbase),
             ('black0004', 6 + recnumbase),
             ('black0004', 7 + recnumbase),
             ('black0004', 8 + recnumbase),
             ('black0004', 9 + recnumbase),
             ('black0004', 10 + recnumbase),
             ('black0004', 11 + recnumbase),
             ('black0004', 12 + recnumbase),
             ('blackdata0', 0 + recnumbase),
             ('blackdata0', 1 + recnumbase),
             ('blackdata0', 2 + recnumbase),
                     ])
        self.check_index(
            ftd,
            EVENT_FIELD_DEF,
            [('eventdata1', 2 + recnumbase),
             ('eventdata2', 0 + recnumbase),
             ('eventdata3', 1 + recnumbase),
                     ])
        self.check_index(
            ftd,
            SITE_FIELD_DEF,
            [('sitedata1', 1 + recnumbase),
             ('sitedata2', 2 + recnumbase),
             ('sitedata3', 0 + recnumbase),
             ])
        self.check_index(
            ftd,
            DATE_FIELD_DEF,
            [('datedata1', 0 + recnumbase),
             ('datedata1', 2 + recnumbase),
             ('datedata3', 1 + recnumbase),
             ])
        self.check_index(
            ftd,
            ROUND_FIELD_DEF,
            [('rounddata1', 1 + recnumbase),
             ('rounddata2', 0 + recnumbase),
             ('rounddata2', 2 + recnumbase),
             ])
        self.check_index(
            ftd,
            WHITE_FIELD_DEF,
            [('whitedata2', 1 + recnumbase),
             ('whitedata3', 0 + recnumbase),
             ('whitedata3', 2 + recnumbase),
             ])
        self.check_index(
            ftd,
            NAME_FIELD_DEF,
            [('black0004', 3 + recnumbase),
             ('black0004', 4 + recnumbase),
             ('black0004', 5 + recnumbase),
             ('black0004', 6 + recnumbase),
             ('black0004', 7 + recnumbase),
             ('black0004', 8 + recnumbase),
             ('black0004', 9 + recnumbase),
             ('black0004', 10 + recnumbase),
             ('black0004', 11 + recnumbase),
             ('black0004', 12 + recnumbase),
             ('blackdata0', 0 + recnumbase),
             ('blackdata0', 1 + recnumbase),
             ('blackdata0', 2 + recnumbase),
             ('whitedata2', 1 + recnumbase),
             ('whitedata3', 0 + recnumbase),
             ('whitedata3', 2 + recnumbase),
             ])
        self.check_table(
            ftd,
            [(0 + recnumbase,
              {'Event': 'eventdata2',
               'Date': 'datedata1',
               'Round': 'rounddata2',
               'White': 'whitedata3',
               'Site': 'sitedata3',
               'Game':'gamedata1',
               'Result':'resultdata0',
               'Black':'blackdata0'}),
             (1 + recnumbase,
              {'Event': 'eventdata3',
               'Date': 'datedata3',
               'Round': 'rounddata1',
               'White': 'whitedata2',
               'Site': 'sitedata1',
               'Game':'gamedata2',
               'Result':'resultdata0',
               'Black':'blackdata0'}),
             (2 + recnumbase,
              {'Event': 'eventdata1',
               'Date': 'datedata1',
               'Round': 'rounddata2',
               'White': 'whitedata3',
               'Site': 'sitedata2',
               'Game':'gamedata3',
               'Result':'resultdata0',
               'Black':'blackdata0'}),
             (3 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (4 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (5 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (6 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (7 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (8 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (9 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (10 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (11 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             (12 + recnumbase,
              {'Game':'game0004',
               'Result':'result0004',
               'Black':'black0004'}),
             ])

    def test___init__(self):
        """"""
        self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 2 required positional arguments: ",
                "'db' and 'dbset'",
                )),
            findvalues.FindValues,
            )
        f = findvalues.FindValues(True, False)
        self.assertEqual(len(f.__dict__), 2)
        self.assertEqual(f._db, True)
        self.assertEqual(f._dbset, False)

    def test_find_values(self):
        """"""
        like_pattern = re.compile('k')
        self.check_name_index(
            {'black0004', 'blackdata0', 'whitedata2', 'whitedata3'})
        self.check_name_index(
            {'whitedata2'},
            above_value='blackdata0',
            below_value='whitedata3')
        self.check_name_index(
            {'whitedata2', 'whitedata3'},
            above_value='blackdata0',
            to_value='whitedata3')
        self.check_name_index(
            {'whitedata2', 'blackdata0'},
            from_value='blackdata0',
            below_value='whitedata3')
        self.check_name_index(
            {'whitedata2', 'blackdata0', 'whitedata3'},
            from_value='blackdata0',
            to_value='whitedata3')
        self.check_name_index(
            {'whitedata2', 'whitedata3'},
            above_value='blackdata0')
        self.check_name_index(
            {'blackdata0', 'black0004'},
            below_value='whitedata2')
        self.check_name_index(
            {'whitedata3'},
            from_value='whitedata3')
        self.check_name_index(
            {'whitedata2', 'black0004', 'blackdata0'},
            to_value='whitedata2')
        self.check_name_index(
            set(),
            in__set={})
        self.check_name_index(
            set(),
            above_value='blackdata0',
            below_value='whitedata3',
            in__set={})
        self.check_name_index(
            set(),
            above_value='blackdata0',
            to_value='whitedata3',
            in__set={})
        self.check_name_index(
            set(),
            from_value='blackdata0',
            below_value='whitedata3',
            in__set={})
        self.check_name_index(
            set(),
            from_value='blackdata0',
            to_value='whitedata3',
            in__set={})
        self.check_name_index(
            set(),
            above_value='blackdata0',
            in__set={})
        self.check_name_index(
            set(),
            below_value='whitedata2',
            in__set={})
        self.check_name_index(
            set(),
            from_value='whitedata3',
            in__set={})
        self.check_name_index(
            set(),
            to_value='whitedata2',
            in__set={})
        self.check_name_index(
            {'black0004', 'blackdata0', 'whitedata3'},
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            set(),
            above_value='blackdata0',
            below_value='whitedata3',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'whitedata3'},
            above_value='blackdata0',
            to_value='whitedata3',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'blackdata0'},
            from_value='blackdata0',
            below_value='whitedata3',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'blackdata0', 'whitedata3'},
            from_value='blackdata0',
            to_value='whitedata3',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'whitedata3'},
            above_value='blackdata0',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'blackdata0', 'black0004'},
            below_value='whitedata2',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'whitedata3'},
            from_value='whitedata3',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'black0004', 'blackdata0'},
            to_value='whitedata2',
            in_=False,
            in__set={'whitedata2'})
        self.check_name_index(
            {'black0004', 'blackdata0'},
            like_pattern=like_pattern)
        self.check_name_index(
            {'whitedata2', 'whitedata3'},
            like=False,
            like_pattern=like_pattern)
        self.check_name_index(
            {'black0004', 'blackdata0'},
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            set(),
            above_value='blackdata0',
            below_value='whitedata3',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            set(),
            above_value='blackdata0',
            to_value='whitedata3',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            {'blackdata0'},
            from_value='blackdata0',
            below_value='whitedata3',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            {'blackdata0'},
            from_value='blackdata0',
            to_value='whitedata3',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            set(),
            above_value='blackdata0',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            {'blackdata0', 'black0004'},
            below_value='whitedata2',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            set(),
            from_value='whitedata3',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        self.check_name_index(
            {'black0004', 'blackdata0'},
            to_value='whitedata2',
            in_=False,
            in__set={'whitedata2'},
            like_pattern=like_pattern)
        

if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(FindValuesTC))
