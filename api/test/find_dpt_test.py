# find_dpt_test.py
# Copyright 2016 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""find_dpt tests"""

import unittest
from copy import copy, deepcopy
import os
from ast import literal_eval
import sys

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
                        createsampledatabase,
                        SampleNameValue,
                        SampleNameRecord,
                        TESTDATA,
                        TESTINDEX,
                        )

# Use the first database engine found in order:
# DPT.
try:
    from ._filespec import DPTDatabase as DatabaseEngine
except ImportError as exc:
    if str(exc) == "cannot import name 'DPTDatabase'":
        print('\n\nThis test should be run only on win32 systems.\n\n')
    sys.exit()

from .. import where_dpt
from .. import find_dpt

# DPT record numbers are based at 0.
RECNUMBASE = 0


class FindTC(unittest.TestCase):

    def setUp(self):
        self.findtest_database = os.path.expanduser(
            os.path.join('~', '_findtest_database'))
        self.filespec = samplefilespec()
        self.findtest_record = SampleNameRecord()
        # Usually data[index] = function(data[GAME_FIELD_DEF]) where index is
        # one of the *_FIELD_DEF items.
        # For testing just set some arbitrary plausible values.
        self.reference_whereclause = where_dpt.WhereClause

    def tearDown(self):
        pass

    def check_index(self, database, field, expected_records):
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

    def _wc(self, context, method, recnums, **kw):
        wc = where_dpt.WhereClause()
        for k in kw:
            if hasattr(wc, k):
                setattr(wc, k, kw[k])
        rs = method(wc)
        rns = set()
        c = rs.OpenCursor()
        while c.Accessible():
            rns.add(c.LastAdvancedRecNum())
            c.Advance()
        rs.CloseCursor(c)
        context.DestroyRecordSet(rs)
        self.assertEqual(rns, recnums)

    def _wcni(self, finder, context, method, recnums, **kw):
        wc = where_dpt.WhereClause()
        for k in kw:
            if hasattr(wc, k):
                setattr(wc, k, kw[k])
        finder.initialize_answer(wc)
        c = finder._db.database_cursor(finder._dbset, finder._dbset)
        nr = SampleNameRecord()
        while True:
            r = c.next()
            if r is None:
                break
            nr.load_record(r)
            method(wc, r[0], nr)
        rs = wc.result.answer
        rns = set()
        c = rs.OpenCursor()
        while c.Accessible():
            rns.add(c.LastAdvancedRecNum())
            c.Advance()
        rs.CloseCursor(c)
        context.DestroyRecordSet(rs)
        self.assertEqual(rns, recnums)

    def _wcop(self, finder, context, method, recnums):
        wcl = where_dpt.WhereClause()
        wcl.field = 'Black'
        wcl.value = 'blackdata0'
        wcl.result = where_dpt.WhereResult()
        wcl.result.answer = finder._eq(wcl)
        wc = where_dpt.WhereClause()
        wc.field = 'Result'
        wc.value = 'result0004'
        wc.result = where_dpt.WhereResult()
        wc.result.answer = finder._eq(wc)
        wc.left = wcl
        rs = method(wc)
        rns = set()
        c = rs.OpenCursor()
        while c.Accessible():
            rns.add(c.LastAdvancedRecNum())
            c.Advance()
        rs.CloseCursor(c)
        context.DestroyRecordSet(rs)
        self.assertEqual(rns, recnums)

    def _wcopres(self, finder, context, method, recnums):
        wcl = where_dpt.WhereClause()
        wcl.field = 'Black'
        wcl.value = 'blackdata0'
        wcl.result = where_dpt.WhereResult()
        wcl.result.answer = finder._eq(wcl)
        wc = where_dpt.WhereClause()
        wc.field = 'Result'
        wc.value = 'result0004'
        wc.result = where_dpt.WhereResult()
        wc.result.answer = finder._eq(wc)
        wc.left = wcl
        wc.operator = method
        finder.operator(wc)
        self.assertIs(wc.result, wc.left.result)
        rs = wc.left.result.answer
        rns = set()
        c = rs.OpenCursor()
        while c.Accessible():
            rns.add(c.LastAdvancedRecNum())
            c.Advance()
        rs.CloseCursor(c)
        context.DestroyRecordSet(rs)
        self.assertEqual(rns, recnums)

    def test___raises(self):
        """"""
        pass

    def test___copy(self):
        """"""
        pass

    def test____assumptions(self):
        """"""
        msg = 'Failure of this test invalidates all other tests'
        self.assertEqual(GAMES_FILE_DEF, 'games')
        self.assertEqual(GAME_FIELD_DEF, 'Game')
        self.assertEqual(EVENT_FIELD_DEF, 'Event')
        self.assertEqual(SITE_FIELD_DEF, 'Site')
        self.assertEqual(DATE_FIELD_DEF, 'Date')
        self.assertEqual(ROUND_FIELD_DEF, 'Round')
        self.assertEqual(WHITE_FIELD_DEF, 'White')
        self.assertEqual(BLACK_FIELD_DEF, 'Black')
        self.assertEqual(RESULT_FIELD_DEF, 'Result')
        self.assertEqual(SEVEN_TAG_ROSTER,
                         (EVENT_FIELD_DEF,
                          SITE_FIELD_DEF,
                          DATE_FIELD_DEF,
                          ROUND_FIELD_DEF,
                          WHITE_FIELD_DEF,
                          BLACK_FIELD_DEF,
                          RESULT_FIELD_DEF,
                          ))
        self.assertEqual(NAME_FIELD_DEF, 'Name')
        self.assertEqual(TESTDATA,
                         ({GAME_FIELD_DEF:'gamedata1',
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
                          ))
        self.assertEqual(TESTINDEX,
                         {GAME_FIELD_DEF:'game',
                          BLACK_FIELD_DEF:'black',
                          RESULT_FIELD_DEF:'result',
                          },
                         )
        ftd = DatabaseEngine(self.findtest_database)
        try:
            if ftd.open_context():
                createsampledatabase(ftd)
                self.check_index(
                    ftd,
                    RESULT_FIELD_DEF,
                    [('result0004', 3 + RECNUMBASE),
                     ('result0004', 4 + RECNUMBASE),
                     ('result0004', 5 + RECNUMBASE),
                     ('result0004', 6 + RECNUMBASE),
                     ('result0004', 7 + RECNUMBASE),
                     ('result0004', 8 + RECNUMBASE),
                     ('result0004', 9 + RECNUMBASE),
                     ('result0004', 10 + RECNUMBASE),
                     ('result0004', 11 + RECNUMBASE),
                     ('result0004', 12 + RECNUMBASE),
                     ('resultdata0', 0 + RECNUMBASE),
                     ('resultdata0', 1 + RECNUMBASE),
                     ('resultdata0', 2 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    BLACK_FIELD_DEF,
                    [('black0004', 3 + RECNUMBASE),
                     ('black0004', 4 + RECNUMBASE),
                     ('black0004', 5 + RECNUMBASE),
                     ('black0004', 6 + RECNUMBASE),
                     ('black0004', 7 + RECNUMBASE),
                     ('black0004', 8 + RECNUMBASE),
                     ('black0004', 9 + RECNUMBASE),
                     ('black0004', 10 + RECNUMBASE),
                     ('black0004', 11 + RECNUMBASE),
                     ('black0004', 12 + RECNUMBASE),
                     ('blackdata0', 0 + RECNUMBASE),
                     ('blackdata0', 1 + RECNUMBASE),
                     ('blackdata0', 2 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    EVENT_FIELD_DEF,
                    [('eventdata1', 2 + RECNUMBASE),
                     ('eventdata2', 0 + RECNUMBASE),
                     ('eventdata3', 1 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    SITE_FIELD_DEF,
                    [('sitedata1', 1 + RECNUMBASE),
                     ('sitedata2', 2 + RECNUMBASE),
                     ('sitedata3', 0 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    DATE_FIELD_DEF,
                    [('datedata1', 0 + RECNUMBASE),
                     ('datedata1', 2 + RECNUMBASE),
                     ('datedata3', 1 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    ROUND_FIELD_DEF,
                    [('rounddata1', 1 + RECNUMBASE),
                     ('rounddata2', 0 + RECNUMBASE),
                     ('rounddata2', 2 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    WHITE_FIELD_DEF,
                    [('whitedata2', 1 + RECNUMBASE),
                     ('whitedata3', 0 + RECNUMBASE),
                     ('whitedata3', 2 + RECNUMBASE),
                     ])
                self.check_index(
                    ftd,
                    NAME_FIELD_DEF,
                    [('black0004', 3 + RECNUMBASE),
                     ('black0004', 4 + RECNUMBASE),
                     ('black0004', 5 + RECNUMBASE),
                     ('black0004', 6 + RECNUMBASE),
                     ('black0004', 7 + RECNUMBASE),
                     ('black0004', 8 + RECNUMBASE),
                     ('black0004', 9 + RECNUMBASE),
                     ('black0004', 10 + RECNUMBASE),
                     ('black0004', 11 + RECNUMBASE),
                     ('black0004', 12 + RECNUMBASE),
                     ('blackdata0', 0 + RECNUMBASE),
                     ('blackdata0', 1 + RECNUMBASE),
                     ('blackdata0', 2 + RECNUMBASE),
                     ('whitedata2', 1 + RECNUMBASE),
                     ('whitedata3', 0 + RECNUMBASE),
                     ('whitedata3', 2 + RECNUMBASE),
                     ])
                self.check_table(
                    ftd,
                    [(0 + RECNUMBASE,
                      {'Event': 'eventdata2',
                       'Date': 'datedata1',
                       'Round': 'rounddata2',
                       'White': 'whitedata3',
                       'Site': 'sitedata3',
                       'Game':'gamedata1',
                       'Result':'resultdata0',
                       'Black':'blackdata0'}),
                     (1 + RECNUMBASE,
                      {'Event': 'eventdata3',
                       'Date': 'datedata3',
                       'Round': 'rounddata1',
                       'White': 'whitedata2',
                       'Site': 'sitedata1',
                       'Game':'gamedata2',
                       'Result':'resultdata0',
                       'Black':'blackdata0'}),
                     (2 + RECNUMBASE,
                      {'Event': 'eventdata1',
                       'Date': 'datedata1',
                       'Round': 'rounddata2',
                       'White': 'whitedata3',
                       'Site': 'sitedata2',
                       'Game':'gamedata3',
                       'Result':'resultdata0',
                       'Black':'blackdata0'}),
                     (3 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (4 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (5 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (6 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (7 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (8 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (9 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (10 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (11 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     (12 + RECNUMBASE,
                      {'Game':'game0004',
                       'Result':'result0004',
                       'Black':'black0004'}),
                     ])
        finally:
            ftd.delete_database()

    def test____module_constants(self):
        """"""

    def test___init__01(self):
        """"""
        with self.assertRaisesRegex(
            TypeError,
            "".join((
                "__init__\(\) missing 2 required positional arguments: ",
                "'db' and 'dbset'",
                )),
            msg="An 'AttributeError ignored in '__del__' report should occur."):
            find_dpt.Find()

    def test___init__02(self):
        """"""
        ftd = DatabaseEngine(self.findtest_database)
        f = find_dpt.Find(ftd, GAMES_FILE_DEF)
        self.assertEqual(len(f.__dict__), 6)
        self.assertEqual(f._db, ftd)
        self.assertEqual(f._dbset, 'games')
        self.assertEqual(f._recordclass, None)
        self.assertEqual(f._context, None)
        self.assertEqual(
            f.compare_field_value,
            {('is', None): f._is,
             ('like', None): f._like_by_index,
             ('starts', None): f._starts_by_index,
             ('present', None): f._present,
             ('eq', True): f._eq,
             ('ne', True): f._ne,
             ('gt', True): f._gt,
             ('lt', True): f._lt,
             ('le', True): f._le,
             ('ge', True): f._ge,
             ('before', True): f._before,
             ('after', True): f._after,
             (('from', 'to'), True): f._from_to,
             (('from', 'below'), True): f._from_below,
             (('above', 'to'), True): f._above_to,
             (('above', 'below'), True): f._above_below,
             ('eq', False): f._eq,
             ('ne', False): f._ne,
             ('gt', False): f._gt,
             ('lt', False): f._lt,
             ('le', False): f._le,
             ('ge', False): f._ge,
             ('before', False): f._before,
             ('after', False): f._after,
             (('from', 'to'), False): f._from_to,
             (('from', 'below'), False): f._from_below,
             (('above', 'to'), False): f._above_to,
             (('above', 'below'), False): f._above_below,
             })
        self.assertEqual(
            f.boolean_operation,
            {'and': f._and,
             'nor': f._nor,
             'or': f._or,
             })

    def test_conditions(self):
        """"""
        ftd = DatabaseEngine(self.findtest_database)
        try:
            ftd.open_context()
            f = find_dpt.Find(ftd, GAMES_FILE_DEF)
            createsampledatabase(ftd)
            self._wc(f._context,
                     f._is,
                     {n + RECNUMBASE for n in (0,2)},
                     field='White', value='whitedata3')
            self._wc(f._context,
                     f._eq,
                     {n + RECNUMBASE for n in (3,4,5,6,7,8,9,10,11,12)},
                     field='Black', value='black0004')
            self._wc(f._context,
                     f._gt,
                     {n + RECNUMBASE for n in (0,1,2)},
                     field='Black', value='black0004')
            self._wc(f._context,
                     f._lt,
                     {n + RECNUMBASE for n in (1,)},
                     field='White', value='whitedata3')
            self._wc(f._context,
                     f._le,
                     {n + RECNUMBASE for n in (0,1,2)},
                     field='Site', value='sitee')
            self._wc(f._context,
                     f._ge,
                     {n + RECNUMBASE for n in (0,2)},
                     field='Round', value='rounddata13')
            self._wc(f._context,
                     f._before,
                     {n + RECNUMBASE for n in (0,2)},
                     field='Event', value='eventdata3')
            self._wc(f._context,
                     f._after,
                     {n + RECNUMBASE for n in (0,1,2)},
                     field='Result', value='result0004')
            self._wc(f._context,
                     f._from_to,
                     {n + RECNUMBASE for n in (0,1,2)},
                     field='Date', value=('datedata1', 'datedata3'))
            self._wc(f._context,
                     f._from_below,
                     {n + RECNUMBASE for n in (0,2)},
                     field='Date', value=('datedata1', 'datedata3'))
            self._wc(f._context,
                     f._above_to,
                     {n + RECNUMBASE for n in (1,)},
                     field='Date', value=('datedata1', 'datedata3'))
            self._wc(f._context,
                     f._above_below,
                     {n + RECNUMBASE for n in ()},
                     field='Date', value=('datedata1', 'datedata3'))
            self._wcni(f,
                       f._context,
                       f._is_not,
                       {n + RECNUMBASE for n in (1,)},
                       field='Date', value='datedata1', not_value=True,
                       result=where_dpt.WhereResult())
            self._wcni(f,
                       f._context,
                       f._present,
                       {n + RECNUMBASE for n in (0,1,2)},
                       field='Event',
                       result=where_dpt.WhereResult())
            self._wcni(f,
                       f._context,
                       f._ne,
                       {n + RECNUMBASE for n in (0,1,2,3,4,5,6,7,8,9,10,11,12)},
                       field='Black', value='blackness',
                       result=where_dpt.WhereResult())
            self._wcni(f,
                       f._context,
                       f._like,
                       {n + RECNUMBASE for n in (3,4,5,6,7,8,9,10,11,12)},
                       field='Black', value='00',
                       result=where_dpt.WhereResult())

            # The two conditions used are defined in _wcop.
            self._wcop(f,
                       f._context,
                       f._and,
                       {n + RECNUMBASE for n in ()})
            self._wcop(f,
                       f._context,
                       f._or,
                       {n + RECNUMBASE for n in (0,1,2,3,4,5,6,7,8,9,10,11,12)})
            self._wcop(f,
                       f._context,
                       f._nor,
                       {n + RECNUMBASE for n in (0,1,2)})

            # The two conditions used are defined in _wcopres and are the ones
            # in _wcop.  The answers should be in the correct place.
            self._wcopres(
                f,
                f._context,
                'and',
                {n + RECNUMBASE for n in ()})
            self._wcopres(
                f,
                f._context,
                'or',
                {n + RECNUMBASE for n in (0,1,2,3,4,5,6,7,8,9,10,11,12)})
            self._wcopres(
                f,
                f._context,
                'nor',
                {n + RECNUMBASE for n in (0,1,2)})

        finally:
            ftd.delete_database()
        

if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    runner().run(loader(FindTC))
