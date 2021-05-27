# dptapi_test.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""dptapi tests using a realistic FileSpec and the real methods defined in the
class hierarchy.  The delete_instance, edit_instance, and put_instance, tests
take a few minutes because each needs a few tens of thousands of records to see
the effects tested.
"""

import unittest
import os
import shutil

import dptdb

from ..api.test._filespec import (
    samplefilespec,
    SampleValue,
    SampleRecord,
    GAMES_FILE_DEF,
    GAME_FIELD_DEF,
    EVENT_FIELD_DEF,
    SITE_FIELD_DEF,
    DATE_FIELD_DEF,
    ROUND_FIELD_DEF,
    WHITE_FIELD_DEF,
    BLACK_FIELD_DEF,
    RESULT_FIELD_DEF,
    SEVEN_TAG_ROSTER,
    PRIMARY,
    FILE,
    DDNAME,
    )
from .. import dptapi, dptbase
from ..api import filespec, record, recordset, database, constants
from . import database_interface


def api(folder):
    os.mkdir(os.path.expanduser(os.path.join('~', 'DPTapi_tests')))
    return dptapi.DPTapi(
        samplefilespec(brecppg=70, default_records=70000),
        os.path.expanduser(
            os.path.join('~', 'DPTapi_tests', folder)),
        )#sysprint='CONSOLE') # DPT messages to stdout not sysprint.txt


def _delete_folder(folder):
    shutil.rmtree(
        os.path.expanduser(os.path.join('~', 'DPTapi_tests', folder)),
        ignore_errors=True)
    os.rmdir(os.path.expanduser(os.path.join('~', 'DPTapi_tests')))


# Defined at module level for pickling.
class _Value(record.Value):
    def pack(self):
        v = super().pack()
        v[1]['Site'] = 'gash' # defined in samplefilespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in samplefilespec(), ignored
        return v


# Defined at module level for pickling.
class _ValueEdited(record.Value):
    def pack(self):
        v = super().pack()
        v[1]['Site'] = 'newgash' # defined in samplefilespec()
        v[1]['hhhhhh'] = 'hhhh' # not defined in samplefilespec(), ignored
        return v


class _DatabaseEncoders(unittest.TestCase):

    def setUp(self):
        self.sde = dptbase._DatabaseEncoders()

    def tearDown(self):
        pass

    def test____assumptions(self):
        self.assertEqual(repr(57), '57')

    def test_encode_record_number_01(self):
        self.assertEqual(self.sde.encode_record_number(57), '57')

    def test_decode_record_number_01(self):
        self.assertRaisesRegex(
            ValueError,
            "malformed node or string: 57",
            self.sde.decode_record_number,
            57)
        self.assertEqual(self.sde.decode_record_number('57'), 57)

    def test_encode_record_selector(self):
        self.assertEqual(self.sde.encode_record_selector('57'), '57')


class DPTapi_put_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('put_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('put_instance')

    def test_put_instance_05(self):
        database_interface.test_put_instance_05(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_06(self):
        database_interface.test_put_instance_06(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_07(self):
        database_interface.test_put_instance_07(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_08(self):
        database_interface.test_put_instance_08(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_09(self):
        database_interface.test_put_instance_09(self,
                                                collect_counts,
                                                game_number_to_record_number)

    def test_put_instance_10(self):
        database_interface.test_put_instance_10(self,
                                                collect_counts,
                                                game_number_to_record_number)


class DPTapi_delete_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('delete_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('delete_instance')

    def test_delete_instance_01(self):
        database_interface.test_delete_instance_01(self,
                                                   collect_counts,
                                                   game_number_to_record_number)

    def test_delete_instance_02(self):
        database_interface.test_delete_instance_02(self,
                                                   collect_counts,
                                                   game_number_to_record_number)


class DPTapi_edit_instance(unittest.TestCase):

    def setUp(self):
        self.api = api('edit_instance')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('edit_instance')

    def test_edit_instance_06(self):
        database_interface.test_edit_instance_06(self,
                                                 collect_counts,
                                                 game_number_to_record_number)

    def test_edit_instance_07(self):
        database_interface.test_edit_instance_07(self,
                                                 collect_counts,
                                                 game_number_to_record_number)


class DPTapi_put_instance_with_holes(unittest.TestCase):

    def setUp(self):

        # DPT segment size is 65280 (8k bytes - 32 reserved) while apsw, bsddb3,
        # and sqlite3, segment size is 32768 (4k bytes).  Reuse record number
        # tests cannot produce the same answer in general on the two segment
        # sizes, even if the DPT algorithm deciding when to fill a hole is the
        # one used for apsw, bsddb3, and sqlite3.
        # The api() function calls samplefilespec with the default fileorg
        # argument causing DPT to reuse record numbers.
        self.dpt_reuse_record_numbers = True
        
        self.api = api('put_instance_holes')
        self.sr = SampleRecord()

    def tearDown(self):
        self.api.close_database()
        _delete_folder('put_instance_holes')

    def test_holes_01(self):
        database_interface.test_holes_01(self,
                                         collect_counts,
                                         game_number_to_record_number)


def game_number_to_record_number(game_number):
    # Record numbers start at 0 and game numbers start at 1
    return game_number - 1


def record_number_to_game_number(record_number):
    # Record numbers start at 0 and game numbers start at 1
    return record_number + 1


def collect_counts(testcase,
                   record_adder,
                   data_adder,
                   answers,
                   counts,
                   segments,
                   recordsets,
                   rowexceptions):
    db_directory = testcase.api.get_database_folder()
    filespec = samplefilespec()
    primaries = {filespec[t][PRIMARY]:t for t in filespec}
    for t in answers['data']:
        testcase.assertEqual(t in primaries, True)
        ft = filespec[primaries[t]]
        dptfile = testcase.api.database_definition[primaries[t]]
        context_spec = dptdb.dptapi.APIContextSpecification(ft[DDNAME])
        dbserv = testcase.api.dbservices
        dbserv.Allocate(ft[DDNAME],
                        os.path.join(db_directory, ft[FILE]),
                        dptdb.dptapi.FILEDISP_OLD)
        context = dbserv.OpenContext(context_spec)
        allrecords = context.FindRecords(
            dptdb.dptapi.APIFindSpecification(
                ft[FILE],
                dptdb.dptapi.FD_ALLRECS,
                dptdb.dptapi.APIFieldValue('')))
        fac = context.OpenFieldAttCursor()
        fields = set()
        while fac.Accessible():
            n = fac.Name()
            fields.add(fac.Name())
            fac.Advance()
        context.CloseFieldAttCursor(fac)
        testcase.assertEqual(fields,
                             {f for f in answers['record_counts'].keys()})
        segments[t] = dict()
        rowexceptions[t] = dict()
        rsc = allrecords.OpenCursor()
        while rsc.Accessible():
            r = rsc.AccessCurrentRecordForRead()
            testcase.assertNotEqual(r.CountOccurrences(t), 0)
            v = []
            while r.AdvanceToNextFVPair():
                if r.LastAdvancedFieldName() == t:
                    v.append(r.LastAdvancedFieldValue().ExtractString())
            data_adder(rowexceptions[t],
                       segments[t],
                       (rsc.LastAdvancedRecNum(), ''.join(v)),
                       answers['data'][t]['defaultrow'],
                       record_number_to_game_number)
            rsc.Advance()
        allrecords.CloseCursor(rsc)
        counts[t] = allrecords.Count()
        fields.remove(t)
        for f in fields:
            if f not in answers['records']:
                continue
            segments[f] = dict()
            recordsets[f] = dict()
            dvc = context.OpenDirectValueCursor(
                dptdb.dptapi.APIFindValuesSpecification(f))
            fieldrecords = context.CreateRecordList()
            while dvc.Accessible():
                v = dvc.GetCurrentValue()
                fvrecords = context.FindRecords(
                    dptdb.dptapi.APIFindSpecification(
                        f,
                        dptdb.dptapi.FD_EQ,
                        v),
                    ) # not constrained by allrecords
                fieldrecords.Place(fvrecords)
                rsc = fvrecords.OpenCursor()
                while rsc.Accessible():
                    record_adder(recordsets[f],
                                 segments[f],
                                 (v.ExtractString(), rsc.LastAdvancedRecNum()),
                                 record_number_to_game_number)
                    rsc.Advance()
                fvrecords.CloseCursor(rsc)
                dvc.Advance()
            context.CloseDirectValueCursor(dvc)
            counts[f] = fieldrecords.Count()
            if f in answers['records']:
                testcase.assertEqual(fieldrecords.Count(), counts[t])
            else:
                testcase.assertEqual(fieldrecords.Count(), 0)
        context.DestroyAllRecordSets()
        dbserv.CloseContext(context)
        dbserv.Free(dptfile._ddname)


if __name__ == '__main__':
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    
    runner().run(loader(_DatabaseEncoders))

    runner().run(loader(DPTapi_put_instance))
    runner().run(loader(DPTapi_delete_instance))
    runner().run(loader(DPTapi_edit_instance))

    # Some important sequences
    
    runner().run(loader(DPTapi_put_instance_with_holes))
