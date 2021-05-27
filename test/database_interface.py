# database_interface.py
# Copyright 2017 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Verify sequences of record insertions and deletions, starting with an empty
database, leave the same records in existence for each of dbapi, dptapi,
sqlt3api, and apswapi.  Adjustments are made because dptapi record numbers
start at 0 while these start at 1 for the others.  Some tests create tens of
thousands of records to test the behaviour when more than one segment exists.
"""

from ast import literal_eval


from ..api.test._filespec import (
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
    )
from ..api.segmentsize import SegmentSize


def record_adder(currec, curseg, r, record_number_to_game_number):
    # Callback to increment counters for index records
    rk, rv = r
    rv = record_number_to_game_number(rv)
    try:
        currec[rk].add(rv)
    except:
        currec[rk] = {rv}
    seg, srn = divmod(rv, SegmentSize.db_segment_size)
    try:
        curseg[seg].add(srn)
    except KeyError:
        curseg[seg] = {srn}


def data_adder(currow, curseg, r, defrow, record_number_to_game_number):
    # Callback to increment counters for data record
    rk, rv = r
    rk = record_number_to_game_number(rk)
    rv = literal_eval(rv)
    if rv != defrow:
        currow[rk] = rv
    seg, srn = divmod(rk, SegmentSize.db_segment_size)
    try:
        curseg[seg].add(srn)
    except KeyError:
        curseg[seg] = {srn}


def record_counts(test, collect_counts, answers):
    test.assertEqual(
        bool(set(answers['records']) - set(answers['record_counts'])),
        False)
    counts = dict()
    segments = dict()
    recordsets = dict()
    rowexceptions = dict()

    # Call database engine specific record loop.
    collect_counts(test,
                   record_adder,   # callback for index records
                   data_adder,     # callback for data records
                   answers,
                   counts,
                   segments,
                   recordsets,
                   rowexceptions)

    # Database records must be those expected.
    indicies = dict()
    for k, v in answers['records'].items():
        indicies[k] = dict()
        index = indicies[k]
        for n, s, c, r in v:
            if n not in index:
                index[n] = [c, r[s]]
            else:
                index[n][0] += c
                index[n][1] += r[s]
    for k in counts:
        test.assertEqual(counts[k], answers['record_counts'][k])
    for k in rowexceptions:
        test.assertEqual(rowexceptions[k], answers['data'][k]['rows'])
    test.assertEqual(len(answers['segments']), len(segments))
    for k, v in answers['segments'].items():
        test.assertTrue(k in segments)
        segv = segments[k]
        test.assertEqual(len(v), len(segv))
        for sk, sv in enumerate(v):
            test.assertTrue(sk in segv)
            records = segv[sk]
            bitcount = 0
            for e, b in enumerate(sv):
                for eb, bitvalue in enumerate((128, 64, 32, 16, 8, 4, 2, 1)):
                    if bitvalue & b:
                        test.assertEqual(eb+e*8 in records, True,
                                             msg=' '.join(
                                                 (str(eb+e*8),
                                                  str(sk),
                                                  str(eb),
                                                  str(e),)))
                        bitcount += 1
            test.assertEqual(bitcount, len(records))
    test.assertEqual(len(indicies), len(recordsets))
    for k, v in indicies.items():
        test.assertTrue(k in recordsets)
        vr = recordsets[k]
        for ik, iv in v.items():
            test.assertTrue(ik in vr)
            records = vr[ik]
            bitcount = 0
            for e, b in enumerate(iv[1]):
                for eb, bitvalue in enumerate((128, 64, 32, 16, 8, 4, 2, 1)):
                    if bitvalue & b:
                        test.assertEqual(eb+e*8 in records, True,
                                             msg=' '.join(
                                                 (str(eb+e*8),
                                                  str(ik),
                                                  str(eb),
                                                  str(e),)))
                        bitcount += 1
            test.assertEqual(bitcount, len(records))


def test_put_instance_05(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    test.sr.load_record((None, repr(value)))
    test.api.start_transaction()
    test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x40',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=1,
                                Game=1,
                                Event=0,
                                Site=0,
                                Black=1,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             # (1,) is the record number.
             records=dict(White=(('A Jones', 0, 1, records),),
                          Black=(('B Smith', 0, 1, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_put_instance_06(test, collect_counts, game_number_to_record_number):
    # commit inside record add loop
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    for i in range(2):
        test.sr.load_record((None, repr(value)))
        test.api.start_transaction()
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
        test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x60',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=2,
                                Game=2,
                                Event=0,
                                Site=0,
                                Black=2,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 2, records),),
                          Black=(('B Smith', 0, 2, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_put_instance_07(test, collect_counts, game_number_to_record_number):
    # commit inside record add loop
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    for i in range(3):
        test.sr.load_record((None, repr(value)))
        test.api.start_transaction()
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
        test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x70',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=3,
                                Game=3,
                                Event=0,
                                Site=0,
                                Black=3,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 3, records),),
                          Black=(('B Smith', 0, 3, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_put_instance_08(test, collect_counts, game_number_to_record_number):
    # commit inside record add loop
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    for i in range(200):
        test.sr.load_record((None, repr(value)))
        test.api.start_transaction()
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
        test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*24,
                         b'\x80',
                         b'\x00'*4070,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=200,
                                Game=200,
                                Event=0,
                                Site=0,
                                Black=200,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 200, records),),
                          Black=(('B Smith', 0, 200, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_put_instance_09(test, collect_counts, game_number_to_record_number):
    # commit after record add loop
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    test.api.start_transaction()
    for i in range(2000):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*249,
                         b'\x80',
                         b'\x00'*3845,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=2000,
                                Game=2000,
                                Event=0,
                                Site=0,
                                Black=2000,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 2000, records),),
                          Black=(('B Smith', 0, 2000, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_put_instance_10(test, collect_counts, game_number_to_record_number):
    # one full and one partially filled segment
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    test.api.open_context()
    test.api.start_transaction()
    for i in range(33000):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*4095,
                         )),
               b''.join((b'\xff'*29,
                         b'\x80',
                         b'\x00'*4066,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=33000,
                                Game=33000,
                                Event=0,
                                Site=0,
                                Black=33000,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32767, records),
                                 ('A Jones', 1, 233, records),
                                 ),
                          Black=(('B Smith', 0, 32767, records),
                                 ('B Smith', 1, 233, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_delete_instance_01(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    # Do a put_instance test to establish starting point.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(3):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x70',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=3,
                                Game=3,
                                Event=0,
                                Site=0,
                                Black=3,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 3, records),),
                          Black=(('B Smith', 0, 3, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Do the delete_instance test.
    # Delete game number 2
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(2)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x50',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=2,
                                Game=2,
                                Event=0,
                                Site=0,
                                Black=2,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 2, records),),
                          Black=(('B Smith', 0, 2, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Delete game number 3
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(3)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x40',
                         b'\x00'*4095,
                         )),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=1,
                                Game=1,
                                Event=0,
                                Site=0,
                                Black=1,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 1, records),),
                          Black=(('B Smith', 0, 1, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_delete_instance_02(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    # Do a put_instance test to establish starting point.
    # range(SegmentSize.db_segment_size + DB_UPPER_CONVERSION_LIMIT).
    test.api.open_context()
    test.api.start_transaction()
    for i in range(32768+2000):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*4095,
                         )),
               b''.join((b'\xff'*250,
                         b'\x80',
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34768,
                                Game=34768,
                                Event=0,
                                Site=0,
                                Black=34768,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32767, records),
                                 ('A Jones', 1, 2001, records),
                                 ),
                          Black=(('B Smith', 0, 32767, records),
                                 ('B Smith', 1, 2001, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Do the delete_instance test.
    # Delete game number 58
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(58)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*250,
                         b'\x80',
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34767,
                                Game=34767,
                                Event=0,
                                Site=0,
                                Black=34767,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 2001, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 2001, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Delete game number 33000
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(33000)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*29,
                         b'\x7f',
                         b'\xff'*220,
                         b'\x80',
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34766,
                                Game=34766,
                                Event=0,
                                Site=0,
                                Black=34766,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 2000, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 2000, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Delete game numbers 33008 to 33056
    test.api.open_context()
    test.api.start_transaction()
    for g in range(33008, 33057):
        test.sr.load_record(test.api.get_primary_record(
            GAMES_FILE_DEF, game_number_to_record_number(g)))
        test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*29,
                         b'\x7f',
                         b'\x00'*6,
                         b'\x7f',
                         b'\xff'*213,
                         b'\x80',
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34717,
                                Game=34717,
                                Event=0,
                                Site=0,
                                Black=34717,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 1951, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 1951, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_edit_instance_06(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    editvalue = {WHITE_FIELD_DEF:'C Brown',
                 BLACK_FIELD_DEF:'D Grey',
                 }
    # set starting point with 3 records.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(3):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x70',
                         b'\x00'*4095)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=3,
                                Game=3,
                                Event=0,
                                Site=0,
                                Black=3,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 3, records),),
                          Black=(('B Smith', 0, 3, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    test.api.open_context()
    # Edit game number 2.
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(2)))
    er = test.sr.clone()
    er.value.White = editvalue[WHITE_FIELD_DEF]
    er.value.Black = editvalue[BLACK_FIELD_DEF]
    test.sr.newrecord = er
    test.api.start_transaction()
    test.api.edit_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x70',
                         b'\x00'*4095)),
               )
    records1 = (b''.join((b'\x50',
                          b'\x00'*4095)),
                )
    records2 = (b''.join((b'\x20',
                          b'\x00'*4095)),
                )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=3,
                                Game=3,
                                Event=0,
                                Site=0,
                                Black=3,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 2, records1),
                                 ('C Brown', 0, 1, records2),
                                 ),
                          Black=(('B Smith', 0, 2, records1),
                                 ('D Grey', 0, 1, records2),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows={2:dict(White='C Brown', Black='D Grey')},
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_edit_instance_07(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    editvalue = {WHITE_FIELD_DEF:'C Brown',
                 BLACK_FIELD_DEF:'D Grey',
                 }
    # set starting point with 9 records.
    # To get lists of record numbers changed by third edit.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(9):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xc0',
                         b'\x00'*4094)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=9,
                                Game=9,
                                Event=0,
                                Site=0,
                                Black=9,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 9, records),),
                          Black=(('B Smith', 0, 9, records),),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    test.api.open_context()
    # Edit game numbers 2, 5, 6.
    test.api.start_transaction()
    for i in (2, 5, 6):
        test.sr.load_record(test.api.get_primary_record(
            GAMES_FILE_DEF, game_number_to_record_number(i)))
        er = test.sr.clone()
        er.value.White = editvalue[WHITE_FIELD_DEF]
        er.value.Black = editvalue[BLACK_FIELD_DEF]
        test.sr.newrecord = er
        test.api.edit_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xc0',
                         b'\x00'*4094)),
               )
    records1 = (b''.join((b'\x59',
                          b'\xc0',
                          b'\x00'*4094)),
                )
    records2 = (b''.join((b'\x26',
                          b'\x00',
                          b'\x00'*4094)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=9,
                                Game=9,
                                Event=0,
                                Site=0,
                                Black=9,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 6, records1),
                                 ('C Brown', 0, 3, records2),
                                 ),
                          Black=(('B Smith', 0, 6, records1),
                                 ('D Grey', 0, 3, records2),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows={k:dict(White='C Brown', Black='D Grey')
                           for k in (2, 5, 6)},
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))


def test_holes_01(test, collect_counts, game_number_to_record_number):
    value = {WHITE_FIELD_DEF:'A Jones',
             BLACK_FIELD_DEF:'B Smith',
             }
    # set starting point like end of test_delete_instance_02
    test.api.open_context()
    test.api.start_transaction()
    for i in range(32768+2000):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(58)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(33000)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    test.api.open_context()
    test.api.start_transaction()
    for g in range(33008, 33057):
        test.sr.load_record(test.api.get_primary_record(
            GAMES_FILE_DEF, game_number_to_record_number(g)))
        test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    test.api.open_context()
    test.api.start_transaction()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(32990)))
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*27,
                         b'\xfd',
                         b'\xff',
                         b'\x7f',
                         b'\x00'*6,
                         b'\x7f',
                         b'\xff'*213,
                         b'\x80',
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34716,
                                Game=34716,
                                Event=0,
                                Site=0,
                                Black=34716,
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 1950, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 1950, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(1):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*27,
                         b'\xfd',
                         b'\xff',
                         b'\x7f',
                         b'\x00'*6,
                         b'\x7f',
                         b'\xff'*213,
                         b'\xc0', # +1
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34717, # +1
                                Game=34717, # +1
                                Event=0,
                                Site=0,
                                Black=34717, # +1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 1951, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 1951, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # First just one record.
    test.api.open_context()
    test.sr.load_record((None, repr(value)))
    test.api.start_transaction()
    test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    records = (b''.join((b'\x7f',
                         b'\xff'*6,
                         b'\xdf',
                         b'\xff'*4088)),
               b''.join((b'\xff'*27,
                         b'\xfd',
                         b'\xff',
                         b'\x7f',
                         b'\x00'*6,
                         b'\x7f',
                         b'\xff'*213,
                         b'\xe0', # +1
                         b'\x00'*3845)),
               )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34718, # +1
                                Game=34718, # +1
                                Event=0,
                                Site=0,
                                Black=34718, # +1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 1952, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 1952, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # Second many records until next record would switch lists to bitmaps.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(1952, 2001):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*31,
                             b'\xe0',
                             b'\x00'*2,
                             b'\x7f',
                             b'\xff'*217,
                             b'\xfe',
                             b'\x00'*3843)),
                   )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff', # +5
                             b'\xff'*5, # +40
                             b'\xf0', # +4
                             b'\x00'*3839)),
                   )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34767, # +49
                                Game=34767, # +49
                                Event=0,
                                Site=0,
                                Black=34767, # +49
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 2001, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 2001, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # Third one record which would switch lists to bitmaps.
    test.api.open_context()
    test.sr.load_record((None, repr(value)))
    test.api.start_transaction()
    test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*31,
                             b'\xf0',
                             b'\x00'*2,
                             b'\x7f',
                             b'\xff'*217,
                             b'\xfe',
                             b'\x00'*3843)),
                   )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff',
                             b'\xff'*5,
                             b'\xf8', # +1
                             b'\x00'*3839)),
                   )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=34768, # +1
                                Game=34768, # +1
                                Event=0,
                                Site=0,
                                Black=34768, # +1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=dict(White=(('A Jones', 0, 32766, records),
                                 ('A Jones', 1, 2001, records),
                                 ),
                          Black=(('B Smith', 0, 32766, records),
                                 ('B Smith', 1, 2001, records),
                                 ),
                          ),
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # Fourth add records to end of segment 1 and start segment 2.
    test.api.open_context()
    test.api.start_transaction()
    for i in range(2052, 32768):
        test.sr.load_record((None, repr(value)))
        test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xff',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*4089,
                             b'\xf8',
                             b'\x00'*6)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ),
                     )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff',
                             b'\xff'*5,
                             b'\xff', # +3
                             b'\xff'*3839)), # +30712
                   b''.join((b'\x80', # +1
                             b'\x00'*4095)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ('A Jones', 2, 1, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ('B Smith', 2, 1, records),
                            ),
                     )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=65484, # +30716
                                Game=65484, # +30716
                                Event=0,
                                Site=0,
                                Black=65484, # +30716
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=color,
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # Fifth add record to segment 2.
    test.api.open_context()
    test.sr.load_record((None, repr(value)))
    test.api.start_transaction()
    test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xff',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*4089,
                             b'\xfc',
                             b'\x00'*6)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ),
                     )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff',
                             b'\xff'*5,
                             b'\xff',
                             b'\xff'*3839)),
                   b''.join((b'\xc0', # +1
                             b'\x00'*4095)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ('A Jones', 2, 2, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ('B Smith', 2, 2, records),
                            ),
                     )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=65485, # +1
                                Game=65485, # +1
                                Event=0,
                                Site=0,
                                Black=65485, # +1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=color,
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records in segment 1 using holes.
    # Sixth delete record in segment 1 to trigger filling it's holes.
    # Delete record number 40000
    test.api.open_context()
    test.sr.load_record(test.api.get_primary_record(
        GAMES_FILE_DEF, game_number_to_record_number(40000)))
    test.api.start_transaction()
    test.api.delete_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xff',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*904,
                             b'\x7f',
                             b'\xff'*3184,
                             b'\xfc',
                             b'\x00'*6)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ),
                     )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff',
                             b'\xff'*5,
                             b'\xff',
                             b'\xff'*647,
                             b'\x7f', # -1
                             b'\xff'*3191)),
                   b''.join((b'\xc0',
                             b'\x00'*4095)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32716, records),
                            ('A Jones', 2, 2, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32716, records),
                            ('B Smith', 2, 2, records),
                            ),
                     )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=65484, # -1
                                Game=65484, # -1
                                Event=0,
                                Site=0,
                                Black=65484, # -1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=color,
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
    # Add records ignoring holes.
    # Seventh add record to segment 2.
    test.api.open_context()
    test.sr.load_record((None, repr(value)))
    test.api.start_transaction()
    test.api.put_instance(GAMES_FILE_DEF, test.sr)
    test.api.commit()
    test.api.close_context()
    # Confirm database state.
    if test.dpt_reuse_record_numbers:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xff',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*904,
                             b'\x7f',
                             b'\xff'*3184,
                             b'\xfe',
                             b'\x00'*6)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32717, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32717, records),
                            ),
                     )
    else:
        records = (b''.join((b'\x7f',
                             b'\xff'*6,
                             b'\xdf',
                             b'\xff'*4088)),
                   b''.join((b'\xff'*27,
                             b'\xfd',
                             b'\xff',
                             b'\x7f',
                             b'\x00'*6,
                             b'\x7f',
                             b'\xff'*213,
                             b'\xff',
                             b'\xff'*5,
                             b'\xff',
                             b'\xff'*647,
                             b'\x7f',
                             b'\xff'*3191)),
                   b''.join((b'\xe0', # +1
                             b'\x00'*4095)),
                   )
        color = dict(White=(('A Jones', 0, 32767, records),
                            ('A Jones', 1, 32716, records),
                            ('A Jones', 2, 3, records),
                            ),
                     Black=(('B Smith', 0, 32767, records),
                            ('B Smith', 1, 32716, records),
                            ('B Smith', 2, 3, records),
                            ),
                     )
    record_counts(
        test,
        collect_counts,
        dict(record_counts=dict(Round=0,
                                White=65485, # +1
                                Game=65485, # +1
                                Event=0,
                                Site=0,
                                Black=65485, # +1
                                Date=0,
                                Result=0,
                                Name=0,
                                ),
             records=color,
             segments=dict(White=records,
                           Game=records,
                           Black=records,
                           ),
             data=dict(
                 Game=dict(
                     rows=dict(),
                     defaultrow=dict(White='A Jones', Black='B Smith')),
                 ),
             ))
