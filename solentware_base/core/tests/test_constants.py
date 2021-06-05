# test_constants.py
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
        """ """
        msg = "Failure of this test invalidates all other tests"
        ae = self.assertEqual
        ae(constants.BERKELEYDB_MODULE, "berkeleydb")
        ae(constants.BSDDB3_MODULE, "bsddb3")
        ae(constants.DPT_MODULE, "dptdb.dptapi")
        ae(constants.SQLITE3_MODULE, "sqlite3")
        ae(constants.APSW_MODULE, "apsw")
        ae(constants.SQLITE_VALUE_COLUMN, "Value")
        ae(constants.SQLITE_SEGMENT_COLUMN, "Segment")
        ae(constants.SQLITE_COUNT_COLUMN, "RecordCount")
        ae(constants.SQLITE_RECORDS_COLUMN, "RecordNumbers")
        ae(constants.ACCESS_METHOD, "access_method")
        ae(constants.BTREE, "btree")
        ae(constants.HASH, "hash")
        ae(constants.RECNO, "recno")
        ae(constants.BLOB, "blob")
        ae(constants.FLT, "float")
        ae(constants.INV, "invisible")
        ae(constants.UAE, "update_at_end")
        ae(constants.ORD, "ordered")
        ae(constants.ONM, "ordnum")
        ae(constants.SPT, "splitpct")
        ae(constants.BSIZE, "bsize")
        ae(constants.BRECPPG, "brecppg")
        ae(constants.BRESERVE, "breserve")
        ae(constants.BREUSE, "breuse")
        ae(constants.DSIZE, "dsize")
        ae(constants.DRESERVE, "dreserve")
        ae(constants.DPGSRES, "dpgsres")
        ae(constants.FILEORG, "fileorg")
        ae(constants.DEFAULT, -1)
        ae(constants.EO, 0)
        ae(constants.RRN, 36)
        ae(constants.SUPPORTED_FILEORGS, (0, 36))
        ae(
            constants.MANDATORY_FILEATTS,
            {
                "bsize": (int, type(None)),
                "brecppg": int,
                "dsize": (int, type(None)),
                "fileorg": int,
            },
        )
        ae(
            constants.SECONDARY_FIELDATTS,
            {
                "float": False,
                "invisible": True,
                "update_at_end": False,
                "ordered": True,
                "ordnum": False,
                "splitpct": 50,
                "access_method": "btree",  # HASH is the other supported value.
                "branching_factor": 50,
            },
        )
        ae(
            constants.PRIMARY_FIELDATTS,
            {
                "float": False,
                "invisible": False,
                "update_at_end": False,
                "ordered": False,
                "ordnum": False,
                "splitpct": 50,
                "access_method": "recno",  # Only supported value.
            },
        )
        ae(constants.DB_FIELDATTS, {"access_method"})
        ae(
            constants.DPT_FIELDATTS,
            {
                "float",
                "invisible",
                "update_at_end",
                "ordered",
                "ordnum",
                "splitpct",
            },
        )
        ae(constants.SQLITE3_FIELDATTS, {"float"})
        ae(constants.NOSQL_FIELDATTS, {"branching_factor", "access_method"})
        ae(
            constants.FILEATTS,
            {
                "bsize": None,
                "brecppg": None,
                "breserve": -1,
                "breuse": -1,
                "dsize": None,
                "dreserve": -1,
                "dpgsres": -1,
                "fileorg": None,
            },
        )
        ae(constants.DDNAME, "ddname")
        ae(constants.FILE, "file")
        ae(constants.FILEDESC, "filedesc")
        ae(constants.FOLDER, "folder")
        ae(constants.FIELDS, "fields")
        ae(constants.PRIMARY, "primary")
        ae(constants.SECONDARY, "secondary")
        ae(constants.DPT_DEFER_FOLDER, "dptdefer")
        ae(constants.DB_DEFER_FOLDER, "dbdefer")
        ae(constants.SECONDARY_FOLDER, "dbsecondary")
        ae(constants.DPT_DU_SEQNUM, "Seqnum")
        ae(constants.DPT_SYS_FOLDER, "dptsys")
        ae(constants.DPT_SYSDU_FOLDER, "dptsysdu")
        ae(constants.TAPEA, "TAPEA")
        ae(constants.TAPEN, "TAPEN")
        ae(constants.DEFER, "defer")
        ae(constants.USERECORDIDENTITY, "userecordidentity")
        ae(constants.RECORDIDENTITY, "RecordIdentity")
        ae(constants.RECORDIDENTITYINVISIBLE, "RecordIdentityInvisible")
        ae(constants.IDENTITY, "identity")
        ae(constants.BTOD_FACTOR, "btod_factor")
        ae(constants.BTOD_CONSTANT, "btod_constant")
        ae(constants.DEFAULT_RECORDS, "default_records")
        ae(constants.DEFAULT_INCREASE_FACTOR, "default_increase_factor")
        ae(constants.TABLE_B_SIZE, 8160)
        ae(constants.DEFAULT_INITIAL_NUMBER_OF_RECORDS, 200)
        ae(constants.INDEXPREFIX, "ix")
        ae(constants.SEGMENTPREFIX, "sg")
        ae(constants.TABLEPREFIX, "t")
        ae(constants.DPT_PRIMARY_FIELD_LENGTH, "dpt_primary_field_length")
        ae(constants.SAFE_DPT_FIELD_LENGTH, 63)
        ae(
            constants.DPT_PATTERN_CHARS,
            {c: "".join(("!", c)) for c in "*+!#/,)(/-="},
        )
        ae(constants.LENGTH_SEGMENT_BITARRAY_REFERENCE, 11)
        ae(constants.LENGTH_SEGMENT_LIST_REFERENCE, 10)
        ae(constants.LENGTH_SEGMENT_RECORD_REFERENCE, 6)
        ae(constants.SEGMENT_HEADER_LENGTH, 6)
        ae(constants.SUBFILE_DELIMITER, "_")
        ae(constants.EXISTENCE_BITMAP_SUFFIX, "_ebm")
        ae(constants.SEGMENT_SUFFIX, "_segment")
        ae(constants.CONTROL_FILE, "___control")
        ae(constants.DEFAULT_SEGMENT_SIZE_BYTES, 4000)
        ae(constants.SPECIFICATION_KEY, b"_specification")
        ae(constants.SEGMENT_SIZE_BYTES_KEY, b"_segment_size_bytes")
        ae(constants.BRANCHING_FACTOR, "branching_factor")
        ae(constants.FREED_RECORD_NUMBER_SEGMENTS_SUFFIX, "_freed")
        ae(constants.UNQLITE_MODULE, "unqlite")
        ae(constants.VEDIS_MODULE, "vedis")
        ae(constants.HIGH_TREE_NODE, "_high_tree_node")
        ae(constants.SEGMENT_KEY_SUFFIX, "0")
        ae(constants.SEGMENT_VALUE_SUFFIX, "1")
        ae(constants.TREE_NODE_SUFFIX, "2")
        ae(constants.LIST_BYTES, "L")
        ae(constants.BITMAP_BYTES, "B")
        ae(constants.GNU_MODULE, "dbm.gnu")
        ae(constants.NDBM_MODULE, "dbm.ndbm")
        ae(constants.TABLE_REGISTER_KEY, b"_table_register")
        ae(constants.FIELD_REGISTER_KEY, b"_field_register")
        cc = [d for d in dir(constants) if not d.endswith("__")]
        ae(len(cc), 97)
        ae(
            sorted(cc),
            sorted(
                [
                    "BERKELEYDB_MODULE",
                    "BSDDB3_MODULE",
                    "DPT_MODULE",
                    "SQLITE3_MODULE",
                    "APSW_MODULE",
                    "GNU_MODULE",
                    "NDBM_MODULE",
                    "SQLITE_VALUE_COLUMN",
                    "SQLITE_SEGMENT_COLUMN",
                    "SQLITE_COUNT_COLUMN",
                    "SQLITE_RECORDS_COLUMN",
                    "ACCESS_METHOD",
                    "BTREE",
                    "HASH",
                    "RECNO",
                    "BLOB",
                    "FLT",
                    "INV",
                    "UAE",
                    "ORD",
                    "ONM",
                    "SPT",
                    "BSIZE",
                    "BRECPPG",
                    "BRESERVE",
                    "BREUSE",
                    "DSIZE",
                    "DRESERVE",
                    "DPGSRES",
                    "FILEORG",
                    "DEFAULT",
                    "EO",
                    "RRN",
                    "SUPPORTED_FILEORGS",
                    "MANDATORY_FILEATTS",
                    "SECONDARY_FIELDATTS",
                    "PRIMARY_FIELDATTS",
                    "DB_FIELDATTS",
                    "DPT_FIELDATTS",
                    "SQLITE3_FIELDATTS",
                    "FILEATTS",
                    "DDNAME",
                    "FILE",
                    "FILEDESC",
                    "FOLDER",
                    "FIELDS",
                    "PRIMARY",
                    "SECONDARY",
                    "DPT_DEFER_FOLDER",
                    "DB_DEFER_FOLDER",
                    "SECONDARY_FOLDER",
                    "DPT_DU_SEQNUM",
                    "DPT_SYS_FOLDER",
                    "DPT_SYSDU_FOLDER",
                    "TAPEA",
                    "TAPEN",
                    "DEFER",
                    "USERECORDIDENTITY",
                    "RECORDIDENTITY",
                    "RECORDIDENTITYINVISIBLE",
                    "IDENTITY",
                    "BTOD_FACTOR",
                    "BTOD_CONSTANT",
                    "DEFAULT_RECORDS",
                    "DEFAULT_INCREASE_FACTOR",
                    "TABLE_B_SIZE",
                    "DEFAULT_INITIAL_NUMBER_OF_RECORDS",
                    "INDEXPREFIX",
                    "SEGMENTPREFIX",
                    "TABLEPREFIX",
                    "DPT_PRIMARY_FIELD_LENGTH",
                    "SAFE_DPT_FIELD_LENGTH",
                    "DPT_PATTERN_CHARS",
                    "LENGTH_SEGMENT_BITARRAY_REFERENCE",
                    "LENGTH_SEGMENT_LIST_REFERENCE",
                    "LENGTH_SEGMENT_RECORD_REFERENCE",
                    "SEGMENT_HEADER_LENGTH",
                    "SUBFILE_DELIMITER",
                    "EXISTENCE_BITMAP_SUFFIX",
                    "SEGMENT_SUFFIX",
                    "CONTROL_FILE",
                    "DEFAULT_SEGMENT_SIZE_BYTES",
                    "SPECIFICATION_KEY",
                    "SEGMENT_SIZE_BYTES_KEY",
                    "BRANCHING_FACTOR",
                    "FREED_RECORD_NUMBER_SEGMENTS_SUFFIX",
                    "NOSQL_FIELDATTS",
                    "UNQLITE_MODULE",
                    "VEDIS_MODULE",
                    "HIGH_TREE_NODE",
                    "SEGMENT_KEY_SUFFIX",
                    "TREE_NODE_SUFFIX",
                    "SEGMENT_VALUE_SUFFIX",
                    "LIST_BYTES",
                    "BITMAP_BYTES",
                    "TABLE_REGISTER_KEY",
                    "FIELD_REGISTER_KEY",
                ]
            ),
        )


if __name__ == "__main__":
    runner = unittest.TextTestRunner
    loader = unittest.defaultTestLoader.loadTestsFromTestCase

    runner().run(loader(ConstantsFunctions))
