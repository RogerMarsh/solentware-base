# constants.py
# Copyright (c) 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Constants used defining and accessing databases via supported engines.

See www.sleepycat.com for details of Berkeley DB
See www.sqlite3.com for details of sqlite3
See www.dptoolkit.com for details of DPT (No longer exists.  The database
interface part of DPT is available at www.solentware.co.uk)

"""
# Module names of supported database engines (<module>.__name__).
BERKELEYDB_MODULE = "berkeleydb"
BSDDB3_MODULE = "bsddb3"
DPT_MODULE = "dptdb.dptapi"
SQLITE3_MODULE = "sqlite3"
APSW_MODULE = "apsw"
UNQLITE_MODULE = "unqlite"
VEDIS_MODULE = "vedis"
GNU_MODULE = "dbm.gnu"
NDBM_MODULE = "dbm.ndbm"

SQLITE_VALUE_COLUMN = "Value"
# Notes on SQLITE_VALUE_COLUMN from a Berkeley DB perspective.
# Application file specifications declare a PRIMARY field.
# In Berkeley DB this corresponds to the value in (key, value) records of the
# primary RECNO database and to the key in (key, value) records of secondary
# databases.
# In DPT this corresponds to the visible field of a record and the indexes
# correspond to the invisible fields of a record (keys in Berkeley DB secondary
# databases).
# In Sqlite3 this corresponds to the row number of a row and an arbitrary name
# is chosen for the equivalent of the value in a Berkeley DB primary database.

SQLITE_SEGMENT_COLUMN = "Segment"
SQLITE_COUNT_COLUMN = "RecordCount"
# Notes on SQLITE_SEGMENT_COLUMN and SQLITE_COUNT_COLUMN from a Berkeley DB
# perspective.
# The ...bit... secondary database values are a composite consisting of the
# segment number, the count of records in the segment, and either a bytes
# representation of a record number or the record number of a list or bitarray
# of record numbers.
# Here segment number gets it's own column and becomes part of the key.
# Count of records gets it's own column but is not part of the key.
# The remnant of the value will be the record number, or the list or bitarray
# of record numbers.

SQLITE_RECORDS_COLUMN = "RecordNumbers"
# Notes on SQLITE_RECORDS_COLUMN from a DPT perspective.
# Index value which reference many records hold the record number references as
# lists or bitmaps of record numbers, one per segment, depending on how many
# records are referenced in the segment.
# The column will be up to 8192 bytes if it holds a list, or exactly 8192 bytes
# if it holds a bitmap.
# It is held in a table of it's own to reduce the movement overheads inserting
# or deleting records and indexes.

# Access method entry in secondary databases for Berkeley DB.
ACCESS_METHOD = "access_method"

# Access methods for Berkeley DB databases.
# (UnQLite and Vedis use BTREE and HASH too.)
BTREE = "btree"
HASH = "hash"
RECNO = "recno"

# Branching factor for BTrees in UnQLite and Vedis databases.
BRANCHING_FACTOR = "branching_factor"

# DPT file and field attributes. (SQLite3 uses FLT too.)
BLOB = "blob"
FLT = "float"
INV = "invisible"
UAE = "update_at_end"
ORD = "ordered"
ONM = "ordnum"
SPT = "splitpct"
BSIZE = "bsize"
BRECPPG = "brecppg"
BRESERVE = "breserve"
BREUSE = "breuse"
DSIZE = "dsize"
DRESERVE = "dreserve"
DPGSRES = "dpgsres"
FILEORG = "fileorg"
DEFAULT = -1
EO = 0
RRN = 36

SUPPORTED_FILEORGS = (EO, RRN)
MANDATORY_FILEATTS = {
    BSIZE: (int, type(None)),
    BRECPPG: int,
    DSIZE: (int, type(None)),
    FILEORG: int,
}
SECONDARY_FIELDATTS = {
    FLT: False,
    INV: True,
    UAE: False,
    ORD: True,
    ONM: False,
    SPT: 50,
    ACCESS_METHOD: BTREE,  # HASH is the other supported value.
    BRANCHING_FACTOR: 50,
}
PRIMARY_FIELDATTS = {
    FLT: False,
    INV: False,
    UAE: False,
    ORD: False,
    ONM: False,
    SPT: 50,
    ACCESS_METHOD: RECNO,  # Only supported value.
}
DB_FIELDATTS = {ACCESS_METHOD}
DPT_FIELDATTS = {FLT, INV, UAE, ORD, ONM, SPT}
SQLITE3_FIELDATTS = {FLT}
NOSQL_FIELDATTS = {BRANCHING_FACTOR, ACCESS_METHOD}
FILEATTS = {
    BSIZE: None,
    BRECPPG: None,
    BRESERVE: DEFAULT,
    BREUSE: DEFAULT,
    DSIZE: None,
    DRESERVE: DEFAULT,
    DPGSRES: DEFAULT,
    FILEORG: None,
}
DDNAME = "ddname"
FILE = "file"
FILEDESC = "filedesc"
FOLDER = "folder"
FIELDS = "fields"
PRIMARY = "primary"
SECONDARY = "secondary"
DPT_DEFER_FOLDER = "dptdefer"
DB_DEFER_FOLDER = "dbdefer"
SECONDARY_FOLDER = "dbsecondary"
DPT_DU_SEQNUM = "Seqnum"
DPT_SYS_FOLDER = "dptsys"
DPT_SYSDU_FOLDER = "dptsysdu"
TAPEA = "TAPEA"
TAPEN = "TAPEN"
DEFER = "defer"
USERECORDIDENTITY = "userecordidentity"
RECORDIDENTITY = "RecordIdentity"
RECORDIDENTITYINVISIBLE = "".join((RECORDIDENTITY, "Invisible"))
IDENTITY = "identity"
BTOD_FACTOR = "btod_factor"
BTOD_CONSTANT = "btod_constant"
DEFAULT_RECORDS = "default_records"
DEFAULT_INCREASE_FACTOR = "default_increase_factor"
TABLE_B_SIZE = 8160
DEFAULT_INITIAL_NUMBER_OF_RECORDS = 200

INDEXPREFIX = "ix"
SEGMENTPREFIX = "sg"
TABLEPREFIX = "t"

# At Python3 problems converting  Python str or bytes to C++ string for DPT API
# interface are worked around by limiting primary field length to 127.  This
# leaves room for encoding expansion within the DPT limit of 255.  Nothing can
# be done about indexes but there will be double, or quadruple the occurrences
# on Table B.  63 four-byte utf-8 encodings fit in 255 bytes.
DPT_PRIMARY_FIELD_LENGTH = "dpt_primary_field_length"
SAFE_DPT_FIELD_LENGTH = 63

# DPT pattern matching special characters.
DPT_PATTERN_CHARS = {c: "".join(("!", c)) for c in "*+!#/,)(/-="}

# Byte length of segment references in secondary database records.
LENGTH_SEGMENT_BITARRAY_REFERENCE = 11
LENGTH_SEGMENT_LIST_REFERENCE = 10
LENGTH_SEGMENT_RECORD_REFERENCE = 6
SEGMENT_HEADER_LENGTH = 6

# Delimiter for file and table names generated from PRIMARY and SECONDARY names.
SUBFILE_DELIMITER = "_"

# Constants defined for bitbases (without descriptions so far).
# (UnQLite, Vedis, dbm.gnu, and dbm.ndbm, use many of these too.)
EXISTENCE_BITMAP_SUFFIX = SUBFILE_DELIMITER + "ebm"
SEGMENT_SUFFIX = SUBFILE_DELIMITER + "segment"
CONTROL_FILE = SUBFILE_DELIMITER * 3 + "control"
DEFAULT_SEGMENT_SIZE_BYTES = 4000
SPECIFICATION_KEY = b"_specification"
SEGMENT_SIZE_BYTES_KEY = b"_segment_size_bytes"
TABLE_REGISTER_KEY = b"_table_register"
FIELD_REGISTER_KEY = b"_field_register"

# Constants defined for UnQLite and Vedis databases.  dbm.gnu and dbm.ndbm are
# added to this list later.
FREED_RECORD_NUMBER_SEGMENTS_SUFFIX = SUBFILE_DELIMITER + "freed"
HIGH_TREE_NODE = SUBFILE_DELIMITER + "high_tree_node"
SEGMENT_KEY_SUFFIX = "0"
SEGMENT_VALUE_SUFFIX = "1"
TREE_NODE_SUFFIX = "2"
LIST_BYTES = "L"
BITMAP_BYTES = "B"
