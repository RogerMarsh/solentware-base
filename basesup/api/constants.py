# constants.py
# Copyright (c) 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Constants used defining and accessing Berkeley DB via bsddb or DPT.

See www.sleepycat.com for details of Berkeley DB
See www.dptoolkit.com for details of DPT

"""

FLT = 'float'
INV = 'invisible'
UAE = 'update_at_end'
ORD = 'ordered'
ONM = 'ordnum'
SPT = 'splitpct'
BSIZE = 'bsize'
BRECPPG = 'brecppg'
BRESERVE = 'breserve'
BREUSE = 'breuse'
DSIZE = 'dsize'
DRESERVE = 'dreserve'
DPGSRES = 'dpgsres'
FILEORG = 'fileorg'
DEFAULT = -1
EO = 0
RRN = 36
SUPPORTED_FILEORGS = (EO, RRN)
MANDATORY_FILEATTS = {
    BSIZE:(int, type(None)),
    BRECPPG:int,
    DSIZE:(int, type(None)),
    FILEORG:int,
    }
SECONDARY_FIELDATTS = {
    FLT:False,
    INV:True,
    UAE:False,
    ORD:True,
    ONM:False,
    SPT:50,
    }
PRIMARY_FIELDATTS = {
    FLT:False,
    INV:False,
    UAE:False,
    ORD:False,
    ONM:False,
    SPT:50,
    }
FILEATTS = {
    BSIZE:None,
    BRECPPG:None,
    BRESERVE:DEFAULT,
    BREUSE:DEFAULT,
    DSIZE:None,
    DRESERVE:DEFAULT,
    DPGSRES:DEFAULT,
    FILEORG:None,
    }
DDNAME = 'ddname'
FILE = 'file'
FILEDESC = 'filedesc'
FOLDER = 'folder'
FIELDS = 'fields'
PRIMARY = 'primary'
SECONDARY = 'secondary'
DPT_DEFER_FOLDER = 'dptdefer'
DB_DEFER_FOLDER = 'dbdefer'
SECONDARY_FOLDER = 'dbsecondary'
DPT_DU_SEQNUM = 'Seqnum'
DPT_SYS_FOLDER = 'dptsys'
DPT_SYSDU_FOLDER = 'dptsysdu'
TAPEA = 'TAPEA'
TAPEN = 'TAPEN'
DEFER = 'defer'
USERECORDIDENTITY = 'userecordidentity'
RECORDIDENTITY = 'RecordIdentity'
RECORDIDENTITYINVISIBLE = ''.join((RECORDIDENTITY, 'Invisible'))
IDENTITY = 'identity'
BTOD_FACTOR = 'btod_factor'
DEFAULT_RECORDS = 'default_records'

DUP = 'dup'
BTREE = 'btree'
HASH = 'hash'
RECNO = 'recno'
DUPSORT = 'dupsort'
HASH_DUPSORT = 'hash_dupsort'
BTREE_DUPSORT = 'btree_dupsort'
