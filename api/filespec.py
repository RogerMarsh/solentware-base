# filespec.py
# Copyright 2009 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide behaviour common to all file specifications.

List of classes

FileSpec

"""

from basesup.api.constants import BSIZE, BRECPPG, DSIZE, BTOD_FACTOR
from basesup.api.constants import DEFAULT_RECORDS, FILEDESC


class FileSpec(dict):

    """Calculate specification details using initialisation arguments.

    BSIZE and DSIZE are calculated from requested number of records for a
    file.  If no request is made these are set to None telling the user of
    the FileSpec subclass that newly created DPT files should be sized for
    the default number of records.

    BSIZE is size of data area, and DSIZE the index area, of a DPT file.

    Methods added:

    None
    
    Methods overridden:

    None
    
    Methods extended:

    __init__

    """

    def __init__(self, default_=None, **kargs):
        """Set BSIZE and DSIZE for files defined in subclass.

        default_={use_filespec_defaults_=<True|False>,
                  name1=<number of records in file 1>,
                  name2=<number of records in file 2>,
                  ...}
        name1=<specification of file 1>,
        name2=<specification of file 2>,
        ...

        Defaults in 'namex' are used when nothing is said in defaults_ for
        a file 'namex' if use_filespec_defaults_ == True.

        """
        super(FileSpec, self).__init__(**kargs)

        if default_ is None:
            default_ = dict()
        use_filespec_defaults = default_.get('use_filespec_defaults_', False)
        for k, v in kargs.iteritems():
            records = default_.get(k)
            if records is None:
                if use_filespec_defaults:
                    records = v[DEFAULT_RECORDS]
            if records is None:
                v[FILEDESC][BSIZE] = None
                v[FILEDESC][DSIZE] = None
            else:
                bsize = records / v[FILEDESC][BRECPPG]
                if bsize * v[FILEDESC][BRECPPG] < records:
                    bsize += 1
                v[FILEDESC][BSIZE] = bsize
                v[FILEDESC][DSIZE] = int(round(bsize * v[BTOD_FACTOR]))

