# view_raw_records.py
# Copyright 2012 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Utility to show key value pairs."""


if __name__ == '__main__':

    import os
    import pickle

    # bsddb removed from Python 3.n
    try:
        from bsddb3.db import DB
    except ImportError:
        from bsddb.db import DB

    import basesup.tools.dialogues

    filename = basesup.tools.dialogues.askopenfilename(
        title='DB file to view')
    if filename:
        db = DB()
        db.open(filename, os.path.basename(filename))
        c = db.cursor()
        rec = c.first()
        print(rec)
        try:
            obj = pickle.loads(rec[1])
            print(repr(obj))
            try:
                print(repr(obj.__dict__))
            except:
                pass
        except:
            print('Cannot unpickle record value')
        for e in range(100):#(1):#0):
            rec = c.next()
            if rec is None:
                break
            print()
            print(rec)
            try:
                obj = pickle.loads(rec[1])
                print(repr(obj))
                try:
                    print(repr(obj.__dict__))
                except:
                    pass
            except:
                print('Cannot unpickle record value')
        c.close()
        db.close()
        del db

