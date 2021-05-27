# databasedu.py
# Copyright 2008 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Deferred update versions of Database methods which are identical in all
database engines.

The databasedu.Database class must be earlier in the method resolution order
than database.Database.

"""
from .segmentsize import SegmentSize


class DatabaseduError(Exception):
    pass


class Database:
    
    """Provide deferred update versions of the record update methods.
    """

    def close_context(self):
        """Close main and deferred update databases and environment."""
        self._control.close()
        super().close_context()

    def _get_deferable_update_files(self, db):
        """Return dictionary of databases in db whose updates are deferable."""
        deferable = False
        main = self.database_definition
        for d in main.values():
            if d.secondary:
                deferable = True
                break
        if not deferable:
            return False
        
        try:
            iter(db)
            if isinstance(db, str):
                db = [db]
        except TypeError:
            db = set(main.keys())
        dbadd = dict()
        for k, v in main.items():
            if k in db:
                dbadd[k] = []
                for s in v.secondary:
                    dbadd[k].append(s)
        return dbadd

    def open_context(self):
        """Open all DBs."""
        super().open_context()
        self._control.open_root(self.dbservices)
        return True

    def put_instance(self, dbset, instance):
        """Put new instance on database dbset.
        
        This method assumes all primary databases are DB_RECNO and enough
        memory is available to do a segemnt at a time.
        
        """
        putkey = instance.key.pack()
        instance.set_packed_value_and_indexes()
        
        main = self.database_definition
        primarydb = main[dbset].primary
        db = main[dbset].secondary
        lookup = main[dbset].dbname_to_secondary_key

        if putkey != 0:
            # reuse record number is not allowed
            raise DatabaseduError(
                'Cannot reuse record number in deferred update.')
        key = primarydb.put(putkey, instance.srvalue)
        if key is not None:
            # put was append to record number database and
            # returned the new primary key. Adjust record key
            # for secondary updates.
            instance.key.load(key)
            putkey = key
        instance.srkey = self.encode_record_number(putkey)

        srindex = instance.srindex
        segment, record_number = divmod(putkey, SegmentSize.db_segment_size)
        primarydb.defer_put(segment, record_number)
        pcb = instance._putcallbacks
        for secondary in srindex:
            lusec = lookup[secondary]
            if lusec not in db:
                if secondary in pcb:
                    pcb[secondary](instance, srindex[secondary])
                continue
            for v in srindex[secondary]:
                db[lusec].defer_put(v, segment, record_number)
        if record_number in self.deferred_update_points:
            self.do_segment_deferred_updates(
                primarydb, segment, record_number, db=dbset)

    def use_deferred_update_process(self, **kargs):
        raise DatabaseduError('Query use of du when in deferred update mode')
