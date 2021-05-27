# sqlite3duapi.py
# Copyright (c) 2011 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Provide sqlite3 database access methods compatible with DPT single-step.

The compatibility provided is doing large updates in a separate process.

The sqlite3 equivalent to any form of DPT deferred update is drop indexes
before the update and create the indexes after the update.

See www.dptoolkit.com for details of DPT

List of classes

Sqlite3duapiError - Exceptions
Sqlite3duapi - Sqlite3 database definition
Sqlite3duapiRecord - Sqlite3 record level access

"""

from sqlite3api import Sqlite3api, Sqlite3apiRecord, Sqlite3apiError


class Sqlite3duapiError(Sqlite3apiError):
    pass


class Sqlite3duapi(Sqlite3api):
    
    """Support sqlite3 equivalent for DPT single-step deferred updates.

    Extend and override Sqlite3api methods for update.

    Methods added:

    None

    Methods overridden:

    delete_instance - raise exception
    do_deferred_updates - raise exception
    edit_instance - raise exception
    make_cursor - raise exception
    use_deferred_update_process - raise exception
    make_root - use DPTduapiRecord to open file

    Methods extended:

    None
    
    """
    
    def delete_instance(self, dbname, instance):
        raise Sqlite3duapiError, 'delete_instance not implemented'

    def do_deferred_updates(self, pyscript, filepath):
        raise Sqlite3duapiError, 'do_deferred_updates not implemented'

    def edit_instance(self, dbname, instance):
        raise DPTduapiError, 'edit_instance not implemented'

    def make_cursor(self, dbname):
        raise Sqlite3duapiError, 'make_cursor not implemented'

    def use_deferred_update_process(self):
        raise Sqlite3duapiError, 'Query use of du when in deferred update mode'

    def make_root(self, name, table, primary):
        """"""
        return Sqlite3duapiRecord(name, table, primary)

    def set_defer_update(self, db=None, duallowed=False):
        # Hoped that drop and create would lead to faster indexing. Not so.
        #for name, table in self._sqtables.iteritems():
            #if not table._primary:
                #table._connectioncursor.execute(
                    #''.join(('drop index if exists ', table._indexname)),
                    #)
        # Originally adjusted journal mode
        #self._journal_mode = self._sqconn_cursor.execute('pragma journal_mode')
        #self._sqconn_cursor.execute('pragma journal_mode = off')
        #self._sqconn_cursor.execute('begin exclusive transaction')
        pass

    def unset_defer_update(self, db=None):
        # Hoped that drop and create would lead to faster indexing. Not so.
        #for name, table in self._sqtables.iteritems():
            #if not table._primary:
                #table._connectioncursor.execute(
                    #''.join((
                        #'create index if not exists ', table._indexname,
                        #' on ', table._name, ' (', table._name, ')')),
                    #)
        # Originally adjusted journal mode
        #self._sqconn.commit()
        #self._sqconn_cursor.execute('commit transaction')
        #self._sqconn_cursor.execute('pragma journal_mode = delete')
        #self._journal_mode = self._sqconn_cursor.execute('pragma journal_mode')
        pass

class Sqlite3duapiRecord(Sqlite3apiRecord):

    """Provide sqlite equivqlent for DPT single-step deferred update.

    This class disables methods not appropriate to deferred update.

    Methods added:

    None

    Methods overridden:

    delete_instance - not implemented in sqlite equivalent for deferred updates.
    edit_instance - not implemented in sqlite equivalent for deferred updates.
    make_cursor - not supported by this class.

    Methods extended:

    None
    
    """

    def delete_instance(self, dbname, instance):
        raise Sqlite3duapiError, 'delete_instance not implemented'

    def edit_instance(self, dbname, instance):
        raise Sqlite3duapiError, 'edit_instance not implemented'

    def make_cursor(self, dbname):
        raise Sqlite3duapiError, 'make_cursor not implemented'
