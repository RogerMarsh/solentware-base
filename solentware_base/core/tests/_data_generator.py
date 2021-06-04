# _data_generator.py
# Copyright 2020 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Generate test records from a sample Portable Game Notation (PGN) file.

The file, 96-97.pgn, was downloaded from the 4NCL website around 2008.

"""

import os
import bz2

from .. import record
from ..constants import (
    PRIMARY,
    SECONDARY,
    DEFER,
    BTOD_FACTOR,
    DEFAULT_RECORDS,
    DEFAULT_INCREASE_FACTOR,
    BTOD_CONSTANT,
    DDNAME,
    FILE,
    FOLDER,
    FIELDS,
    FILEDESC,
    FLT,
    INV,
    UAE,
    ORD,
    ONM,
    SPT,
    EO,
    RRN,
    BSIZE,
    BRECPPG,
    BRESERVE,
    BREUSE,
    DSIZE,
    DRESERVE,
    DPGSRES,
    FILEORG,
    DPT_PRIMARY_FIELD_LENGTH,
    ACCESS_METHOD,
    HASH,
    BTREE,
)
from .. import filespec


def generate_filespec(data):
    """Define a minimal FileSpec and return it."""
    ordered, unordered = data.fields()
    games = "Games"
    filedef = {
        PRIMARY: games,
        FILE: filespec.FileSpec.dpt_dsn(games),
        DDNAME: games.upper(),
    }
    filedef[SECONDARY] = {}
    filedef[FIELDS] = {filedef[PRIMARY]: None}
    for f in ordered:
        filedef[SECONDARY][f] = None
        filedef[FIELDS][f] = {INV: True, ORD: True, ACCESS_METHOD: BTREE}
    for f in unordered:
        filedef[SECONDARY][f] = None
        filedef[FIELDS][f] = {INV: True, ORD: True, ACCESS_METHOD: HASH}
    # Add adjustments to DPT defaults when suitable values are known.
    filedef[DEFAULT_RECORDS] = 2000
    # Return FileSpec
    return filespec.FileSpec(**{games: filedef})


class Value(record.ValueData):
    """A value for a record."""

    def __init__(self):
        """Extend, (does nothing else so method will be removed)."""
        super().__init__()

    def pack(self):
        """Extend, populate indicies, and return tuple of value and indicies."""
        v = super().pack()
        i = v[1]
        i[self.movetextkey] = self.movetext
        for t, u in self.tags:
            i[t] = [u]
        return v


def populate(database, data, transaction=True):
    """Create record on database using data, in a transaction by default."""
    Record = record.Record
    for tags, moves, score in data.records():
        r = Record(valueclass=Value)
        r.value.load(score)
        r.value.movetext = list(set(moves[1]))
        r.value.movetextkey = moves[0]
        r.value.tags = tags
        if transaction:
            database.start_transaction()
        database.put_instance("Games", r)
        if transaction:
            database.commit()


class _DataGenerator:
    def __init__(self):
        datafile = os.path.join(os.path.dirname(__file__), "96-97.pgn.bz2")
        blocks = bz2.open(datafile, mode="rt").read().split("\n\n")
        games = []
        data = []
        for g in range(0, len(blocks) - 1, 2):
            data.append((blocks[g], blocks[g + 1]))
            games.append("\n\n".join((blocks[g], blocks[g + 1])))
        self.data = data
        self.games = games

    def fields(self):
        ordered = set()  # All the Tag names in the PGN file.
        unordered = {"Movetext"}  # For whitespace delimited items in Movetext.
        for g in self.data:
            for fn in [
                u[1:-1] for u, v in [t.split('"', 1) for t in g[0].split("\n")]
            ]:
                ordered.add(fn)
        return ordered, unordered

    def records(self):
        games = self.games
        for e, g in enumerate(self.data):
            yield [
                (u[1:-1], v[:-2])
                for u, v in [t.split('"', 1) for t in g[0].split("\n")]
            ], ("Movetext", g[1].split()), repr(games[e])


if __name__ == "__main__":

    dg = _DataGenerator()
    dg.fields()  # Create a database with these fields.
    for i in dg.records():
        pass  # Populate the database with these records.
