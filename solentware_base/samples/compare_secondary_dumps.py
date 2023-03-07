# compare_secondary_dumps.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Compare secondary database output from mdb_dump with output from db_dump.

Actually this module does not care who produced what: it's purpose is to
verify databases generated from the same source in the two database engines
are equivalent in terms of keys and values stored.

Equivalent, not the same, because the least significant part of the value is
expected to be an integer whose value in Berkeley DB is one more than it's
value in Symas LMMD.

Output is the single line:

done

if the compared databases are equivalent.

This module was written while investigating the problem with
'display by secondary database key' noted in a comment in the
solentware_grid.datagrid.DataGridBase.set_yscrollbar() method.

The command for Symas LMMD for the stated problem is:
mdb_dump -n -s games_Black [envpath] > output

The command for Berkeley DB for the stated problem is:
db4_dump -s games_Black [file] > output

"""

if __name__ == "__main__":

    import os
    import binascii

    home = os.path.expanduser("~")
    paths = (
        (home, "sliderproblem", "bsddbblackone"),
        (home, "sliderproblem", "bsddbblacktwo"),
    )
    records = ({}, {})
    for r, p in zip(records, paths):
        path = os.path.join(*p)
        capture = False
        key = None
        for line in open(path):
            line = line.strip()
            if not capture:
                if line == "HEADER=END":
                    capture = True
                continue
            if line == "DATA=END":
                break
            if key is None:
                key = line
                continue
            r[key] = line
            key = None
    mismatch = 0
    if set(records[0]) != set(records[1]):
        print("key mismatch", len(records[0]), len(records[1]))
    else:
        for key, value in sorted(records[0].items()):
            paired_value = records[1][key]
            if len(value) != len(paired_value):
                print("Reference length mismatch", key, value, paired_value)
                mismatch += 1
                continue
            ref = int.from_bytes(binascii.unhexlify(value), byteorder="big")
            refp = int.from_bytes(
                binascii.unhexlify(paired_value), byteorder="big"
            )
            if ref != refp - 1:
                print("Reference mismatch", key, value, paired_value)
                mismatch += 1
    if mismatch:
        print("total records:", len(records[0]))
        if mismatch == 1:
            print(mismatch, "mismatched value")
        else:
            print(mismatch, "mismatched values")
        if mismatch == len(records[0]):
            print("Perhaps first dump is not 0-based or second not 1-based")
    print("done")
