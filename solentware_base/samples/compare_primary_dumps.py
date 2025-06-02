# compare_primary_dumps.py
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
mdb_dump -n -s games [envpath] > output

The command for Berkeley DB for the stated problem is:
db4_dump -k -p -s games [file] > output

"""

if __name__ == "__main__":
    import os
    import binascii

    home = os.path.expanduser("~")
    paths = (
        (home, "sliderproblem", "bsddbone"),
        (home, "sliderproblem", "bsddbtwo"),
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
            if len(key) == 8:
                r[int.from_bytes(binascii.unhexlify(key), byteorder="big")] = (
                    binascii.unhexlify(line.encode("iso-8859-1")).decode(
                        "iso-8859-1"
                    )
                )
            else:
                r[int(key) - 1] = line
            key = None
    escape = 0
    if set(records[0]) != set(records[1]):
        print("key mismatch", len(records[0]), len(records[1]))
    else:
        for key, value in sorted(records[0].items()):
            paired_value = records[1][key]
            if value != paired_value:
                if value != paired_value.replace("\\\\", "\\"):
                    print("Value mismatch", key)
                    print(value)
                    print(paired_value)
                else:
                    escape += 1
    if escape:
        print("total records:", len(records[0]))
        if escape == 1:
            print(escape, "record assumed equal taking '\\' into account")
        else:
            print(escape, "records assumed equal taking '\\' into account")
    print("done")
