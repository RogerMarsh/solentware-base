# _test_case_constants.py
# Copyright 2023 Roger Marsh
# Licence: See LICENCE (BSD licence)

"""Constants which are identical for assert<> statements in unittests.

The test result is expected to be the same for all database engines.

"""

DATABASE_MAKE_RECORDSET_KEYS = (
    "a_o",
    "aa_o",
    "ba_o",
    "bb_o",
    "c_o",
    "cep",
    "deq",
)
DATABASE_MAKE_RECORDSET_SEGMENTS = (
    b"".join(
        (
            b"\x7f\xff\xff\xff\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\xff\xff\xff\x00\x00",
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\x00\x00\xff\xff\xff",
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\x00\x00\x00\x00\xff",
            b"\xff\xff\x00\x00\x00\x00\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\xff\xff\xff\x00\x00\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\xff\xff\xff\x00\x00",
        )
    ),
    b"".join(
        (
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\x00\x00\x00\x00\xff\xff\xff",
        )
    ),
    b"\x00\x25\x00\x41",
    b"\x00\x42\x00\x43\x00\x44",
)
database_make_recordset = {
    "05_01": (
        frozenset(),
        frozenset((("indexvalue", b"\x00\x00\x00\x01\x00\x00"),)),
        frozenset(),
    ),
    "05_02": (
        frozenset((("nin", b"\x00\x00\x00\x00\x00d"),)),
        frozenset((("nin", b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\t"),)),
        frozenset(),
    ),
    "05_03": (
        frozenset(
            (
                (8, b"\x00B\x00C\x00D"),
                ("twy", b"\x00\x00\x00\x00\x00\x03\x00\x00\x00\x08"),
            )
        ),
        frozenset((("twy", b"\x00\x00\x00\x00\x00\x04\x00\x00\x00\x08"),)),
        frozenset(((8, b"\x00B\x00C\x00D\x00c"),)),
    ),
    "05_04": (
        frozenset(
            (
                (
                    1,
                    b"".join(
                        (
                            b"\x00\x00\x00\xff\xff\xff\x00\x00",
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
                ("aa_o", b"\x00\x00\x00\x00\x00 \x00\x00\x00\x01"),
            )
        ),
        frozenset((("aa_o", b"\x00\x00\x00\x00\x00\x19\x00\x00\x00\x01"),)),
        frozenset(
            (
                (
                    1,
                    b"".join(
                        (
                            b"\x00\x00\x00\xff\xff\xff\x00\x00",
                            b"\x00\x00\x00\x00\x10\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
    ),
    "05_05": (
        frozenset(
            (
                (7, b"\x00%\x00A"),
                ("tww", b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x07"),
            )
        ),
        frozenset((("tww", b"\x00\x00\x00\x00\x00\x03\x00\x00\x00\x07"),)),
        frozenset(((7, b"\x00%\x00/\x00A"),)),
    ),
    "05_06": (
        frozenset(
            (
                (8, b"\x00B\x00C\x00D"),
                ("twy", b"\x00\x00\x00\x00\x00\x03\x00\x00\x00\x08"),
            )
        ),
        frozenset((("twy", b"\x00\x00\x00\x00\x00\x07\x00\x00\x00\x08"),)),
        frozenset(((8, b"\x00B\x00C\x00D\x00c\x00d\x00e\x00f"),)),
    ),
    "05_07": (
        frozenset(
            (
                (8, b"\x00B\x00C\x00D"),
                ("twy", b"\x00\x00\x00\x00\x00\x03\x00\x00\x00\x08"),
            )
        ),
        frozenset((("twy", b"\x00\x00\x00\x00\x00\x08\x00\x00\x00\x08"),)),
        frozenset(
            (
                (
                    8,
                    b"".join(
                        (
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                            b"8\x00\x00\x00\x1e\x02\x00\x00",
                        )
                    ),
                ),
            )
        ),
    ),
    "05_08": (
        frozenset(),
        frozenset(
            (
                ("ten", b"\x00\x00\x00\x00\x002"),
                ("ten", b"\x00\x00\x00\x01\x003"),
            )
        ),
        frozenset(),
    ),
    "05_09": (
        frozenset((("one", b"\x00\x00\x00\x00\x002"),)),
        frozenset((("one", b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00"),)),
        frozenset(((1, None),)),
    ),
    "11_01": (
        frozenset(),
        frozenset(),
        frozenset(),
    ),
    "11_02": (
        frozenset((("nin", b"\x00\x00\x00\x00\x00d"),)),
        frozenset((("nin", b"\x00\x00\x00\x00\x00d"),)),
        frozenset(),
    ),
    "11_03": (
        frozenset(
            (
                ("twy", b"\x00\x00\x00\x00\x00\x03\x00\x00\x00\x08"),
                (8, b"\x00B\x00C\x00D"),
            )
        ),
        frozenset((("twy", b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x08"),)),
        frozenset(((8, b"\x00B\x00C"),)),
    ),
    "11_04": (
        frozenset(
            (
                ("bb_o", b"\x00\x00\x00\x00\x00 \x00\x00\x00\x03"),
                (
                    3,
                    b"".join(
                        (
                            b"\x00\x00\x00\x00\x00\x00\x00\xff",
                            b"\xff\xff\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
        frozenset((("bb_o", b"\x00\x00\x00\x00\x00\x17\x00\x00\x00\x03"),)),
        frozenset(
            (
                (
                    3,
                    b"".join(
                        (
                            b"\x00\x00\x00\x00\x00\x00\x00\xff",
                            b"\xf7\xff\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
    ),
    "11_05": (
        frozenset(
            (
                ("tww", b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x07"),
                (7, b"\x00%\x00A"),
            )
        ),
        frozenset((("tww", b"\x00\x00\x00\x00\x00%"),)),
        frozenset(((7, None),)),
    ),
    "11_06": (
        frozenset((("one", b"\x00\x00\x00\x00\x002"),)),
        frozenset(),
        frozenset((("one", None),)),
    ),
    "11_07": (
        frozenset(
            (
                ("a_o", b"\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x00"),
                (
                    0,
                    b"".join(
                        (
                            b"\x7f\xff\xff\xff\x00\x00\x00\x00",
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
        frozenset((("a_o", b"\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00"),)),
        frozenset(
            (
                (
                    0,
                    b"".join(
                        (
                            b"\x78\x00\x00\x01\x00\x00\x00\x00",
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
    ),
    "11_08": (
        frozenset(
            (
                ("a_o", b"\x00\x00\x00\x00\x00\x1f\x00\x00\x00\x00"),
                (
                    0,
                    b"".join(
                        (
                            b"\x7f\xff\xff\xff\x00\x00\x00\x00",
                            b"\x00\x00\x00\x00\x00\x00\x00\x00",
                        )
                    ),
                ),
            )
        ),
        frozenset((("a_o", b"\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00"),)),
        frozenset(((0, b"\x00\x01\x00\x03\x00\x04\x00\x1f"),)),
    ),
    "05_0": (
        frozenset(((),)),
        frozenset(((),)),
        frozenset(((),)),
    ),
}
database_populate_recordset_segment = {
    "c_o": b"".join(
        (
            b"\x00\x00\x00\x00\x00\x00\x00\x00",
            b"\x00\xff\xff\xff\x00\x00\x00\x00",
        )
    ),
    "tww": b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x07",
}
