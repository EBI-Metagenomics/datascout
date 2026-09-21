#!/usr/bin/env python3
"""Checks for the OrthoDB database path of ncbi_orthodb_data.py, against a database built the
same way accessory/orthodb.py builds the real one. Run: python3 tests/test_orthodb_local.py"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bin"))

try:
    import duckdb
except ImportError:
    sys.exit("duckdb is not installed, skipping")

from ncbi_orthodb_data import dump_release, write_combined_fa_from_db

OG2GENES = [
    ("1at5690", "5691_0:000001"),
    ("1at5690", "5702_0:000002"),
    ("2at5690", "5691_0:000003"),
    ("9at4890", "4932_0:000004"),   # another level, must be left out
]
PROTEINS = [
    ("5691_0:000001", "MKV"),
    ("5702_0:000002", "MTT"),
    ("5691_0:000003", "MGG"),
    ("4932_0:000004", "MAA"),
    ("5691_0:000009", "MCC"),       # in no cluster at all
]


def build_db(db_path):
    with duckdb.connect(db_path) as con:
        con.execute("CREATE TABLE og2genes (og_id VARCHAR, gene_id VARCHAR)")
        con.executemany("INSERT INTO og2genes VALUES (?, ?)", OG2GENES)
        con.execute("CREATE TABLE proteins (gene_id VARCHAR, seq VARCHAR)")
        con.executemany("INSERT INTO proteins VALUES (?, ?)", PROTEINS)
        con.execute("CREATE TABLE meta AS SELECT 'odb12v2' AS release")


def main():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "orthodb.duckdb")
        build_db(db_path)
        assert dump_release(db_path) == "odb12v2"

        out = os.path.join(tmp, "combined_orthodb_5690.faa")
        assert write_combined_fa_from_db(["1at5690", "2at5690"], db_path, out) == 3

        #   headers keep the gene id only, and no gene of another level leaks in
        written = open(out).read()
        assert sorted(written.split()) == sorted(
            ">5691_0:000001 MKV >5702_0:000002 MTT >5691_0:000003 MGG".split()
        ), written

        #   a cluster with no genes is not an error, it is simply no proteins
        assert write_combined_fa_from_db(["7at5690"], db_path, out) == 0

    print("ok")


if __name__ == "__main__":
    main()
