#!/usr/bin/env python3
"""
Script to create a mini NCBI taxonomy database and taxdump files for testing
"""

import sqlite3
import tarfile
import os

def make_mini_db():
    db_file = os.path.join(script_dir, "mini_trypanosoma.sqlite")
    db_pkl_file = os.path.join(script_dir, "mini_trypanosoma.sqlite.traverse.pkl")
    if os.path.exists(db_file):
        os.remove(db_file)
    if os.path.exists(db_pkl_file):
        os.remove(db_pkl_file)

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    cur.execute("CREATE TABLE nodes (taxid INTEGER PRIMARY KEY, parent_taxid INTEGER, rank TEXT)")
    cur.execute("CREATE TABLE names (taxid INTEGER, name TEXT, name_class TEXT)")

    nodes = [
        (1, 1, "no rank"),  # root
        (2759, 1, "superkingdom"),
        (2611352, 2759, "clade"),
        (33682, 2611352, "phylum"),
        (5653, 33682, "class"),
        (2704647, 5653, "subclass"),
        (2704949, 2704647, "order"),
        (5654, 2704949, "family"),
        (5690, 5654, "genus"),
        (47570, 5690, "subgenus"),
        (5693, 47570, "species"),
    ]

    names = [
        (1, "root", "scientific name"),
        (2759, "Eukaryota", "scientific name"),
        (2611352, "Discoba", "scientific name"),
        (33682, "Euglenozoa", "scientific name"),
        (5653, "Kinetoplastea", "scientific name"),
        (2704647, "Metakinetoplastina", "scientific name"),
        (2704949, "Trypanosomatida", "scientific name"),
        (5654, "Trypanosomatidae", "scientific name"),
        (5690, "Trypanosoma", "scientific name"),
        (47570, "Schizotrypanum", "scientific name"),
        (5693, "Trypanosoma cruzi", "scientific name"),
    ]

    cur.executemany("INSERT INTO nodes VALUES (?,?,?)", nodes)
    cur.executemany("INSERT INTO names VALUES (?,?,?)", names)
    conn.commit()
    conn.close()
    return names, nodes

def make_nodes_dmp(nodes, filename):
    with open(filename, "w") as f:
        for taxid, parent, rank in nodes:
            line = f"{taxid}\t|\t{parent}\t|\t{rank}\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\t-\t|\n"
            f.write(line)

def make_names_dmp(names, filename):
    with open(filename, "w") as f:
        for taxid, name, name_class in names:
            line = f"{taxid}\t|\t{name}\t|\t-\t|\t{name_class}\t|\n"
            f.write(line)


script_dir = os.path.dirname(os.path.abspath(__file__))
nodes_file = os.path.join(script_dir, "nodes.dmp")
names_file = os.path.join(script_dir, "names.dmp")
merged_file = os.path.join(script_dir, "merged.dmp")
taxdump = os.path.join(script_dir, "mini_taxdump.tar.gz")
if os.path.exists(taxdump):
    os.remove(taxdump)

names, nodes = make_mini_db()
make_nodes_dmp(nodes, nodes_file)
make_names_dmp(names, names_file)
with open(merged_file, "w") as f:
    pass # needs to be present but can be empty

with tarfile.open(taxdump, "w:gz") as tar:
    tar.add(nodes_file, arcname="nodes.dmp")
    tar.add(names_file, arcname="names.dmp")
    tar.add(merged_file, arcname="merged.dmp")

os.remove(nodes_file)
os.remove(names_file)
os.remove(merged_file)

