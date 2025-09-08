#!/usr/bin/env python3

import ete3
from ete3 import NCBITaxa
import argparse
import os
from datetime import datetime

def get_tax_lineage(taxid, outfile, db_path=None, taxdump=None):

    if db_path and taxdump:
        ncbi = NCBITaxa(dbfile=db_path, taxdump_file=taxdump)
    elif db_path:
        ncbi = NCBITaxa(dbfile=db_path)
    else:
        ncbi = NCBITaxa()    

    lineage = ncbi.get_lineage(taxid)
    lineage_names = ncbi.get_taxid_translator(lineage)
    lineage_ranks = ncbi.get_rank(lineage)

    lineage.reverse()

    tax_dict = {}
    with open(outfile, 'w') as tax_ranks:
        for taxid in lineage:
            taxname = lineage_names.get(taxid, "no rank")
            rank = lineage_ranks.get(taxid, "no rank")
            if rank != "no rank":
                tax_ranks.write(f'{rank}\t{taxid}\t{taxname}\n')

def main():
    parser = argparse.ArgumentParser(description="Get taxonomic lineage information using ete3")
    parser.add_argument(
        "-t", "--taxid", type=int, help="Taxid to retrieve lineage information"
    )
    parser.add_argument(
        "-d", "--db_path", type=str, help="Path to the ete3 SQLite taxonomy database (taxa.sqlite)", required=False, default=None
    )
    parser.add_argument(
        "-td", "--taxdump", type=str, help="Path to the taxdump.tar.gz", required=False, default=None
    )
    parser.add_argument(
        "-o", "--output", type=str, help="output file name"
    )
    parser.add_argument(
        "-v", "--version", action="store_true"
    )
    args = parser.parse_args()

    if args.db_path and args.taxdump:
        db_date = datetime.fromtimestamp(os.path.getmtime(args.taxdump)).strftime('%Y-%m-%d')
    else:
        db_date = datetime.now().strftime('%Y-%m-%d')

    if args.version:
        print(f"\tete3: {ete3.__version__}")
        print(f"\tNCBI taxdump downloaded on: {db_date}")
        exit(0)
    
    get_tax_lineage(args.taxid, args.output, args.db_path, args.taxdump)


if __name__ == "__main__":
    main()