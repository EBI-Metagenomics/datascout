#!/usr/bin/env python3

import os
import duckdb
import requests
import argparse
import logging
import shutil
import glob

#   set all requests static params beforehand
SEARCH_URL = "https://data.orthodb.org/current/search?"

SEARCH_URL_ARGS = {
    "universal": "0.9",
    "singlecopy": "0.9",
    "take": "5000"
}

def parse_taxa(taxa_file):
    logging.info("Parsing taxa lineages from file")
    tax_dict = {}
    with open(taxa_file, 'r') as taxa:
        for line in taxa:
            data = line.rstrip().split('\t')
            #   rank: taxid
            tax_dict[data[1]] = data[0]
    return tax_dict

def get_orthodb_data(taxa_dict, max_lineage=None):
    """query orthoDB per taxonmic rank. Stop when non-empty result
    is returned. Use rank if provided by user"""
    logging.info("Getting OrthoDB data")
    clusters = []

    for taxid, rank in taxa_dict.items():
        data_found = False
        query_terms = SEARCH_URL_ARGS.copy()
        query_terms.update(
            {
                "level": str(taxid),
                "species": str(taxid)
            }
        )

        while not data_found:
            logging.info(f"Searching OrthoDB entries for taxid {taxid}")
            response = query_orthodb(query_terms, search=True)
            data = response.json()

            if data["count"] == "0":
                logging.info(f"No data found for taxid {taxid}. Moving to next rank.")
                #   if rank provided by user was reached then exit
                if max_lineage and rank == max_lineage:
                    logging.error(f"No OrthoDB groups found up to taxonomic rank {max_lineage}, as provided by user.")
                    return None
                #   Continue to the next taxid
                break
            else:
                #   add clusters to existing dict
                clusters.extend(data["data"])
                num_clusters = data["count"]
                data_found = True
                logging.info(f"Found {num_clusters} clusters for taxid {taxid}")

                # If rank provided by user was not reached then keep going
                if max_lineage and rank == max_lineage:
                    logging.info(f"Found {len(clusters)} clusters in OrthoDB")
                    return clusters

                logging.info(f"Found {len(clusters)} clusters in OrthoDB")
                return clusters
    #   in case nothing is found at any rank
    if clusters:
        logging.info(f"Found {len(clusters)} clusters in OrthoDB")
        return clusters
    else:
        if max_lineage:
            logging.error(f"No OrthoDB groups found up to taxonomic rank {max_lineage}, as provided by user.")
        else:
            logging.error("No OrthoDB groups found at any taxonomic rank.")
        return None

def query_orthodb(query_terms, search=False):
    if search:
        url = SEARCH_URL
    try:
        response = requests.get(url=url, params=query_terms)
        return response
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def dump_release(orthodb_db):
    """OrthoDB release recorded by accessory/orthodb.py when it built the database"""
    with duckdb.connect(orthodb_db, read_only=True) as con:
        return con.execute("SELECT release FROM meta").fetchone()[0]

def write_combined_fa_from_db(clusters, orthodb_db, fasta_file_path):
    """Write the proteins of the given clusters, joining OG membership and sequences in the
    database built by accessory/orthodb.py. Return the number of proteins written"""
    n_proteins = 0
    with duckdb.connect(orthodb_db, read_only=True) as con:
        query = con.execute("""
            SELECT DISTINCT proteins.gene_id, proteins.seq
            FROM og2genes JOIN proteins USING (gene_id)
            WHERE og2genes.og_id IN (SELECT * FROM UNNEST(?))
        """, [list(clusters)])
        with open(fasta_file_path, 'w') as outfile:
            while batch := query.fetchmany(100000):
                for gene, sequence in batch:
                    outfile.write(f">{gene}\n{sequence}\n")
                n_proteins += len(batch)
    return n_proteins


def main():
    parser = argparse.ArgumentParser(description="Fetch data from orthodb")
    parser.add_argument(
        "-t", "--tax_file", type=str, help="File with taxonomic lineage information"
    )
    parser.add_argument(
        "-l", "--lineage_max", type=str, help="""Least specfic lineage rank to fetch data from.
        The default behaviour is to traverse the taxonomic tree until othologous groups are found""", required=False
    )
    parser.add_argument(
        "-o", "--output_dir", type=str, help="output directory"
    )
    parser.add_argument(
        "--max_clusters", type=int, default=None, help="Limit the number of clusters to fetch (mainly for testing)"
    )
    parser.add_argument(
        "--min_proteins", type=int, default=0, help="""Minimum number of proteins required to keep the sample.
        Below it no output directory is produced and the sample is traced in low_proteins.csv"""
    )
    parser.add_argument(
        "--sample_id", type=str, default="", help="Sample identifier used when tracing a dropped sample"
    )
    parser.add_argument(
        "--orthodb_db", type=str, required=True, help="""Path to the OrthoDB database built by
        accessory/orthodb.py, the source of the protein sequences"""
    )
    parser.add_argument(
        "--version", action="store_true", help="Show orthodb version number and exit"
    )
    args = parser.parse_args()

    if args.version:
        print(f"OrthoDB: {dump_release(args.orthodb_db)}")
        return

    logging.basicConfig(level=logging.INFO)

    if not args.tax_file or not args.output_dir:
        parser.error("--tax_file and --output_dir are required unless --version is specified")

    if args.lineage_max == "default":
        max_lineage = None
    else:
        max_lineage = args.lineage_max

    taxa_dict = parse_taxa(args.tax_file)
    clusters = get_orthodb_data(taxa_dict, max_lineage)

    n_proteins = 0
    taxid = ""

    if clusters:
        if args.max_clusters:
            logging.info(f"Limit to first {args.max_clusters} clusters")
            clusters = clusters[:args.max_clusters]
        taxid = clusters[0].split('at')[1]
        orthodb_dir = f"{taxid}_sequences"
        os.makedirs(orthodb_dir, exist_ok=True)
        n_proteins = write_combined_fa_from_db(
            clusters, args.orthodb_db,
            os.path.join(orthodb_dir, f"combined_orthodb_{taxid}.faa")
        )

    #   trace the sample and produce no output directory rather than publish too few proteins
    if n_proteins < args.min_proteins:
        logging.warning(f"Dropping {args.sample_id}: {n_proteins} proteins, minimum is {args.min_proteins}")
        with open('low_proteins.csv', 'w') as trace:
            trace.write(f"{args.sample_id},{taxid},{n_proteins}\n")
        return

    logging.info(f"Found {n_proteins} proteins in OrthoDB")
    os.makedirs(args.output_dir, exist_ok=True)
    for folder in glob.glob("*_sequences"):
        shutil.move(folder, os.path.join(args.output_dir, folder))

if __name__ == "__main__":
    main()
