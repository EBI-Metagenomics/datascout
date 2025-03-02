#!/usr/bin/env python3

import os
import requests
import argparse
import logging
import multiprocessing
import shutil
import time
import glob
from Bio import SeqIO

logging.basicConfig(level=logging.INFO)

#   set all requests static params beforehand
PARSE_URL = "https://data.orthodb.org/current/fasta?"
SEARCH_URL = "https://data.orthodb.org/current/search?"

SEARCH_URL_ARGS = {
    "universal": "0.9",
    "singlecopy": "0.9",
    "take": "5000"
}
MAX_NB_QUERIES_PER_BLOCK = 50
NUM_JOBS = 50 # no of simultaneous runs
WAIT = 10


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

def get_sequences(cluster):
    """get sequence fasta per orthoDB cluster"""
    #   example cluster ID 10626at5690
    taxid = cluster.split('at')[1]
    params = {
        "id": str(cluster),
    }
    orthodb_dir = f"{taxid}_sequences"
    fasta_file_path = os.path.join(orthodb_dir, f"{cluster}.faa")
    if not os.path.exists(orthodb_dir):
        os.makedirs(orthodb_dir, exist_ok=True)
    if not os.path.exists(fasta_file_path):
        response = query_orthodb(params, download=True)
        with open(fasta_file_path, 'wb') as fasta:
            fasta.write(response.content)

def parallelize_jobs(clusters):
    with multiprocessing.Pool(NUM_JOBS) as pool:
        pool.map(get_sequences, clusters)

def query_orthodb(query_terms, search=False, download=False):
    if search:
        url = SEARCH_URL
    if download:
        url = PARSE_URL
    try:
        response = requests.get(url=url, params=query_terms)
        return response
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def create_combined_fa(taxid, orthodb_ncbi_subfolder):
    """Combine orthodb clusters into single file per taxid. Retain unique orthoDB seqIDs. Keep only ID and 
    remove description in fasta headers"""

    ortho_ids = set()
    final_ortho_fasta = os.path.join(orthodb_ncbi_subfolder, f"combined_orthodb_{taxid}.faa")
    if os.path.exists(final_ortho_fasta):
        logging.warning(f"combined fasta file {final_ortho_fasta} exists. Overwriting...")
        os.remove(final_ortho_fasta)
    with open(final_ortho_fasta, 'w') as outfile:
        for fasta_file in glob.glob(os.path.join(orthodb_ncbi_subfolder, "*.faa")):
            if not "combined" in fasta_file:
                for entry in SeqIO.parse(fasta_file, "fasta"):
                    if entry.id not in ortho_ids:
                        outfile.write(f">{entry.id}\n")
                        outfile.write(f"{str(entry.seq)}\n")
                        ortho_ids.add(entry.id)
                os.remove(fasta_file)


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
    args = parser.parse_args()

    if args.lineage_max == "default":
        max_lineage = None
    else:
        max_lineage = args.lineage_max

    taxa_dict = parse_taxa(args.tax_file)
    clusters = get_orthodb_data(taxa_dict, max_lineage)

    os.makedirs(args.output_dir, exist_ok=True)

    if clusters:
    # Chop the data in small blocks and query them with a wait intervall to avoid spaming orthoDB
        num_clusters = len(clusters)
        start = 0
        end = MAX_NB_QUERIES_PER_BLOCK

        while start < num_clusters:
            groups = clusters[start:end]
            start = end
            end = min(end + MAX_NB_QUERIES_PER_BLOCK, num_clusters)
            logging.info(f"Fetching sequences for cluster {start} to {end}")
            parallelize_jobs(groups)

            time.sleep(WAIT)
        
        taxa_folders = glob.glob("*_sequences")
        for folder in taxa_folders:
            taxid = str(folder).split('_')[0]
            create_combined_fa(taxid, folder)
            shutil.move(folder, os.path.join(args.output_dir, folder))
    
if __name__ == "__main__":
    main()
