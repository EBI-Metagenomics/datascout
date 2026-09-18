#!/usr/bin/env python3

import os
import random
import requests
import argparse
import logging
import multiprocessing
import shutil
import sys
import time
import glob
from Bio import SeqIO

#   set all requests static params beforehand
PARSE_URL = "https://data.orthodb.org/current/fasta?"
SEARCH_URL = "https://data.orthodb.org/current/search?"
VERSION_URL = "https://data.orthodb.org/current/orthodb_release_id"

SEARCH_URL_ARGS = {
    "universal": "0.9",
    "singlecopy": "0.9",
    "take": "5000"
}
MAX_NB_QUERIES_PER_BLOCK = 50
NUM_JOBS = 2 # no of simultaneous runs. Kept modest: OrthoDB 403s ("too high request rate") well below 50
WAIT = 10
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60


class OrthoDBRequestError(Exception):
    """Raised when an OrthoDB request fails after all retries are exhausted."""


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
    num_ranks = len(taxa_dict)
    logging.info(f"Getting OrthoDB data: {num_ranks} taxonomic rank(s) available in lineage, most specific first")
    clusters = []

    for rank_num, (taxid, rank) in enumerate(taxa_dict.items(), start=1):
        data_found = False
        query_terms = SEARCH_URL_ARGS.copy()
        query_terms.update(
            {
                "level": str(taxid),
                "species": str(taxid)
            }
        )

        while not data_found:
            logging.info(f"[{rank_num}/{num_ranks}] Searching OrthoDB entries for taxid {taxid} (rank: {rank})")
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

                #   OrthoDB reports the protein count of each cluster ("gene_count")
                #   in "bigdata", so we can log the expected download size upfront
                bigdata = data.get("bigdata", [])
                if bigdata:
                    expected_proteins = sum(
                        int(entry["gene_count"])
                        for entry in bigdata
                        if str(entry.get("gene_count", "")).isdigit()
                    )
                    logging.info(
                        f"Taxid {taxid}: ~{expected_proteins} protein sequences expected "
                        f"across {len(bigdata)} clusters, before deduplication"
                    )

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
    """
    Get sequence fasta per orthoDB cluster.
    Raises OrthoDBRequestError if the download does not succeed
    after MAX_RETRIES retries.
    """
    #   example cluster ID 10626at5690
    taxid = cluster.split('at')[1]
    params = {
        "id": str(cluster),
    }
    orthodb_dir = f"{taxid}_sequences"
    fasta_file_path = os.path.join(orthodb_dir, f"{cluster}.faa")
    if not os.path.exists(orthodb_dir):
        os.makedirs(orthodb_dir, exist_ok=True)
    #   a leftover empty/truncated file from a previous failed attempt should
    #   not be mistaken for a completed download
    if os.path.exists(fasta_file_path) and os.path.getsize(fasta_file_path) > 0:
        logging.info(f"Cluster {cluster}: already downloaded, skipping")
        return
    logging.info(f"Cluster {cluster}: starting download")
    response = query_orthodb(params, download=True)
    if not response.content.lstrip().startswith(b">"):
        body_preview = response.content[:200]
        raise OrthoDBRequestError(
            f"OrthoDB returned a non-FASTA response for cluster {cluster} "
            f"(body: {body_preview!r})"
        )
    n_seqs = response.content.count(b">")
    with open(fasta_file_path, 'wb') as fasta:
        fasta.write(response.content)
    logging.info(f"Cluster {cluster}: downloaded {n_seqs} protein sequence(s)")

def parallelize_jobs(clusters):
    with multiprocessing.Pool(NUM_JOBS) as pool:
        pool.map(get_sequences, clusters)

def query_orthodb(query_terms, search=False, download=False):
    if search:
        url = SEARCH_URL
    if download:
        url = PARSE_URL

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url=url, params=query_terms, timeout=REQUEST_TIMEOUT)
            if response.ok and response.content:
                if search:
                    #   validate the body is actually JSON before handing it back,
                    #   since OrthoDB can return a 200 with an HTML/empty error body
                    response.json()
                return response
            body_preview = response.text[:200].replace("\n", " ")
            last_error = f"HTTP {response.status_code}, body: {body_preview!r}"
        except requests.exceptions.JSONDecodeError:
            body_preview = response.text[:200].replace("\n", " ")
            last_error = (
                f"OrthoDB returned HTTP {response.status_code} but the body was not "
                f"valid JSON (body: {body_preview!r})"
            )
        except requests.exceptions.RequestException as e:
            last_error = f"{type(e).__name__}: {e}"

        if attempt < MAX_RETRIES:
            #   exponential backoff with jitter: with NUM_JOBS workers retrying
            #   in parallel, a fixed sleep just re-synchronizes them into the
            #   next rate-limit rejection, so decorrelate with randomness
            backoff = WAIT * (2 ** (attempt - 1)) + random.uniform(0, WAIT)
            logging.warning(
                f"OrthoDB request to {url} failed ({last_error}), retrying "
                f"({attempt}/{MAX_RETRIES}) in {backoff:.1f}s: {query_terms}"
            )
            time.sleep(backoff)

    raise OrthoDBRequestError(
        f"OrthoDB request to {url} with params {query_terms} failed after "
        f"{MAX_RETRIES} attempts. Last error: {last_error}"
    ) from None

def create_combined_fa(taxid, orthodb_ncbi_subfolder):
    """Combine orthodb clusters into single file per taxid. Retain unique orthoDB seqIDs. Keep only ID and
    remove description in fasta headers. Return the number of proteins written"""

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
    return len(ortho_ids)


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
        "--version", action="store_true", help="Show orthodb version number and exit"
    )
    args = parser.parse_args()

    if args.version:
        version_response = requests.get(VERSION_URL)
        version = version_response.text.strip('"')
        print(f"OrthoDB: {version}")
        return

    logging.basicConfig(level=logging.INFO)

    if not args.tax_file or not args.output_dir:
        parser.error("--tax_file and --output_dir are required unless --version is specified")

    if args.lineage_max == "default":
        max_lineage = None
    else:
        max_lineage = args.lineage_max

    try:
        taxa_dict = parse_taxa(args.tax_file)
        clusters = get_orthodb_data(taxa_dict, max_lineage)

        n_proteins = 0
        taxid = ""

        if clusters:
            if args.max_clusters:
                logging.info(f"Limit to first {args.max_clusters} clusters")
                clusters = clusters[:args.max_clusters]
            num_clusters = len(clusters)
            logging.info(f"Downloading sequences for {num_clusters} OrthoDB clusters")
            start = 0
            num_blocks = -(-num_clusters // MAX_NB_QUERIES_PER_BLOCK)  # ceil division
            block_num = 0

            while start < num_clusters:
                end = min(start + MAX_NB_QUERIES_PER_BLOCK, num_clusters)
                block_num += 1
                groups = clusters[start:end]
                logging.info(
                    f"Batch {block_num}/{num_blocks}: fetching clusters {start + 1}-{end} "
                    f"of {num_clusters} (using {NUM_JOBS} parallel workers)"
                )
                parallelize_jobs(groups)
                logging.info(f"Batch {block_num}/{num_blocks} complete")
                start = end

                time.sleep(WAIT)

            for folder in glob.glob("*_sequences"):
                taxid = str(folder).split('_')[0]
                n_proteins_for_taxid = create_combined_fa(taxid, folder)
                logging.info(f"Taxid {taxid}: combined {n_proteins_for_taxid} unique protein sequence(s)")
                n_proteins += n_proteins_for_taxid
    except OrthoDBRequestError as e:
        logging.error(f"Aborting: could not fetch data from OrthoDB. {e}")
        sys.exit(1)

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
