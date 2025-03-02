#!/usr/bin/env python3

import sys
import requests
import logging
from Bio import SeqIO
import re 
import argparse

logging.basicConfig(level=logging.INFO)

def query_ena(genome_accession, genome_file=None):
    reheaded_fasta_file = f"{genome_accession}_reheaded_assembly.fasta"
    if not args.genome_file:
        fasta_file_path = f"{genome_accession}_original_genome.fasta"
        url = f"https://www.ebi.ac.uk/ena/browser/api/fasta/{genome_accession}?download=true&gzip=false"
        try:
            response = requests.get(url)
            with open(fasta_file_path, 'wb') as fasta:
                fasta.write(response.content)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise
    else:
        fasta_file_path = genome_file
    with open(reheaded_fasta_file, 'w') as outfile:
        for entry in SeqIO.parse(fasta_file_path, "fasta"):
            cleaned_header = re.sub(r'[^a-zA-Z0-9_.:,\-()]', '_', entry.id)
            outfile.write(f">{cleaned_header}\n")
            outfile.write(f"{str(entry.seq)}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from orthodb")
    parser.add_argument(
        "-g", "--genome_id", type=str, help="Genome identifier", required=True
    )
    parser.add_argument(
        "-f", "--genome_file", type=str, help="Genome file path if exists", required=False
    )
    args = parser.parse_args()

    if args.genome_file == "default":
        genome_file = None
    else:
        genome_file = args.genome_file

    query_ena(args.genome_id, genome_file)
