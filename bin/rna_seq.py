#!/usr/bin/env python3

import query_ena_transcriptome
import logging
import argparse
import csv

logging.basicConfig(level=logging.INFO)

#check if we need to include thse after sourmash
#MAX_NB_SAME_DESCRIPTORS = 6
#MAX_NB_TOTAL_FILES = 3
#check which descriptor is used - sample name in this case
#check if filtering of descripions for RNAs is needed

def parse_taxa(taxa_file):
    logging.info("Parsing taxa lineages from file")
    tax_dict = {}
    with open(taxa_file, 'r') as taxa:
        for line in taxa:
            data = line.rstrip().split('\t')
            #   rank: taxid
            tax_dict[data[1]] = data[0]
    return tax_dict

import logging
import query_ena_transcriptome

def find_transcriptome_data(tax_ranks, preferred_rank=False, order_by_smallest=False):
    """Use preferred rank if given by user. Iterate through taxa until transcriptome data is found."""
    
    if preferred_rank:
        taxid = next((taxid for taxid, rank in tax_ranks.items() if rank == preferred_rank), None)
        ena = query_ena_transcriptome.EnaMetadata(taxid, order_by_smallest)
        transciptome_metadata = ena.query_ena()
        if len(transciptome_metadata):
            return transciptome_metadata
        else:
            logging.info(f"No transcriptome data found at the preferred rank {preferred_rank}")

    for taxid, rank in tax_ranks.items():
        ena = query_ena_transcriptome.EnaMetadata(taxid, order_by_smallest)
        transciptome_metadata = ena.query_ena()
        if transciptome_metadata:
            return transciptome_metadata

    logging.info("No transcriptome data found at any taxonomic rank")
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from orthodb")
    parser.add_argument(
        "-t", "--tax_file", type=str, help="File with taxonomic lineage information", required=True
    )
    parser.add_argument(
        "-o", "--output_file", type=str, help="output file name", required=True
    )
    parser.add_argument(
        "-r", "--rank", type=str, help="Preferred taxonomic rank to search for transcriptomic data", required=False
    )
    parser.add_argument(
        "-s", "--select_smallest", action='store_true', help="Order from smallest sequence file to largest file size", required=False, default=False
    )
    args = parser.parse_args()

    if args.rank == "default":
        rank = None
    else:
        rank = args.rank
        
    taxa_dict = parse_taxa(args.tax_file)
    transcriptome_metadata = find_transcriptome_data(taxa_dict, rank, order_by_smallest=args.select_smallest)

    logging.info(f"Writing metadata to output file {args.output_file}")
    with open(args.output_file, mode='w', newline='') as outfile:
        if not transcriptome_metadata:
            outfile.write("None")
        else:
            headers = transcriptome_metadata[0].keys()
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()
            for sample in transcriptome_metadata:
                writer.writerow(sample)


