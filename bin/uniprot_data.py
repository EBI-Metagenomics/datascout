#!/usr/bin/env python3

import requests
import os
import argparse
import logging
from Bio import SeqIO

URL = "https://rest.uniprot.org/uniprotkb/stream?"

SEARCH_URL_ARGS = {
    "compressed" : "false",
    "format": "fasta"
}

evidence_1 = "existence:1"
evidence_2 = "existence:1 OR existence:2"
evidence_3 = "existence:1 OR existence:2 OR existence:3"

def parse_taxa(taxa_file):
    logging.info("Parsing taxa lineages from file")
    tax_dict = {}
    with open(taxa_file, 'r') as taxa:
        for line in taxa:
            data = line.rstrip().split('\t')
            #   rank: taxid
            tax_dict[data[1]] = data[0]
    return tax_dict

def build_query(taxid, evidence_level, swissprot_only=False):
    if evidence_level == '1':
        query = f"(taxonomy_id={taxid} AND ({evidence_1}))"
    elif evidence_level == '2':
        query = f"(taxonomy_id={taxid} AND ({evidence_2}))"
    elif evidence_level == "3":
        query = f"(taxonomy_id={taxid} AND ({evidence_3}))"
    #   default is evidence 1 and 2
    else:
        query = f"(taxonomy_id={taxid} AND ({evidence_2}))"
    
    if swissprot_only:
        query += " AND (reviewed:true)"

    params = SEARCH_URL_ARGS.copy()
    params["query"] = query

    try:
        response = requests.get(URL, params)
        return response
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

def query_uniprot(taxa_dict, output_dir, preferred_rank=None, preferred_evidence=None, swissprot_only=False):
    """query uniprot per taxonmic rank. Stop when non-empty result 
    is returned. Use rank if provided by user"""
    logging.info("Getting UniProt data")

    if preferred_rank:
        taxid = next((taxid for taxid, rank in taxa_dict.items() if rank == preferred_rank), None)
        response = build_query(taxid, preferred_evidence, swissprot_only)
        if response.status_code == 200:
            uniprot_fasta_file = os.path.join(output_dir, f"{taxid}_uniprot_raw.faa")
            with open(uniprot_fasta_file, 'wb') as outfile:
                outfile.write(response.content)
            return uniprot_fasta_file, taxid
        if response.status_code == 204:
            logging.info(f"No uniprot data found at the preferred rank {preferred_rank} with taxid {taxid}")
            return None

    for taxid, rank in taxa_dict.items():
        logging.info(f"Searching UniProt entries for taxid {taxid}")
        response = build_query(taxid, preferred_evidence, swissprot_only)
        if response.status_code == 200:
            uniprot_fasta_file = os.path.join(output_dir, f"{taxid}_uniprot_raw.faa")
            with open(uniprot_fasta_file, 'wb') as outfile:
                outfile.write(response.content)
                return uniprot_fasta_file, taxid
        if response.status_code == 204:
            logging.info(f"No data found for taxid {taxid}. Moving to next rank.")
    
    logging.info("No UniProt data found at any taxonomic rank.")
    return None

#   #>sp|Q1PCB1|PDXK_BOMMO Pyridoxal kinase OS=Bombyx mori OX=7091 GN=Pdxk PE=1 SV=1
def reformat_fasta(fasta_path, output_dir, taxid):
    """Simplify input ID to identifier and version labelled with SV. Elminate duplicates
    Input:>sp|Q1PCB1|PDXK_BOMMO Pyridoxal kinase OS=Bombyx mori OX=7091 GN=Pdxk PE=1 SV=1
    Output: >Q1PCB1.1"""
    uniprot_ids = set()
    uniprot_fasta_file = os.path.join(output_dir, f"{taxid}_uniprot_proteins.faa")
    with open(uniprot_fasta_file, 'w') as outfile:
        for entry in SeqIO.parse(fasta_path, "fasta"):
            id = entry.id.split('|')[1]
            desc = entry.description.split("=")[-1]
            seq_id = f"{id}.{desc}"
            if seq_id not in uniprot_ids:
                outfile.write(f">{seq_id}\n")
                outfile.write(f"{str(entry.seq)}\n")
                uniprot_ids.add(seq_id)

def get_uniprot_version():
    """Fetch UniProt release version and date from headers"""
    response = requests.get(
        URL,
        params={"query": "taxonomy_id:9606", "format": "fasta", "size":0},  
        stream=True,
    )
    if response.status_code == 200:
        release = response.headers.get("X-UniProt-Release", "unknown")
        return release
    else:
        raise RuntimeError(f"Failed to fetch UniProt version (status {response.status_code})")


def main():
    parser = argparse.ArgumentParser(description="Fetch data from orthodb")
    parser.add_argument(
        "-t", "--tax_file", type=str, help="File with taxonomic lineage information"
    )
    parser.add_argument(
        "-o", "--output_dir", type=str, help="output directory"
    )
    parser.add_argument(
        "-e", "--evidence", type=str, help="Highest evidence level to search Uniprot proteins"
    )
    parser.add_argument(
        "-r", "--rank", type=str, help="Preferred taxonomic rank to search for proteins"
    )
    parser.add_argument(
        "--swissprot_only", action="store_true", help="Search only SwissProt reviewed entries", default=False
    )
    parser.add_argument(
        "--version", action="store_true", help="Print UniProt release version and exit"
    )
    args = parser.parse_args()


    if args.version:
        version = get_uniprot_version()
        print(f"\tUniProt: {version}")
        return  

    logging.basicConfig(level=logging.INFO)

    if not args.tax_file or not args.output_dir:
        parser.error("--tax_file and --output_dir are required unless --version is specified")

    os.makedirs(args.output_dir, exist_ok=True)
    if args.rank == "default":
        rank = None
    else:
        rank = args.rank
    taxa_dict = parse_taxa(args.tax_file)
    raw_file_path, taxid = query_uniprot(taxa_dict, args.output_dir, rank, args.evidence, args.swissprot_only)
    reformat_fasta(raw_file_path, args.output_dir, taxid)



if __name__ == "__main__":
    main()