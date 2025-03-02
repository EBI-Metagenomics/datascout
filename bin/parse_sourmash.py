#!/usr/bin/env python3

import argparse
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def parse_containment(sourmash_file):
    """
    Read in sourmash containment file
    Keep runs which contribute >= 1% unique kmers mapped
    Return total containment and relevant runs
    """
    df = pd.read_csv(sourmash_file, usecols=['f_unique_to_query', 'filename'])
    filtered_df = df[df['f_unique_to_query'] > 0.01]
    total_containment = filtered_df['f_unique_to_query'].sum()
    run_dict = list(filtered_df['filename'])

    return total_containment, run_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get taxonomic lineage information using ete3")
    parser.add_argument(
        "-s", "--sourmash", type=str, help="CSV output from sourmash gather"
    )
    parser.add_argument(
        "-o", "--output_file", type=str, help="output file name"
    )
    args = parser.parse_args()

    containment, runs = parse_containment(args.sourmash)
    print(containment)

    if len(runs):
        with open(args.output_file, 'w') as matching_runs:
            for run in runs:
                matching_runs.write(f"{run.split('.')[0]}\n")

