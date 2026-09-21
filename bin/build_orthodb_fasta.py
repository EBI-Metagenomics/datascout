#!/usr/bin/env python3

import argparse
import logging
import duckdb


def parse_clusters(clusters_file):
    """cluster ids listed by resolve_orthodb_taxon.py, one per line after the header"""
    with open(clusters_file, 'r') as clusters:
        header = clusters.readline()
        if not header.startswith("cluster_id"):
            raise ValueError(f"{clusters_file} does not start with the cluster_id header")
        return [line.split('\t')[0] for line in clusters if line.strip()]


def dump_release(orthodb_db):
    """OrthoDB release recorded by accessory/orthodb.py when it built the database"""
    with duckdb.connect(orthodb_db, read_only=True) as con:
        return con.execute("SELECT release FROM meta").fetchone()[0]


CLUSTER_PROTEINS = """
    SELECT proteins.gene_id, proteins.seq
    FROM og2genes JOIN proteins USING (gene_id)
    WHERE og2genes.og_id IN (SELECT * FROM UNNEST(?))
"""


def connect(orthodb_db, threads=None, memory=None, temp_directory="."):
    """Open the database within the resources the task was given. Duckdb otherwise sizes itself
    from the whole machine, and under a scheduler it is killed rather than spilling to disk"""
    config = {"temp_directory": temp_directory}
    if threads:
        config["threads"] = threads
    if memory:
        config["memory_limit"] = memory
    return duckdb.connect(orthodb_db, read_only=True, config=config)


def count_proteins(clusters, orthodb_db, threads=None, memory=None):
    """how many proteins the given clusters hold, without materialising any of them"""
    with connect(orthodb_db, threads, memory) as con:
        return con.execute(f"SELECT count(*) FROM ({CLUSTER_PROTEINS})", [clusters]).fetchone()[0]


def write_combined_fa(clusters, orthodb_db, fasta_file_path, threads=None, memory=None):
    """Write the proteins of the given clusters, joining OG membership and sequences in the
    database built by accessory/orthodb.py. Return the number of proteins written"""
    n_proteins = 0
    with connect(orthodb_db, threads, memory) as con:
        query = con.execute(CLUSTER_PROTEINS, [clusters])
        with open(fasta_file_path, 'w') as outfile:
            #   fetch in batches, a taxon can hold well over a million sequences
            while batch := query.fetchmany(100000):
                for gene, sequence in batch:
                    outfile.write(f">{gene}\n{sequence}\n")
                n_proteins += len(batch)
    return n_proteins


def main():
    parser = argparse.ArgumentParser(
        description="Compose the combined protein fasta of one OrthoDB taxon from a local OrthoDB database"
    )
    parser.add_argument(
        "--taxid", type=str, help="OrthoDB taxon the clusters were resolved to"
    )
    parser.add_argument(
        "--clusters_file", type=str, help="Cluster list written by resolve_orthodb_taxon.py"
    )
    parser.add_argument(
        "-o", "--output", type=str, help="combined fasta to write"
    )
    parser.add_argument(
        "--orthodb_db", type=str, required=True, help="""Path to the OrthoDB database built by
        accessory/orthodb.py, the source of the protein sequences"""
    )
    parser.add_argument(
        "--threads", type=int, default=None, help="Cores the task was allocated"
    )
    parser.add_argument(
        "--memory", type=str, default=None, help="""Memory the task was allocated, e.g. 36GB. Duckdb
        does not see the scheduler's limit, so without this it is killed instead of spilling to disk"""
    )
    parser.add_argument(
        "--min_proteins", type=int, default=0, help="""Minimum number of proteins required to keep the
        taxon. Below it no fasta is produced and the taxon is traced in low_proteins.csv, which drops
        every genome that resolved to it"""
    )
    parser.add_argument(
        "--version", action="store_true", help="Show orthodb version number and exit"
    )
    args = parser.parse_args()

    if args.version:
        print(f"OrthoDB: {dump_release(args.orthodb_db)}")
        return

    logging.basicConfig(level=logging.INFO)

    if not args.taxid or not args.clusters_file or not args.output:
        parser.error("--taxid, --clusters_file and --output are required unless --version is specified")

    clusters = parse_clusters(args.clusters_file)
    logging.info(f"Composing fasta for taxid {args.taxid} from {len(clusters)} clusters")

    #   counting first keeps a dropped taxon from writing a fasta only to have it thrown away
    if args.min_proteins:
        n_proteins = count_proteins(clusters, args.orthodb_db, args.threads, args.memory)
        if n_proteins < args.min_proteins:
            logging.warning(f"Dropping taxid {args.taxid}: {n_proteins} proteins, "
                            f"minimum is {args.min_proteins}")
            with open('low_proteins.csv', 'w') as trace:
                trace.write(f"{args.taxid},{n_proteins}\n")
            return

    n_proteins = write_combined_fa(clusters, args.orthodb_db, args.output, args.threads, args.memory)
    logging.info(f"Wrote {n_proteins} proteins to {args.output}")


if __name__ == "__main__":
    main()
