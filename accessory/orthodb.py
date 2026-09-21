#!/usr/bin/env python3

"""Build the OrthoDB duckdb database consumed by the pipeline.

This script is not part of the workflow. It is run once per OrthoDB release to turn the
data dump into a database that NCBI_ORTHODB can query, instead of scanning the dump files
on every sample. Only two files of the dump are needed, OG2genes and og_aa_fasta, and they
are downloaded when not provided.

Dependencies are pinned in accessory/orthodb.requirements.txt.
"""

import argparse
import gzip
import logging
import os
import re
import shlex
import subprocess
import tempfile
import duckdb
import requests

DUMP_URL = "https://data.orthodb.org/{odb_version}/download/odb_data_dump/"
RELEASE_URL = "https://data.orthodb.org/{odb_version}/orthodb_release_id"
DUMP_FILES = {"og2genes": r"\S+_OG2genes\.tab\.gz", "og_aa_fasta": r"\S+_og_aa_fasta\.gz"}


def dump_urls(odb_version):
    """the download url of each needed file, as listed by OrthoDB for that version"""
    dump_url = DUMP_URL.format(odb_version=odb_version)
    listing = requests.get(dump_url)
    listing.raise_for_status()
    urls = {}
    for name, pattern in DUMP_FILES.items():
        found = re.search(rf"""href=["']?({pattern})""", listing.text)
        if not found:
            raise RuntimeError(f"no {name} file listed at {dump_url}")
        urls[name] = dump_url + os.path.basename(found.group(1))
    return urls


def download(url, download_dir):
    """fetch url into download_dir, skipping a file that is already there"""
    file_path = os.path.join(download_dir, os.path.basename(url))
    if os.path.exists(file_path):
        logging.info(f"{file_path} is already downloaded")
        return file_path
    logging.info(f"Downloading {url}, this takes a while")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(file_path, 'wb') as dump_file:
            for chunk in response.iter_content(chunk_size=2**20):
                dump_file.write(chunk)
    return file_path


def release_of(og2genes_file):
    """OrthoDB release as named by the dump files, e.g. odb12v2_OG2genes.tab.gz"""
    return os.path.basename(og2genes_file).split('_')[0]


def api_release_of(odb_version):
    """The release id the API reports for this version, stored so the pipeline can compare it
    with the one served at run time."""
    response = requests.get(RELEASE_URL.format(odb_version=odb_version))
    response.raise_for_status()
    return response.text.strip().strip('"')


def check_dump_files(og2genes_file, og_aa_fasta_file):
    """fail before the long load when a file is missing, or when the two were swapped: the
    OG2genes dump is two tab separated columns starting with an OG id, the fasta starts with >"""
    for file_path in (og2genes_file, og_aa_fasta_file):
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)

    with gzip.open(og2genes_file, 'rt') as og2genes:
        first_line = og2genes.readline().rstrip('\n')
    columns = first_line.split('\t')
    if len(columns) != 2 or 'at' not in columns[0]:
        raise ValueError(f"{og2genes_file} is not the OG2genes dump, "
                         f"its first line is {first_line[:80]!r}")

    with gzip.open(og_aa_fasta_file, 'rt') as og_aa_fasta:
        first_line = og_aa_fasta.readline()
    if not first_line.startswith('>'):
        raise ValueError(f"{og_aa_fasta_file} is not the og_aa_fasta dump, "
                         f"its first line is {first_line[:80]!r}")


def build_og2genes(con, og2genes_file):
    """OG to gene correspondence, read straight from the gzipped dump"""
    logging.info(f"Loading {og2genes_file}")
    con.execute("""
        CREATE TABLE og2genes AS
        SELECT column0 AS og_id, column1 AS gene_id
        FROM read_csv(?, delim='\t', header=false, quote='', escape='',
                      columns={'column0':'VARCHAR','column1':'VARCHAR'})
    """, [og2genes_file])


def build_proteins(con, og_aa_fasta_file):
    """Protein sequences. The dump holds one header plus one unwrapped sequence per record,
    so paste turns each record into a row before duckdb reads it. The organism id of the
    second column is dropped, being the gene id prefix already"""
    logging.info(f"Loading {og_aa_fasta_file}")
    with tempfile.TemporaryDirectory() as tmp_dir:
        fifo = os.path.join(tmp_dir, "records.tsv")
        os.mkfifo(fifo)
        paste = subprocess.Popen(
            f"set -o pipefail; gzip -dc {shlex.quote(og_aa_fasta_file)} | paste - - > {shlex.quote(fifo)}",
            shell=True, executable="/bin/bash"
        )
        con.execute("""
            CREATE TABLE proteins AS
            SELECT substr(column0, 2) AS gene_id, column2 AS seq
            FROM read_csv(?, delim='\t', header=false, quote='', escape='',
                          columns={'column0':'VARCHAR','column1':'VARCHAR','column2':'VARCHAR'})
        """, [fifo])
        if paste.wait() != 0:
            raise RuntimeError(f"reading {og_aa_fasta_file} failed, the database is incomplete")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "-o", "--output", type=str, default="orthodb.duckdb", help="database to write [default: orthodb.duckdb]"
    )
    parser.add_argument(
        "--og2genes", type=str, default="", help="OG2genes file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--og_aa_fasta", type=str, default="", help="og_aa_fasta file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--download_dir", type=str, default=".", help="where missing dump files are downloaded to [default: .]"
    )
    parser.add_argument(
        "--odb_version", type=str, default="v12", help="""OrthoDB version to download the dump from,
        as it appears in the data.orthodb.org path, e.g. v11 or current [default: v12]"""
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if os.path.exists(args.output):
        parser.error(f"{args.output} exists, remove it or pick another --output")

    og2genes_file, og_aa_fasta_file = args.og2genes, args.og_aa_fasta
    if not og2genes_file or not og_aa_fasta_file:
        os.makedirs(args.download_dir, exist_ok=True)
        urls = dump_urls(args.odb_version)
        og2genes_file = og2genes_file or download(urls["og2genes"], args.download_dir)
        og_aa_fasta_file = og_aa_fasta_file or download(urls["og_aa_fasta"], args.download_dir)

    check_dump_files(og2genes_file, og_aa_fasta_file)

    release = release_of(og2genes_file)
    with duckdb.connect(args.output) as con:
        build_og2genes(con, og2genes_file)
        build_proteins(con, og_aa_fasta_file)
        con.execute("CREATE TABLE meta AS SELECT ? AS release, ? AS api_release",
                    [release, api_release_of(args.odb_version)])
        n_pairs = con.execute("SELECT count(*) FROM og2genes").fetchone()[0]
        n_proteins = con.execute("SELECT count(*) FROM proteins").fetchone()[0]

    logging.info(f"{args.output} holds OrthoDB {release}: "
                 f"{n_pairs} OG to gene pairs and {n_proteins} proteins")


if __name__ == "__main__":
    main()
