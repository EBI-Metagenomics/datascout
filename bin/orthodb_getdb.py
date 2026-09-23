#!/usr/bin/env python3

"""Build the OrthoDB duckdb database the pipeline reads protein sequences from.

Turns the OrthoDB data dump into a database BUILD_ORTHODB_FASTA can query, instead of
downloading one cluster at a time from the OrthoDB API. Only two files of the dump are
needed, OG2genes and og_aa_fasta, and they are downloaded when not provided.
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
from requests.adapters import HTTPAdapter, Retry

#   OrthoDB serves a release, v12.2, under the path of its major version, v12
DUMP_URL = "https://data.orthodb.org/{major}/download/odb_data_dump/"
RELEASE_URL = "https://data.orthodb.org/{major}/orthodb_release_id"
DUMP_FILES = {"og2genes": r"\S+_OG2genes\.tab\.gz", "og_aa_fasta": r"\S+_og_aa_fasta\.gz"}

SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])))


def connect(output, threads=None, memory=None):
    """Build within the resources the task was given. Duckdb otherwise sizes itself from the
    whole machine, and under a scheduler it is killed rather than spilling to disk"""
    config = {"temp_directory": "."}
    if threads:
        config["threads"] = threads
    if memory:
        config["memory_limit"] = memory
    return duckdb.connect(output, config=config)


def major_of(release):
    """the part of a release OrthoDB puts in a path, v12.2 to v12"""
    return release.split('.')[0]


def dump_urls(release):
    """The download URL of each needed file, as listed by OrthoDB for that version"""
    dump_url = DUMP_URL.format(major=major_of(release))
    listing = SESSION.get(dump_url, timeout=60)
    listing.raise_for_status()
    urls = {}
    for name, pattern in DUMP_FILES.items():
        found = re.search(rf"""href=["']?({pattern})""", listing.text)
        if not found:
            raise RuntimeError(f"no {name} file listed at {dump_url}")
        urls[name] = dump_url + os.path.basename(found.group(1))
    return urls


def download(url, download_dir):
    """Fetch URL into download_dir, skipping a file that is already there"""
    file_path = os.path.join(download_dir, os.path.basename(url))
    if os.path.exists(file_path):
        logging.info(f"{file_path} is already downloaded")
        return file_path
    logging.info(f"Downloading {url}, this takes a while")
    with SESSION.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(file_path, 'wb') as dump_file:
            for chunk in response.iter_content(chunk_size=2**20):
                dump_file.write(chunk)
    return file_path


def served_release(release):
    """The OrthoDB release currently served for this version."""
    response = SESSION.get(RELEASE_URL.format(major=major_of(release)), timeout=60)
    response.raise_for_status()
    return response.text.strip().strip('"')


def check_dump_files(og2genes_file, og_aa_fasta_file):
    """Fail before the long load when a file is missing, or when the two were swapped: the
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
        "-o", "--output", type=str, default="orthodb.duckdb", help="""database to write when
        [default: orthodb.duckdb]"""
    )
    parser.add_argument(
        "--release", type=str, default="v12.2", help="""OrthoDB release to use. [default: v12.2]"""
    )
    parser.add_argument(
        "--og2genes", type=str, default=None, help="OG2genes file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--og_aa_fasta", type=str, default=None, help="og_aa_fasta file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--download_dir", type=str, default=".", help="where missing dump files are downloaded to [default: .]"
    )
    parser.add_argument(
        "--threads", type=int, default=None, help="Cores the task was allocated."
    )
    parser.add_argument(
        "--memory", type=str, default=None, help="""Memory the task was allocated, e.g. 28GB."""
    )
    parser.add_argument(
        "--db_dir", type=str, default=None, help="""Directory holding one database per OrthoDB
        release. Used with --release, the database is built only when it is not there yet"""
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    served = served_release(args.release)
    if args.release != served:
        logging.warning(f"OrthoDB now serves {served}, this run uses {args.release}. "
                        f"Set the release to {served} to move on")

    output = args.output
    if args.db_dir:
        output = os.path.join(args.db_dir, args.release, os.path.basename(args.output))
        if os.path.exists(output):
            logging.info(f"OrthoDB {args.release} is already built at {output}")
            return # Do nothing, the database already exists
        logging.info(f"OrthoDB {args.release} is not built at {output} yet")
        os.makedirs(os.path.dirname(output), exist_ok=True)

    og2genes_file, og_aa_fasta_file = args.og2genes, args.og_aa_fasta
    if not og2genes_file or not og_aa_fasta_file:
        os.makedirs(args.download_dir, exist_ok=True)
        urls = dump_urls(args.release)
        og2genes_file = og2genes_file or download(urls["og2genes"], args.download_dir)
        og_aa_fasta_file = og_aa_fasta_file or download(urls["og_aa_fasta"], args.download_dir)

    check_dump_files(og2genes_file, og_aa_fasta_file)

    with connect(output, args.threads, args.memory) as con:
        build_og2genes(con, og2genes_file)
        build_proteins(con, og_aa_fasta_file)
        con.execute("CREATE TABLE meta AS SELECT ? AS release", [args.release])
        n_pairs = con.execute("SELECT count(*) FROM og2genes").fetchone()[0]
        n_proteins = con.execute("SELECT count(*) FROM proteins").fetchone()[0]

    logging.info(f"{output} holds OrthoDB {args.release}: "
                 f"{n_pairs} OG to gene pairs and {n_proteins} proteins")


if __name__ == "__main__":
    main()
