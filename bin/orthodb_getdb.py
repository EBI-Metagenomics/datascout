#!/usr/bin/env python3

"""Check the OrthoDB duckdb database the pipeline reads protein sequences from, building it
if asked to.

The requested --release has to be the one OrthoDB currently serves (or "current"), otherwise
the script exists with error. Given --input, that database is checked to hold the served release,
failing when it's older.
Without --input, a database is built under --db_dir/<release>
(reusing one already there for that release rather than rebuilding it).

Building turns the OrthoDB data dump into a database BUILD_ORTHODB_FASTA can query, instead of
downloading one cluster at a time from the OrthoDB API. Only two files of the dump are needed,
OG2genes and og_aa_fasta, and they are downloaded when not provided.
"""

import argparse
import gzip
import logging
import os
import re
import shlex
import subprocess
import tempfile
from pathlib import Path

import duckdb
import requests
from requests.adapters import HTTPAdapter, Retry

#   OrthoDB serves a release, v12.2, under the API endpoint of its major version, v12
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
    """The part of a release OrthoDB puts in the API URL, v12.2 to v12. "current" (accepted by
    --release as an alias for the latest OrthoDB release) passes through unchanged"""
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
        urls[name] = dump_url + Path(found.group(1)).name
    return urls


def download(url, download_dir):
    """Fetch URL into download_dir, skipping a file that is already there. Returns its path"""
    file_path = Path(download_dir) / Path(url).name
    if file_path.exists():
        logging.info(f"{file_path} is already downloaded")
        return file_path
    logging.info(f"Downloading {url}, this takes a while")
    with SESSION.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with file_path.open('wb') as dump_file:
            for chunk in response.iter_content(chunk_size=2**20):
                dump_file.write(chunk)
    return file_path


def get_served_release(release):
    """Fetch the OrthoDB release currently served for this version."""
    response = SESSION.get(RELEASE_URL.format(major=major_of(release)), timeout=60)
    response.raise_for_status()
    return response.text.strip().strip('"')


def db_release(orthodb_db):
    """The release recorded in a database's meta table, None when it has none"""
    with duckdb.connect(str(orthodb_db), read_only=True) as con:
        try:
            return con.execute("SELECT release FROM meta").fetchone()[0]
        except duckdb.CatalogException:
            return None


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
        "-i", "--input", type=Path, help="""Existing OrthoDB database to reuse. Fails unless it
        holds the release OrthoDB API currently serves"""
    )
    parser.add_argument(
        "-o", "--output", type=str, help="Filename for the newly build database [default: orthodb_<release>.duckdb]"
    )
    parser.add_argument(
        "--release", type=str, required=True, help="""OrthoDB release requested, e.g. v12.2, or
        "current" for the latest OrthoDB release. Fails when it isn't the release OrthoDB
        currently serves"""
    )
    parser.add_argument(
        "--db_dir", type=Path, default=".", help="""Directory to create the database in.
        The database is built in the <release> subdirectory, reusing one already there"""
    )
    parser.add_argument(
        "--og2genes", type=Path, help="OG2genes file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--og_aa_fasta", type=Path, help="og_aa_fasta file of the dump. Downloaded when not given"
    )
    parser.add_argument(
        "--download_dir", type=Path, default=".", help="where missing dump files are downloaded to [default: .]"
    )
    parser.add_argument(
        "--threads", type=int, help="Cores the task was allocated"
    )
    parser.add_argument(
        "--memory", type=str, help="Memory the task was allocated, e.g. 28GB"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    #   OrthoDB only ever hosts the dump of the release it currently serves for a major version
    #   (v12.2 under the v12 API), and the API answers cluster queries for that release only, so
    #   a requested release that has fallen behind can't be used together with the API
    release = get_served_release(args.release)
    #   "current" always resolves to whatever was just fetched, so it's never "fallen behind"
    if args.release not in (release, "current"):
        rerun = (f"rerun with --orthodb_version {release} (or current) and with --orthodb_db "
                 f"pointing to an OrthoDB {release} database, or unset to build one"
                 if args.input else f"rerun with --orthodb_version {release} (or current)")
        raise SystemExit(f"OrthoDB API now serves {release}, the requested {args.release} is "
                         f"obsolete: {rerun}")

    if args.input:
        if not args.input.exists():
            raise SystemExit(f"{args.input} does not exist: point --orthodb_db to an existing "
                             f"database, or unset it to build OrthoDB {release}")
        input_release = db_release(args.input)
        if input_release != release:
            raise SystemExit(f"{args.input} holds OrthoDB {input_release or 'of unknown release'}, "
                             f"but OrthoDB API serves {release}: rerun with --orthodb_version "
                             f"{release} (or current) and --orthodb_db unset to build it")
        logging.info(f"{args.input} holds OrthoDB {release}, reusing it")
        print(args.input.resolve())
        return

    canonical_db = args.db_dir / release / Path(args.output or f"orthodb_{release}.duckdb").name

    #   an earlier run may already have built the very release this one now needs
    if canonical_db.exists():
        logging.info(f"OrthoDB {release} is already built at {canonical_db}, reusing it")
        print(canonical_db.resolve())
        return

    og2genes_file, og_aa_fasta_file = args.og2genes, args.og_aa_fasta
    if not og2genes_file or not og_aa_fasta_file:
        args.download_dir.mkdir(parents=True, exist_ok=True)
        urls = dump_urls(release)
        og2genes_file = og2genes_file or download(urls["og2genes"], args.download_dir)
        og_aa_fasta_file = og_aa_fasta_file or download(urls["og_aa_fasta"], args.download_dir)

    check_dump_files(og2genes_file, og_aa_fasta_file)

    canonical_db.parent.mkdir(parents=True, exist_ok=True)
    with connect(str(canonical_db), args.threads, args.memory) as con:
        build_og2genes(con, str(og2genes_file))
        build_proteins(con, str(og_aa_fasta_file))
        con.execute("CREATE TABLE meta AS SELECT ? AS release", [release])
        n_pairs = con.execute("SELECT count(*) FROM og2genes").fetchone()[0]
        n_proteins = con.execute("SELECT count(*) FROM proteins").fetchone()[0]

    logging.info(f"OrthoDB {release}: built new database at {canonical_db}, "
                f"{n_pairs} OG to gene pairs and {n_proteins} proteins")
    print(canonical_db.resolve())


if __name__ == "__main__":
    main()
