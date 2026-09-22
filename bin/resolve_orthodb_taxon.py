#!/usr/bin/env python3

import argparse
import logging
import random
import sys
import time

import requests

#   set request static params beforehand
SEARCH_URL = "https://data.orthodb.org/current/search?"
VERSION_URL = "https://data.orthodb.org/current/orthodb_release_id"

SEARCH_URL_ARGS = {
    "universal": "0.9",
    "singlecopy": "0.9",
    "take": "5000"
}
WAIT = 10
MAX_RETRIES = 3
REQUEST_TIMEOUT = 60


class OrthoDBRequestError(Exception):
    """Raised when an OrthoDB search request fails after all retries are exhausted."""


def parse_taxa(taxa_file: str) -> dict[str, str]:
    """
    Read a tab-separated taxonomic lineage file and return a taxid-to-rank map.

    Each line has columns rank, taxid, name (no header), with the most specific
    rank listed first.
    Only the rank and taxid columns are used.

    Example file contents::

        species	5693	Trypanosoma cruzi
        subgenus	47570	Schizotrypanum
        genus	5690	Trypanosoma

    :param taxa_file: Path to the tab-separated lineage file.
    :return: Mapping of taxid to its rank name, in file order.
    """
    logging.info("Parsing taxa lineages from file")
    tax_dict = {}
    with open(taxa_file, 'r') as taxa:
        for line in taxa:
            data = line.rstrip().split('\t')
            #   rank: taxid
            tax_dict[data[1]] = data[0]
    return tax_dict

def resolve_taxon(taxa_dict: dict[str, str], max_lineage: str | None = None) -> tuple[str | None, list[tuple[str, str]] | None]:
    """
    Find the first taxonomic rank in the lineage that has OrthoDB ortholog groups.

    Walks the lineage most-specific-first, querying OrthoDB at each rank until
    one has ortholog groups or ``max_lineage`` is reached.

    :param taxa_dict: Mapping of taxid to rank name, most specific first (as
        returned by :func:`parse_taxa`).
    :param max_lineage: Least specific rank to search up to; stop early once
        this rank is reached with no match. If not given, the whole lineage
        is searched.
    :return: A ``(resolved_taxid, clusters)`` tuple, where ``clusters`` is a
        list of ``(cluster_id, gene_count)`` tuples. ``(None, None)`` if no
        rank in scope has data.
    """
    num_ranks = len(taxa_dict)
    logging.info(f"Resolving OrthoDB taxon: {num_ranks} taxonomic rank(s) available in lineage, most specific first")

    for rank_num, (taxid, rank) in enumerate(taxa_dict.items(), start=1):
        query_terms = SEARCH_URL_ARGS.copy()
        query_terms.update(
            {
                "level": str(taxid),
                "species": str(taxid)
            }
        )

        logging.info(f"[{rank_num}/{num_ranks}] Searching OrthoDB entries for taxid {taxid} (rank: {rank})")
        response = query_orthodb_search(query_terms)
        data = response.json()

        if data["count"] == "0":
            logging.info(f"No data found for taxid {taxid}. Moving to next rank.")
            if max_lineage and rank == max_lineage:
                logging.error(f"No OrthoDB groups found up to taxonomic rank {max_lineage}, as provided by user.")
                return None, None
            continue

        cluster_ids = data["data"]
        gene_counts = {entry["id"]: entry.get("gene_count", "") for entry in data.get("bigdata", [])}
        clusters = [(cluster_id, gene_counts.get(cluster_id, "")) for cluster_id in cluster_ids]
        logging.info(f"Found {data['count']} clusters for taxid {taxid}")

        total_proteins = sum(
            int(gene_count) for _, gene_count in clusters if str(gene_count).isdigit()
        )
        logging.info(
            f"Taxid {taxid}: ~{total_proteins} protein sequences expected "
            f"across {len(clusters)} clusters"
        )

        return taxid, clusters

    logging.error(
        f"No OrthoDB groups found up to taxonomic rank {max_lineage}, as provided by user."
        if max_lineage else "No OrthoDB groups found at any taxonomic rank."
    )
    return None, None


def query_orthodb_search(query_terms: dict[str, str]) -> requests.Response:
    """
    Query OrthoDB's search endpoint, retrying on failure with exponential backoff.

    Retries up to ``MAX_RETRIES`` times (with jittered exponential backoff
    between attempts) on request errors, non-OK responses, or a response
    body that isn't valid JSON, since OrthoDB can return HTTP 200 with an
    HTML/empty error body.

    :param query_terms: Query parameters to send to ``SEARCH_URL``.
    :return: The successful response, with a validated JSON body.
    :raises OrthoDBRequestError: If all retry attempts are exhausted.
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url=SEARCH_URL, params=query_terms, timeout=REQUEST_TIMEOUT)
            if response.ok and response.content:
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
            #   exponential backoff with jitter to decorrelate retries across concurrently-running samples
            backoff = WAIT * (2 ** (attempt - 1)) + random.uniform(0, WAIT)
            logging.warning(
                f"OrthoDB search request failed ({last_error}), retrying "
                f"({attempt}/{MAX_RETRIES}) in {backoff:.1f}s: {query_terms}"
            )
            time.sleep(backoff)

    raise OrthoDBRequestError(
        f"OrthoDB search request with params {query_terms} failed after "
        f"{MAX_RETRIES} attempts. Last error: {last_error}"
    ) from None



def main():
    parser = argparse.ArgumentParser(
        description="Resolve which OrthoDB taxon a sample's lineage maps to by walking ranks "
                     "most-specific-first until one has ortholog groups, and list its clusters. "
                     "Writes <sample_id>_taxon.txt and <sample_id>_clusters.tsv, or "
                     "no_orthodb_taxon.csv if no rank matched"
    )
    parser.add_argument(
        "-t", "--tax_file", type=str, help="File with taxonomic lineage information"
    )
    parser.add_argument(
        "-l", "--lineage_max", type=str, help="""Least specific lineage rank to search up to.
        The default behaviour is to traverse the taxonomic tree until orthologous groups are found""", required=False
    )
    parser.add_argument(
        "--max_clusters", type=int, default=None, help="Limit the number of clusters listed (mainly for testing)"
    )
    parser.add_argument(
        "--sample_id", type=str, default="", help="Sample identifier used when naming output files"
    )
    parser.add_argument(
        "--version", action="store_true", help="Show orthodb version number and exit"
    )
    args = parser.parse_args()

    if args.version:
        try:
            version_response = requests.get(VERSION_URL, timeout=REQUEST_TIMEOUT)
            version_response.raise_for_status()
            version = version_response.text.strip('"')
        except requests.exceptions.RequestException as e:
            print(f"OrthoDB: unknown (failed to fetch version: {e})")
            return
        print(f"OrthoDB: {version}")
        return

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.tax_file or not args.sample_id:
        parser.error("--tax_file and --sample_id are required unless --version is specified")

    max_lineage = None if args.lineage_max == "default" else args.lineage_max

    try:
        taxa_dict = parse_taxa(args.tax_file)
        taxid, clusters = resolve_taxon(taxa_dict, max_lineage)
    except OrthoDBRequestError as e:
        logging.error(f"Aborting: could not resolve OrthoDB taxon for {args.sample_id}. {e}")
        sys.exit(1)

    if taxid is None:
        #   log the samples for which there are no ortholog clusters in OrthoDB
        with open("no_orthodb_taxon.csv", "w") as trace:
            trace.write(f"{args.sample_id}\n")
        return

    if args.max_clusters:
        logging.info(f"Limit to first {args.max_clusters} clusters")
        clusters = clusters[:args.max_clusters]

    with open(f"{args.sample_id}_taxon.txt", "w") as f:
        f.write(f"{taxid}\n")

    with open(f"{args.sample_id}_clusters.tsv", "w") as f:
        f.write("cluster_id\tgene_count\n")
        for cluster_id, gene_count in clusters:
            f.write(f"{cluster_id}\t{gene_count}\n")

    logging.info(f"Resolved sample {args.sample_id} to OrthoDB taxid {taxid} with {len(clusters)} clusters")


if __name__ == "__main__":
    main()
