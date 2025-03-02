#!/usr/bin/env python3

import argparse
import pandas as pd
import logging
import subprocess
import os
import hashlib
from multiprocessing import Pool
from s3fire_public_downloader import transform_ftp_to_s3, download_file_from_fire

logging.basicConfig(level=logging.INFO)
RETRY_COUNT = 3
MAX_PARALLEL_DOWNLOADS = 10

def chunk_ena_csv(csv_file, start, numlines):
    """
    Parse ENA file and select fastq locations and md5
    split by semi-colon which exists for paired end
    return dictionary fastq_path: fastq_checksum
    """
    df = pd.read_csv(csv_file, usecols=["run_accession", "fastq_ftp", "fastq_md5"])
    start_line = max(0, start - 1)
    end_line = min(start_line + numlines, len(df))
    subset_df = df.iloc[start_line:end_line]
    dict_df = dict(zip(subset_df['fastq_ftp'], subset_df['fastq_md5']))
    files_for_download = {}
    for file, md5 in dict_df.items():
        sep_files = file.split(';')
        sep_md5 = md5.split(';')
        files_for_download[sep_files[0]] = sep_md5[0]
        files_for_download[sep_files[1]] = sep_md5[1]
    logging.info(f"{len(files_for_download)} file locations found for download")
    return files_for_download


def calculate_checksum(file_path):
    hash_func = hashlib.md5()
    with open(file_path, 'rb') as file:
        for chunk in iter(lambda: file.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def decompress(file_path):
    """
    Decompress files with pigz
    """
    cmd = [
        'pigz',
        '-d', 
        file_path
        ]
    
    if file_path.endswith('.gz'):
        logging.info(f"Decompressing {file_path}")
        result = subprocess.run(cmd, text=True, capture_output=True)
        if result.returncode != 0:
            logging.info(f"Failed to decompress file. Command:\n{' '.join(cmd)}")
            return False
        return True
    else:
        logging.info(f"File {file_path} is already decompressed skipping...")
        return True

def download_file(download_tuple):
    """
    Input single tuple file,md5
    Download file with fire
    Check for matching checksum
    Unzip files
    Check success of all above steps
    """
    file, md5 = download_tuple 
    attempt = 0
    success = False
    s3_key = transform_ftp_to_s3(file)
    output_path = os.path.basename(file)

    while attempt < RETRY_COUNT and not success:
        attempt += 1
        logging.info(f"Downloading {file} (Attempt {attempt})")

        if not os.path.exists(output_path):
            if download_file_from_fire(s3_key):
                if calculate_checksum(output_path) == md5:
                    success = decompress(output_path)
        else:
            logging.info(f"{file} exists, verifying checksum...")
            if calculate_checksum(output_path) == md5:
                success = decompress(output_path)
            else:
                logging.warning(f"File incomplete. Removing and re-downloading {file}")
                os.remove(output_path)

    if not success:
        logging.error(f"Failed to download {file} after {RETRY_COUNT} attempts")

def download_files_parallel(file_dict):
    """
    Download multiple files in parallel
    """
    file_list = list(file_dict.items())
    with Pool(MAX_PARALLEL_DOWNLOADS) as pool:
        pool.map(download_file, file_list)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Parse RNA CSV file from ENA and extract run identifiers 5 (or specified number) at a time. Download fastq files in parallel.",
    )
    parser.add_argument(
        "-n", "--numlines", type=int, default=5, required=False, help="Number of lines to parse. Default is 5"
    )
    parser.add_argument(
        "-s", "--startline", type=int, required=True, help="Starting line to parse from"
    )
    parser.add_argument(
        "-t", "--transcriptomes", type=str, required=True, help="Path to file of ENA transcriptome reads accessions"
    )

    args = parser.parse_args()

    files_for_download = chunk_ena_csv(args.transcriptomes, args.startline, args.numlines)
    download_files_parallel(files_for_download)