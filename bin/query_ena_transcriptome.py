#!/usr/bin/env python3

import requests
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout
import logging
from itertools import cycle


logging.basicConfig(level=logging.INFO)

MAX_READ_COUNT = 100000000

def get_default_connection_headers():
    return {
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "*/*",
        }
    }

def check_paired_end(library_layout, fastq_ftp, fastq_md5, fastq_aspera):
    fastq_files = fastq_ftp.split(';')
    if library_layout == 'PAIRED':
        if len(fastq_files) == 2:
            return True, None, None, None
        #   account for more than expected files and only keep _1 and _2 formats
        elif len(fastq_files) > 2:
            files = fastq_ftp.split(';')
            checksums = fastq_md5.split(';')
            aspera = fastq_aspera.split(';')
            new_files, new_checksums, new_aspera = [], [], []
            for file, checksum, aspera in zip(files, checksums, aspera):
                # Check if the file name ends with '_1.fastq.gz' or '_2.fastq.gz'
                if '_1.fastq.gz' or '_2.fastq.gz' in file:
                    new_files.append(file)
                    new_checksums.append(checksum)
                    new_aspera.append(aspera)
            if new_files == 2:
                return True, ';'.join(new_files), ';'.join(new_checksums), ';'.join(new_aspera)
        return False, None, None, None

def calculate_read_length(base_count, read_count):
    if base_count == '0' or read_count == '0':
        return 0
    return (int(base_count) / int(read_count)) / 2 #divided by 2 assuming we only have PAIRED library layout

# Sort output_keys by their maximum base_count in descending order

class EnaMetadata:
    def __init__(self, taxid, order_by_smallest=False):
        self.ena_base_url = "https://www.ebi.ac.uk/ena/portal/api/search"
        self.fields = """sample_accession,secondary_sample_accession,run_accession,study_accession,sample_title,
        experiment_title,instrument_platform,library_layout,read_count,base_count,fastq_aspera,fastq_ftp,fastq_md5"""
        self.retry_count = 2
        self.taxid = taxid
        self.queries= {
            "instrument_platform": "ILLUMINA",
            "library_source": "TRANSCRIPTOMIC",
            "library_layout": "PAIRED"
        }
        self.order_by_smallest = order_by_smallest

        join_queries = " AND ".join([f"{key}={value}" for key, value in self.queries.items()])
        taxid_query = f"tax_tree({self.taxid})"
        query_string = taxid_query  + " AND " +  join_queries

        self.params= {
            "result": "read_run",
            "query": query_string,
            "format": "json",
            "fields": self.fields
        }
        
    def post_request(self):
        response = requests.post(self.ena_base_url, data=self.params, **get_default_connection_headers())
        return response
    
    def retry_or_handle_request_error(self, request, *args, **kwargs):
        attempt = 0
        while attempt < self.retry_count:
            try:
                response = request(*args, **kwargs)
                response.raise_for_status()
                return response
            #   all other RequestExceptions are raised below
            except (Timeout, ConnectionError) as retry_err:
                attempt += 1
                if attempt >= self.retry_count:
                    raise ValueError(f"Could not query ENA after {self.retry_count} attempts. Error: {retry_err}")
            except HTTPError as http_err:
                print(f"HTTP response has an error status: {http_err}")
                raise
            except RequestException as req_err:
                print(f"Network-related error status: {req_err}")
                raise
            #   all other issues
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                raise
    

    def query_ena(self):
        """Fetch run metadata with taxid from ENA
        check data is paired and with two files
        filter short sequences
        sort by base count descending
        group by sample name and cycle through alternating the names"""
        response = self.retry_or_handle_request_error(self.post_request)
        try:
            data_txt = response.text
            if data_txt is None:
                logging.warning(f"No data exists for taxid {self.taxid}")
            ena_data = response.json()
        except (IndexError, TypeError, ValueError, KeyError):
            logging.error("Failed to parse API output")
        
        logging.info(f"{len(ena_data)} transcriptomes found at taxid {self.taxid}")
        if not len(ena_data):
            return []
            
        qc_set = []
        for entry in ena_data:
            paired, new_files, new_checksums, new_aspera = check_paired_end(entry['library_layout'], entry['fastq_ftp'], entry['fastq_md5'], entry['fastq_aspera'])
            read_len = calculate_read_length(entry['base_count'], entry['read_count'])
            if paired and read_len >= 75 and int(entry['read_count']) < MAX_READ_COUNT:
                if new_files:
                    entry['fastq_ftp'] = new_files
                    entry['fastq_md5'] = new_checksums
                    entry['fastq_aspera'] = new_aspera
                qc_set.append(entry)

        logging.info(f"{len(qc_set)} transcriptomes remained at taxid {self.taxid} after QC filters applied")
        if not len(qc_set):
            return []
        if self.order_by_smallest:
            sorted_data = sorted(qc_set, key=lambda x: int(x['base_count']))
        else:
            sorted_data = sorted(qc_set, key=lambda x: int(x['base_count']), reverse=True)
        
        grouped = {}
        reordered = []
        for entry in sorted_data:
            sample = entry['sample_title']
            if sample not in grouped:
                grouped[sample] = []
            grouped[sample].append(entry)

        group_cycle = cycle(grouped.values())
        groups_empty = False
        while not groups_empty:
            try:
                group = next(group_cycle)
                reordered.append(group.pop(0))
                 #  Stop when all groups are empty
                if all(len(g) == 0 for g in grouped.values()):
                    groups_empty = True
            #   catch groups that are shorter in length    
            except IndexError:
                continue
        
        return reordered
    
