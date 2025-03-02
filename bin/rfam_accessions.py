#!/usr/bin/env python3

import pymysql
import logging 
import argparse
import os

logging.basicConfig(level=logging.INFO)

def parse_taxa(taxa_file):
    logging.info("Parsing taxa lineages from file")
    tax_dict = {}
    with open(taxa_file, 'r') as taxa:
        for line in taxa:
            data = line.rstrip().split('\t')
            #   rank: taxa name
            tax_dict[data[2]] = data[0]
    return tax_dict

def query_rfam(tax_ranks, config_file_path, preferred_rank=None):
    """query Rfam per taxonmic rank. Stop when after querying family (order is the stop point). 
    Result is returned. Use rank if provided by user"""
    connection_dict = {}
    with open(config_file_path, 'r') as config:
        connection_details = config.readline()
        for c in connection_details.split(','):
            key = c.split('=')[0]
            value = c.rstrip().split('=')[1]
            connection_dict[key] = value
    try:
        logging.info("Connecting to public Rfam database")
        connection = pymysql.connect(
            host=connection_dict.get("host"),
            user=connection_dict.get("user"),
            database=connection_dict.get("database"),
            port=int(connection_dict.get("port"))
        )

        # Create a cursor object to interact with the database
        cursor = connection.cursor()
        families_sql_query = f"""
        SELECT distinct family_ncbi.rfam_acc 
        FROM family_ncbi, taxonomy 
        WHERE family_ncbi.ncbi_id = taxonomy.ncbi_id 
        AND species LIKE %s
        """

        if preferred_rank:
            tax_name = next((name for name, rank in tax_ranks.items() if rank == preferred_rank), None)
            cursor.execute(families_sql_query, f"%{tax_name}%")
            results = cursor.fetchall()
            if not results:
                logging.info(f"No families found at selected rank {preferred_rank}")
                cursor.close()
                connection.close()
                return None
            rfam_results = set(row[0] for row in cursor)
            logging.info(f"{len(rfam_results)} found at rank {preferred_rank}")
            cursor.close()
            connection.close()
            return rfam_results
        
        families = set()
        data_found = False

        while not data_found:
            for tax_name, rank in tax_ranks.items():
                if rank != "order":       
                    cursor.execute(families_sql_query, f"%{tax_name}%")
                    rfam_results = {row[0] for row in cursor.fetchall()}
                    families.update(rfam_results)
                    logging.info(f"total families_count after searcing rank {rank}: {len(families)}")
                else:
                    if len(families) < 1:
                        logging.info("No families found up to rank family")
                        return None
                    else:
                        return families      
        cursor.close()        
        connection.close()

    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL RFam server: {e}")
        return None
            

def main():
    parser = argparse.ArgumentParser(description="Fetch data from Rfam")
    parser.add_argument(
        "-t", "--tax_file", type=str, help="File with taxonomic lineage information"
    )
    parser.add_argument(
        "-r", "--rank", type=str, help="Preferred rank to search for families", required=False
    )
    parser.add_argument(
        "-o", "--output_dir", type=str, help="output directory"
    )
    parser.add_argument(
        "-c", "--config", type=str, help="path to Rfam connection details", required=True
    )
    args = parser.parse_args()

    if args.rank == "default":
        rank = None
    else:
        rank = args.rank

    os.makedirs(args.output_dir, exist_ok=True)

    taxa_dict = parse_taxa(args.tax_file)
    rfam_results = query_rfam(taxa_dict, args.config, rank)

    if rfam_results:
        logging.info("Writing families to rfam_ids.txt")
        with open(os.path.join(args.output_dir, "rfam_ids.txt"), 'w') as outfile:
            for rf in rfam_results:
                outfile.write(rf + "\n")

if __name__ == "__main__":
    main()
