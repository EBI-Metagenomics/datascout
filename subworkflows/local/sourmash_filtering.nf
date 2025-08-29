#!/usr/bin/env nextflow

include { SOURMASH_SKETCH as SOURMASH_SKETCH_FASTQ  } from '../../modules/nf-core/sourmash/sketch/main'
include { SOURMASH_SKETCH as SOURMASH_SKETCH_GENOME } from '../../modules/nf-core/sourmash/sketch/main'
include { DOWNLOAD_FASTQ_FILES                      } from "../../modules/local/download_fastq_files/main.nf"
include { SOURMASH_GATHER                           } from "../../modules/local/sourmash_gather/main.nf"
include { GET_CONTAINMENT                           } from "../../modules/local/get_containment/main.nf"
include { PUBLISH_RUNS                              } from "../../modules/local/publish_runs/main.nf"


workflow SOURMASH {

    take:
    ena_metadata // tuple: meta, metdata_csv
    genome // tuple: meta, genome_path
    start_line // int
    num_lines // int

    main:

    DOWNLOAD_FASTQ_FILES(ena_metadata, start_line, num_lines)

    DOWNLOAD_FASTQ_FILES.out.fastq_files
        .flatMap { meta, fastqs -> 
            def filePairs = fastqs.collect { file(it) }
                .groupBy { file -> file.name.split('_')[0] }
                .collect { run_id, files ->
                    def forward = files.find { it.name.endsWith('_1.fastq') }
                    def reverse = files.find { it.name.endsWith('_2.fastq') }
                    def new_meta = meta + [run_id: run_id]
                    tuple(new_meta, [forward, reverse])
                }
            filePairs
        }
        .set { paired_fastq_files }


    SOURMASH_SKETCH_FASTQ(paired_fastq_files)
    ch_versions = SOURMASH_SKETCH_FASTQ.out.versions

    SOURMASH_SKETCH_GENOME(genome)

    SOURMASH_SKETCH_FASTQ.out.signatures
        .map { meta, sketch -> 
            def new_meta = [id: meta.id, ena_tax: meta.ena_tax] // remove run ID
            tuple(new_meta, sketch)
        }
        .groupTuple()
        .set { signatures_ch }

    SOURMASH_SKETCH_GENOME.out.signatures
        .join(signatures_ch)
        .map { meta, genome_sig, list_of_read_sigs -> 
            tuple(meta, genome_sig, list_of_read_sigs)
        }
        .set { gather_input_ch }

    SOURMASH_GATHER(gather_input_ch)

    SOURMASH_GATHER.out.gather_csv
        .filter { _meta, file -> !file.name.contains('_empty.csv') }
        .set { gather_output_filtered }

    GET_CONTAINMENT(gather_output_filtered)

    GET_CONTAINMENT.out.keep_runs
        .filter { _meta, file -> !file.name.contains('empty') }
        .map { run_data -> 
            def meta = run_data[0]
            def runs = run_data[1]  
            def run_ids = runs.readLines().collect { it.trim() }.toList()
            [meta, run_ids] 
        }
        .set { keep_runs_list_ch }

    DOWNLOAD_FASTQ_FILES.out.fastq_files
        .join(keep_runs_list_ch)
        .map { meta, fastq_files, runs ->  
            def filtered_files = fastq_files.findAll { fq_path -> 
                def base_name = fq_path.getFileName().toString().split('_')[0]
                runs.any { id -> base_name == id } 
            }
            tuple(meta, filtered_files)
        }
        .set { filtered_fastq_ch }

    PUBLISH_RUNS(filtered_fastq_ch)

    emit:
    sourmash    = SOURMASH_GATHER.out.gather_csv
    fastq_files = PUBLISH_RUNS.out.fastq_files
    versions    = ch_versions
}