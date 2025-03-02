#!/usr/bin/env nextflow
process SOURMASH_RNA_CSV {
    container 'oras://community.wave.seqera.io/library/samtools_pymysql_requests:97922c3500673735'
    debug true
    errorStrategy  'retry'
    maxRetries 2

    tag "${meta}"

    input:
    tuple val(genome), val(fastq_files), path(tax_ranks)

    output:
    tuple val(genome),path("smashed_rna/"), emit: smashed_rna

    script:
    """
    sourmash_rna.py ${genome} "${fastq_files}" ${tax_ranks} ${params.rna_fastq_dir}
    """
}
