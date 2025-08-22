#!/usr/bin/env nextflow
process ENA_RNA_CSV {

    conda "${moduleDir}/biopython_requests.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"

    debug true
    publishDir "${params.output}", mode: "copy"
    label 'process_medium'

    tag "${meta}"

    errorStrategy 'retry'
    maxRetries 2

    input:
      tuple val(meta), path(tax_ranks)

    output:
    tuple val(meta), path("${meta.genome_id}_ENA_filtered_rna.csv"), emit: rna_csv

    script:
    prefix = meta.genome_id
    """
    rna_seq.py --tax_file ${tax_ranks} --output_file "${prefix}_ENA_filtered_rna.csv" --rank ${meta.ena_tax}
    """
}

// Get transcriptome metadata from ena, filtered and reordered by sample name
