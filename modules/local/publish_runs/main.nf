#!/usr/bin/env nextflow
process PUBLISH_RUNS {

    label 'process_low'

    tag "${meta}"

    input:
      tuple val(meta), path(fastq_files)

    output:
      tuple val(meta), path("${meta.id}_${meta.ena_tax}_rna_fastq_dir/*"), emit: fastq_files

    script:
    """
    mkdir -p ${meta.id}_${meta.ena_tax}_rna_fastq_dir/
    echo "publishing fastq files for ${meta.id}_${meta.ena_tax}"
    for fq in ${fastq_files.join(' ')}; do mv \$fq ${meta.id}_${meta.ena_tax}_rna_fastq_dir/; done
    """
}

// Publish filtered fastq files only 
