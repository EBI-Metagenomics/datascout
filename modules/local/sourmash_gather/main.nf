#!/usr/bin/env nextflow
process SOURMASH_GATHER {
    container 'quay.io/biocontainers/sourmash:4.8.14--hdfd78af_0'

    publishDir "${params.output}", mode: "copy"
    debug true
    label "process_high"

    tag "${meta}"

    errorStrategy  'retry'
    maxRetries 2

    input:
      tuple val(meta), path(genome_sig), path(list_of_read_sigs)

    output:
      tuple val(meta), path("*.csv"), emit: gather_csv
      path("versions.yml"), emit: versions


    script:
    """
    sourmash gather ${genome_sig} ${list_of_read_sigs.join(' ')} -o ${meta.id}_${meta.ena_tax}_sourmash_gather.csv
    if [[ ! -f "${meta.id}_${meta.ena_tax}_sourmash_gather.csv" ]]; then
        echo "${meta.id}_${meta.ena_tax} had no matching transcriptomic data. No transcriptomic fastq files will be returned";
        touch "${meta.id}_${meta.ena_tax}_empty.csv";
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        Sourmash: \$( sourmash --version 2>&1 | cut -d' ' -f2 )
    END_VERSIONS
    """
}

