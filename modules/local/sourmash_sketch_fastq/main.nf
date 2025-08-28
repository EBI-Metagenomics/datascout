#!/usr/bin/env nextflow
process SOURMASH_SKETCH_FASTQ {
    container 'quay.io/biocontainers/sourmash:4.8.14--hdfd78af_0'

    debug true
    label "process_long"

    tag "${meta}"

    errorStrategy  'retry'
    maxRetries 2

    input:
      tuple val(meta), path(forward_file), path(reverse_file)

    output:
      tuple val(meta), path("*.sig"), emit: sketch
      path("versions.yml"), emit: versions


    script:
    """
    sourmash sketch dna -p k=31,abund ${forward_file} ${reverse_file} --merge ${meta.run_id} -o ${meta.run_id}.sig

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        Sourmash: \$( sourmash --version 2>&1 | cut -d' ' -f2 )
    END_VERSIONS
    """
}

