#!/usr/bin/env nextflow
process SOURMASH_SKETCH {
    container 'biocontainers/sourmash:4.8.4--hdfd78af_0'
    debug true

    errorStrategy  'retry'
    maxRetries 2

    input:
      tuple val(meta), path(fastq_file_forward), path(fastq_file_reverse)

    output:
      tuple val(meta), path("${meta.run_id}.sig"), emit: fastq_sketch
      path("versions.yml"), emit: versions

    script:
    """
    sourmash sketch dna -p k=31,abund ${fastq_file_forward} ${fastq_file_reverse} --merge ${meta.run_id} -o ${meta.run_id}.sig

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        Sourmash: \$( sourmash --version 2>&1 | cut -d' ' -f2 )
    END_VERSIONS
    """
}
