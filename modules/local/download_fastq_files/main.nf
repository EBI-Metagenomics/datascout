#!/usr/bin/env nextflow
process DOWNLOAD_FASTQ_FILES {

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8c/8c0fd3dac7f68110b869985e8f9399217bf3099468b9515b5f1cebd1d0167b41/data' :
        'community.wave.seqera.io/library/pigz_python_pip_boto3_pandas:ffa6f661982e0829' }"

    label 'process_high'

    tag "${meta}"

    input:
      tuple val(meta), path(ena_metadata)
      val(start_line)
      val(num_lines)

    output:
      tuple val(meta), path("*fastq"), emit: fastq_files
      path("versions.yml"), emit: versions

    script:
    """
    download_rnaseq_fastqs.py --startline ${start_line} --transcriptomes ${ena_metadata} --numlines ${num_lines}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
      Python: \$(python --version 2>&1 | sed 's/Python //g')
    """
}
