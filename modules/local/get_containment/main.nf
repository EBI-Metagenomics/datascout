#!/usr/bin/env nextflow
process GET_CONTAINMENT {

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/19/196beb70d2a47fe84cb163297905a7dab5ebaee0e583f0fc39fd283a94bda028/data' :
        'community.wave.seqera.io/library/python_pip_pandas:7de8af9201842540' }"

    label "process_low"

    tag "${meta}"

    errorStrategy  'retry'
    maxRetries 2

    input:
      tuple val(meta), path(sourmash_file)

    output:
      tuple val(meta), path("*runs.txt"), emit: keep_runs


    script:
    """
    parse_sourmash.py --sourmash ${sourmash_file} --output_file runs.txt

    if [[ ! -f runs.txt ]]; then
        echo "${meta.id} had no matching transcriptomic data after filtering."
        touch empty_runs.txt
    fi
    """
}
