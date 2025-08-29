process GENOME_ASSEMBLY {
    maxForks 1

    conda "./environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"
        
    debug true
    publishDir "${params.outdir}", mode: "copy"
    label "process_medium"

    tag "${meta}"

    errorStrategy  'retry'
    maxRetries 3

    input:
    tuple val(meta), path(genome_file)

    output:
    tuple val(meta), path ("*_reheaded_assembly.fasta"), emit: assembly_fa

    script:
    """
    if [[ "${genome_file}" == "" ]]; then
        genome_assembly.py --genome_id ${meta.id}
    else
        genome_assembly.py --genome_id ${meta.id} -f ${genome_file}
    fi

    if [[ ! -s "${meta.id}_reheaded_assembly.fasta" ]]; then
        rm -rf "${meta.id}_reheaded_assembly.fasta";
    fi
    """
}

// Download genome fasta file 