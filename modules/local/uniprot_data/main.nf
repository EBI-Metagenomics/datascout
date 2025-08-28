process UNIPROT_DATA {

    conda "./environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"

    debug true
    publishDir "${params.output}", mode: "copy"
    label "process_low"

    tag "${meta}"

    errorStrategy 'retry'
    maxRetries 2

    input:
      tuple val(meta), path(tax_ranks), val(rank), val(evidence)

    output:
      tuple val(meta), path("${meta.id}_uniprot_dir"), emit: uniprot_results
      path "versions.yml", emit: versions

    script:
    """
    mkdir -p ${meta.id}_uniprot_dir
    uniprot_data.py --tax_file ${tax_ranks} --output "${meta.id}_uniprot_dir" --rank ${rank} --evidence ${evidence}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$( uniprot_data.py --tax_file ${tax_ranks} --output "${meta.id}_uniprot_dir" --version 2>&1 )
    END_VERSIONS
    """
}

// Get proteins from uniprot and reformat headers
