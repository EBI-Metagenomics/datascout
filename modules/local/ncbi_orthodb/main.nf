process NCBI_ORTHODB {
    maxForks 3

    conda "${moduleDir}/biopython_requests.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"
        
    debug true
    publishDir "${params.output}", mode: "copy"
    label "process_medium"

    tag "${meta}"

    errorStrategy 'retry'
    maxRetries 2

    input:
      tuple val(meta), file(tax_ranks), val(max_rank)

    output:
      tuple val(meta), path("${meta.id}_orthodb_dir"), emit: orthodb_results

    script:
    """
    mkdir -p ${meta.id}_orthodb_dir
    ncbi_orthodb_data.py --tax_file ${tax_ranks} --lineage_max ${max_rank} --output "${meta.id}_orthodb_dir"
    """
}


// Get proteins from orthodb and reformat into combined fasta file per taxid