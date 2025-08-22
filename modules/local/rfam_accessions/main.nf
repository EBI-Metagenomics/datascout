process RFAM_ACCESSIONS {

    conda "./rfam_accessions.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/4f/4fe75ceed4e54cac511cb591843ec3fe0016b9d2f996fcbc639b2289f886e5de/data' :
        'community.wave.seqera.io/library/python_pip_pymysql:0b6be43d90920e61' }"
    debug true
    publishDir "${params.output}", mode: "copy"
    label "process_low"

    tag "${meta}"

    errorStrategy 'retry'
    maxRetries 2

    input:
      tuple val(meta), path(tax_ranks), val(rank)
      val(rfam_db)

    output:
      tuple val(meta), path("${meta.id}_rfam_dir"), emit: rfam_results

    script:
    """
    mkdir -p ${meta.id}_rfam_dir
    rfam_accessions.py --tax_file ${tax_ranks} --output_dir "${meta.id}_rfam_dir" --rank ${rank} --config ${rfam_db}
    """
}

// Get families from RFAM and write list to file
