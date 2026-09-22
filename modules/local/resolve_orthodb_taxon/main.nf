process RESOLVE_ORTHODB_TAXON {
    maxForks 2

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"

    label "process_low"

    tag "${meta}"

    input:
    tuple val(meta), path(tax_ranks), val(max_rank)
    val(max_clusters)
    val(release)

    output:
    tuple val(meta), path("*_taxon.txt"), path("*_clusters.tsv"), emit: taxon_clusters, optional: true
    path("no_orthodb_taxon.csv"), emit: no_taxon, optional: true
    path("versions.yml"), emit: versions

    script:
    def max_clusters_arg = max_clusters && max_clusters > 0 ? "--max_clusters ${max_clusters}" : ""
    """
    resolve_orthodb_taxon.py --tax_file ${tax_ranks} --lineage_max ${max_rank} \\
        --sample_id ${meta.id} --release ${release} ${max_clusters_arg}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(resolve_orthodb_taxon.py --release ${release} --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
