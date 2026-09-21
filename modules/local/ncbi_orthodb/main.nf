process NCBI_ORTHODB {
    maxForks 3

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8a/8accb72ace277615baf111306781e4e6e877bbc556a927121cffab1957edf11a/data' :
        'community.wave.seqera.io/library/duckdb_python_requests:73171acde812caf0' }"

    label "process_medium"

    tag "${meta}"

    input:
      tuple val(meta), path(tax_ranks), val(max_rank)
      val(max_clusters)
      val(min_proteins)
      val(orthodb_db)

    output:
      tuple val(meta), path("${meta.id}_orthodb_dir"), emit: orthodb_results, optional: true
      path("low_proteins.csv"), emit: low_proteins, optional: true
      path("versions.yml"), emit: versions

    script:
    def max_clusters_arg = max_clusters && max_clusters >0 ? "--max_clusters ${max_clusters}" : ""
    """
    ncbi_orthodb_data.py --tax_file ${tax_ranks} --lineage_max ${max_rank} --output "${meta.id}_orthodb_dir" \\
        --sample_id ${meta.id} --min_proteins ${min_proteins} --orthodb_db ${orthodb_db} ${max_clusters_arg}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(ncbi_orthodb_data.py --orthodb_db ${orthodb_db} --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
