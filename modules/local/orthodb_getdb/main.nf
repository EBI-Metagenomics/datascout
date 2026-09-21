process ORTHODB_GETDB {
    // Builds the OrthoDB database when the user has none. storeDir keeps it outside the work
    // directory, so the ~39 GB download and the load only ever happen once: later runs find
    // the database already there and skip this process entirely.

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8a/8accb72ace277615baf111306781e4e6e877bbc556a927121cffab1957edf11a/data' :
        'community.wave.seqera.io/library/duckdb_python_requests:73171acde812caf0' }"

    label "process_medium"

    storeDir "${params.orthodb_db_dir}"

    tag "${odb_version}"

    input:
      val(odb_version)
      val(og2genes)
      val(og_aa_fasta)

    output:
      path("orthodb.duckdb"), emit: orthodb_db
      path("versions.yml"), emit: versions

    script:
    def dump_args = og2genes && og_aa_fasta ? "--og2genes ${og2genes} --og_aa_fasta ${og_aa_fasta}" : ""
    """
    orthodb_getdb.py --output orthodb.duckdb --odb_version ${odb_version} \\
        --download_dir . ${dump_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(build_orthodb_fasta.py --orthodb_db orthodb.duckdb --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
