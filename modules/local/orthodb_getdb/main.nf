process ORTHODB_GETDB {
    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8a/8accb72ace277615baf111306781e4e6e877bbc556a927121cffab1957edf11a/data' :
        'community.wave.seqera.io/library/duckdb_python_requests:73171acde812caf0' }"

    label "process_medium"

    tag "${release}"

    input:
      val(release)
      path(orthodb_db)
      path(og2genes)
      path(og_aa_fasta)

    output:
      path("*/orthodb.duckdb"), emit: new_db, optional: true
      path("versions.yml"), emit: versions

    script:
    def dump_args = og2genes && og_aa_fasta ? "--og2genes ${og2genes} --og_aa_fasta ${og_aa_fasta}" : ""
    def input_arg = orthodb_db ? "--input ${orthodb_db}" : ""
    """
    orthodb_getdb.py \\
        --release ${release} \\
        --output orthodb.duckdb \\
        ${input_arg} \\
        ${dump_args} \\
        --threads ${task.cpus} \\
        --memory ${(task.memory.toGiga() * 0.8) as int}GB

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(build_orthodb_fasta.py --orthodb_db "\${approved_db}" --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
