process ORTHODB_RELEASE {
    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8a/8accb72ace277615baf111306781e4e6e877bbc556a927121cffab1957edf11a/data' :
        'community.wave.seqera.io/library/duckdb_python_requests:73171acde812caf0' }"

    label "process_single"

    tag "${odb_version}"

    input:
      val(odb_version)

    output:
      path("release.txt"), emit: release
      path("versions.yml"), emit: versions

    script:
    """
    build_orthodb_fasta.py --odb_version ${odb_version} --resolve_release

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        OrthoDB: \$(cat release.txt)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
