process BUILD_ORTHODB_FASTA {
    // Runs once per unique resolved OrthoDB taxon (see workflows/datascout.nf),
    // not once per genome. Sequences come from the local OrthoDB database built
    // by accessory/orthodb.py, never from the OrthoDB API.

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/8a/8accb72ace277615baf111306781e4e6e877bbc556a927121cffab1957edf11a/data' :
        'community.wave.seqera.io/library/duckdb_python_requests:73171acde812caf0' }"

    label "process_medium"

    tag "${taxid}"

    input:
      tuple val(taxid), path(clusters_file)
      val(orthodb_db)
      val(min_proteins)

    output:
      tuple val(taxid), path("combined_orthodb_${taxid}.faa"), emit: fasta, optional: true
      tuple val(taxid), path("low_proteins.csv"), emit: low_proteins, optional: true
      path("versions.yml"), emit: versions

    script:
    """
    build_orthodb_fasta.py --taxid ${taxid} --clusters_file ${clusters_file} \\
        --output combined_orthodb_${taxid}.faa --orthodb_db ${orthodb_db} \\
        --min_proteins ${min_proteins} --threads ${task.cpus}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(build_orthodb_fasta.py --orthodb_db ${orthodb_db} --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
