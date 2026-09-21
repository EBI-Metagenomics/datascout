process BUILD_ORTHODB_FASTA {
    // MOCK implementation: bin/build_orthodb_fasta.py is a placeholder pending
    // the real per-cluster download/combine logic (to be implemented separately).
    // Runs once per unique resolved OrthoDB taxon (see workflows/datascout.nf),
    // not once per genome, so this is where the real implementation's cost lands.

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://community-cr-prod.seqera.io/docker/registry/v2/blobs/sha256/d2/d2cc550ff67f8541d44dc2db1b5d2d2e1cfccfe8536222b49788deefde7460f0/data' :
        'community.wave.seqera.io/library/python_pip_biopython_requests:725bda83fb97ec48' }"

    label "process_medium"

    tag "${taxid}"

    input:
      tuple val(taxid), path(clusters_file)

    output:
      tuple val(taxid), path("combined_orthodb_${taxid}.faa"), emit: fasta
      path("versions.yml"), emit: versions

    script:
    """
    build_orthodb_fasta.py --taxid ${taxid} --clusters_file ${clusters_file} \\
        --output combined_orthodb_${taxid}.faa

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(build_orthodb_fasta.py --version 2>&1)
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
