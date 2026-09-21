process ASSIGN_ORTHODB_FASTA {

    label "process_low"

    tag "${meta}"

    input:
      tuple val(meta), val(taxid), path(fasta)

    output:
      tuple val(meta), path("${meta.id}_orthodb_dir"), emit: orthodb_results

    script:
    """
    mkdir -p "${meta.id}_orthodb_dir/${taxid}_sequences"
    cp "${fasta}" "${meta.id}_orthodb_dir/${taxid}_sequences/combined_orthodb_${taxid}.faa"
    """
}
