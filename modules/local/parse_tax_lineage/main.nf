process TAX_LINEAGE {

    container 'quay.io/biocontainers/ete3:3.1.2'

    label "process_medium"

    tag "${meta}"

    input:
      tuple val(meta), val(taxid)
      val(taxdump)
      val(db_path)

    output:
      tuple val(meta), path("*_tax_ranks.tsv"), emit: tax_ranks
      path("versions.yml"), emit: versions

    script:
    prefix = meta.id
    """
    cp -r ${db_path} ./
    parse_tax_lineage.py --taxid ${taxid} --output ${prefix}_tax_ranks.tsv --taxdump "${taxdump}" --db_path "${db_path}"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
    \$( parse_tax_lineage.py --taxdump "${taxdump}" --db_path "${db_path}" --version 2>&1 )
      Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}

// Get taxonomic lineage and ranks of query genome from NCBI
