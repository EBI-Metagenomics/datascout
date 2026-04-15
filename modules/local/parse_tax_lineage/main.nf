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
    def copy_db = db_path != "" ? "cp -r ${db_path} local_taxa.sqlite" : ""
    def taxdump_arg = taxdump != "" ? "--taxdump \"${taxdump}\"" : ""
    def db_path_arg = db_path != "" ? "--db_path local_taxa.sqlite" : ""
    """
    ${copy_db}
    parse_tax_lineage.py --taxid ${taxid} --output ${prefix}_tax_ranks.tsv ${taxdump_arg} ${db_path_arg}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        \$(parse_tax_lineage.py ${taxdump_arg} ${db_path_arg} --version 2>&1 | grep "ete3:")
        \$(parse_tax_lineage.py ${taxdump_arg} ${db_path_arg} --version 2>&1 | grep "NCBI taxdump downloaded on:")
        Python: \$(python --version 2>&1 | sed 's/Python //g')
    END_VERSIONS
    """
}
// Get taxonomic lineage and ranks of query genome from NCBI
