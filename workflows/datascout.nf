/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { paramsSummaryLog } from 'plugin/nf-schema'

log.info paramsSummaryLog(workflow)

/************************** 
* INPUT CHANNELS 
**************************/

include { samplesheetToList } from 'plugin/nf-schema'

samplesheet = Channel.fromList(samplesheetToList(params.samplesheet, "${projectDir}/assets/schema_input.json"))

/* split the inputs */
samplesheet.multiMap {meta, tax_name, taxid, orthodb_tax, uniprot_tax, rfam_tax, uniprot_evidence ->
    meta: meta
    tax_name: [ meta, tax_name ]
    taxid: [ meta, taxid ]
    orthodb_tax: [ meta, orthodb_tax ]
    uniprot_tax: [ meta, uniprot_tax ]
    rfam_tax: [ meta, rfam_tax ]
    uniprot_evidence: [ meta, uniprot_evidence ]
}.set {
    input
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { TAX_LINEAGE                } from "../modules/local/parse_tax_lineage/main.nf"
include { NCBI_ORTHODB               } from "../modules/local/ncbi_orthodb/main.nf"
include { GENOME_ASSEMBLY            } from "../modules/local/genome_assembly/main.nf"
include { UNIPROT_DATA               } from "../modules/local/uniprot_data/main.nf"
include { RFAM_ACCESSIONS            } from "../modules/local/rfam_accessions/main.nf"
include { ENA_RNA_CSV                } from "../modules/local/ena_rna_csv/main.nf"
include { DOWNLOAD_FASTQ_FILES       } from "../modules/local/download_fastq_files/main.nf"
include { PUBLISH_RUNS               } from "../modules/local/publish_runs/main.nf"
include { SOURMASH                   } from "../subworkflows/local/sourmash_filtering.nf"
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/


workflow DATASCOUT{

    // get taxonomy lineage 
    TAX_LINEAGE(input.taxid, params.taxdump)
    taxa_ch = TAX_LINEAGE.out.tax_ranks

    // query databases for supporting proteins and rnas
    NCBI_ORTHODB(taxa_ch, input.orthodb_tax)
    UNIPROT_DATA(taxa_ch, input.uniprot_tax, input.uniprot_evidence)
    RFAM_ACCESSIONS(taxa_ch, input.rfam_tax, params.rfam_db)

    // modify meta
    genome_ch = input.meta.map { meta ->
        def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
        return new_meta
    }

    // fetch genome fasta file
    GENOME_ASSEMBLY(genome_ch)

    // fetch ENA metadata
    ENA_RNA_CSV(taxa_ch)

    // modify meta
    ena_metadata_ch = ENA_RNA_CSV.out.rna_csv.map { meta, path ->
        def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
        return [ new_meta, path ]
    }
    
    // group by genome_id and ena_tax and select first metadata path - they should be identical
    ena_metadata_grouped = ena_metadata_ch
        .groupTuple()
        .map { metadata -> 
            def meta = metadata[0]   
            def paths = metadata[1] 
            return [meta, paths.first()]
    }


    // continue processing with the grouped metadata and CSV file path

    if ( params.sourmash ) {

        SOURMASH(
            ena_metadata_grouped,
            GENOME_ASSEMBLY.out.assembly_fa,
            1,
            params.max_runs
        )
    }

    else {

        DOWNLOAD_FASTQ_FILES(
            ena_metadata_grouped,
	    1,
            params.max_runs
        )

        PUBLISH_RUNS(DOWNLOAD_FASTQ_FILES.out.fastq_files)
    }
    
}

// /*
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//     THE END
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// */
