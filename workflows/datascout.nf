/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { paramsSummaryLog } from 'plugin/nf-schema'

/************************** 
* INPUT CHANNELS 
**************************/

include { samplesheetToList } from 'plugin/nf-schema'

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


workflow DATASCOUT {

    main:
    
        log.info paramsSummaryLog(workflow)

        Channel
            .fromList(samplesheetToList(params.samplesheet, "${projectDir}/assets/schema_input.json"))
            .multiMap { meta, taxid, orthodb_tax, uniprot_tax, rfam_tax, uniprot_evidence, genome_file ->
                meta: meta
                taxid: [ meta, taxid ]
                orthodb_tax: [ meta, orthodb_tax ]
                uniprot_tax: [ meta, uniprot_tax ]
                rfam_tax: [ meta, rfam_tax ]
                uniprot_evidence: [ meta, uniprot_evidence ]
                genome_file: [ meta, genome_file ]
            }
            .set { input }

        // get taxonomy lineage 
        TAX_LINEAGE(input.taxid, params.taxdump, params.db_path)
        TAX_LINEAGE.out.tax_ranks.set { taxa_ch }

        // prevent meta getting mixed up
        taxa_ch.join(input.orthodb_tax).set { joined_orthodb }
        taxa_ch.join(input.uniprot_tax).join(input.uniprot_evidence).set { joined_uniprot }
        taxa_ch.join(input.rfam_tax).set { joined_rfam }

        // query databases for supporting proteins and rnas
        NCBI_ORTHODB(joined_orthodb)
        UNIPROT_DATA(joined_uniprot)
        RFAM_ACCESSIONS(joined_rfam, params.rfam_db)

        // modify meta
        input.genome_file
            .map { meta, gf ->
                def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
                [ new_meta, gf ]
            }
            .set { genome_ch }

        // fetch genome fasta file
        GENOME_ASSEMBLY(genome_ch)
        GENOME_ASSEMBLY.out.assembly_fa.set { assembly_ch }

        // fetch ENA metadata
        ENA_RNA_CSV(taxa_ch)

        // modify meta
        ENA_RNA_CSV.out.rna_csv
            .map { meta, path ->
                def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
                [ new_meta, path ]
            }
            .set { ena_metadata_ch }
        
        // group by genome_id and ena_tax and select first metadata path - they should be identical
        ena_metadata_ch
            .groupTuple()
            .map { metadata -> 
                def meta = metadata[0]   
                def paths = metadata[1] 
                [meta, paths.first()]
            }
            .set { ena_metadata_grouped }

        // continue processing with the grouped metadata and CSV file path

        if ( params.sourmash ) {

            SOURMASH(
                ena_metadata_grouped,
                assembly_ch,
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
