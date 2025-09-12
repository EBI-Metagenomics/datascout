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
include { TAX_LINEAGE                } from '../modules/local/parse_tax_lineage/main.nf'
include { NCBI_ORTHODB               } from '../modules/local/ncbi_orthodb/main.nf'
include { GENOME_ASSEMBLY            } from '../modules/local/genome_assembly/main.nf'
include { UNIPROT_DATA               } from '../modules/local/uniprot_data/main.nf'
include { RFAM_ACCESSIONS            } from '../modules/local/rfam_accessions/main.nf'
include { ENA_RNA_CSV                } from '../modules/local/ena_rna_csv/main.nf'
include { DOWNLOAD_FASTQ_FILES       } from '../modules/local/download_fastq_files/main.nf'
include { PUBLISH_RUNS               } from '../modules/local/publish_runs/main.nf'
include { SOURMASH                   } from '../subworkflows/local/sourmash_filtering.nf'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow DATASCOUT {

    main:
    
        log.info paramsSummaryLog(workflow)

        // Initialize versions channel
        ch_versions = Channel.empty()

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
        TAX_LINEAGE(input.taxid, params.taxdump, params.sqlite)
        taxa_ch = TAX_LINEAGE.out.tax_ranks
        ch_versions = ch_versions.mix(TAX_LINEAGE.out.versions.first())

        // prevent meta getting mixed up
        taxa_ch.join(input.orthodb_tax).set { joined_orthodb }
        taxa_ch.join(input.uniprot_tax).join(input.uniprot_evidence).set { joined_uniprot }
        taxa_ch.join(input.rfam_tax).set { joined_rfam }

        // query databases for supporting proteins and rnas
        NCBI_ORTHODB(joined_orthodb, params.max_orthodb_clusters)
        ch_versions = ch_versions.mix(NCBI_ORTHODB.out.versions.first())

        UNIPROT_DATA(joined_uniprot, params.swissprot ?: false)
        ch_versions = ch_versions.mix(UNIPROT_DATA.out.versions.first())

        RFAM_ACCESSIONS(joined_rfam, params.rfam_db)
        ch_versions = ch_versions.mix(RFAM_ACCESSIONS.out.versions.first())

        // modify meta
        input.genome_file
            .map { meta, gf ->
                def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
                [ new_meta, gf ]
            }
            .set { genome_ch }

        // fetch genome fasta file
        GENOME_ASSEMBLY(genome_ch)

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
                GENOME_ASSEMBLY.out.assembly_fa,
                1,
                params.max_runs
            )
            ch_versions = ch_versions.mix(SOURMASH.out.versions.first())
        }

        else {

            DOWNLOAD_FASTQ_FILES(
                ena_metadata_grouped,
                1,
                params.max_runs
            )

            PUBLISH_RUNS(DOWNLOAD_FASTQ_FILES.out.fastq_files)
        }

        // Collect and concatenate all versions
        ch_versions
            .unique()
            .collectFile(
                name: 'software_versions.yml',
                sort: true,
                storeDir: "${params.outdir}/pipeline_info"
            )
}

// /*
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//     THE END
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
// */