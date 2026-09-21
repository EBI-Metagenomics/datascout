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
include { CHECK_ORTHODB_RELEASE      } from '../modules/local/check_orthodb_release/main.nf'
include { RESOLVE_ORTHODB_TAXON      } from '../modules/local/resolve_orthodb_taxon/main.nf'
include { BUILD_ORTHODB_FASTA        } from '../modules/local/build_orthodb_fasta/main.nf'
include { ASSIGN_ORTHODB_FASTA       } from '../modules/local/assign_orthodb_fasta/main.nf'
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

        // the API lists the clusters and the local database holds their sequences, so refuse
        // to start unless both are the same OrthoDB release
        CHECK_ORTHODB_RELEASE(params.orthodb_db)
        ch_versions = ch_versions.mix(CHECK_ORTHODB_RELEASE.out.versions)

        // resolve which OrthoDB taxon each genome maps to, and list that taxon's clusters
        RESOLVE_ORTHODB_TAXON(joined_orthodb, params.max_orthodb_clusters)
        ch_versions = ch_versions.mix(RESOLVE_ORTHODB_TAXON.out.versions.first())

        // trace genomes for which no OrthoDB taxon could be resolved at any lineage rank
        RESOLVE_ORTHODB_TAXON.out.no_taxon
            .collectFile(
                name: 'no_orthodb_taxon.csv',
                seed: 'sample_id\n',
                sort: true,
                storeDir: "${params.outdir}"
            )

        // pull out the resolved taxid (text content of the *_taxon.txt file)
        RESOLVE_ORTHODB_TAXON.out.taxon_clusters
            .map { meta, taxon_file, clusters_file -> tuple(meta, taxon_file.text.trim(), clusters_file) }
            .set { resolved_taxon_clusters }

        // dedupe: build each unique taxon's FASTA once, no matter how many genomes share it
        resolved_taxon_clusters
            .map { _meta, taxid, clusters_file -> tuple(taxid, clusters_file) }
            .unique { taxid, _clusters_file -> taxid }
            .set { unique_taxon_clusters }

        BUILD_ORTHODB_FASTA(unique_taxon_clusters, CHECK_ORTHODB_RELEASE.out.checked.first(), params.orthodb_min_proteins)
        ch_versions = ch_versions.mix(BUILD_ORTHODB_FASTA.out.versions.first())

        // fan the per-taxon FASTA back out to every genome that resolved to it
        resolved_taxon_clusters
            .map { meta, taxid, _clusters_file -> tuple(taxid, meta) }
            .combine(BUILD_ORTHODB_FASTA.out.fasta, by: 0)
            .map { taxid, meta, fasta -> tuple(meta, taxid, fasta) }
            .set { genome_fasta_ch }

        ASSIGN_ORTHODB_FASTA(genome_fasta_ch)

        // trace every genome whose resolved taxon held too few proteins to be published
        resolved_taxon_clusters
            .map { meta, taxid, _clusters_file -> tuple(taxid, meta) }
            .combine(BUILD_ORTHODB_FASTA.out.low_proteins, by: 0)
            .map { _taxid, meta, low_proteins -> "${meta.id},${low_proteins.text.trim()}\n" }
            .collectFile(
                name: 'low_protein_genomes.csv',
                seed: 'sample_id,taxid,n_proteins\n',
                sort: true,
                storeDir: "${params.outdir}"
            )

        UNIPROT_DATA(joined_uniprot, params.swissprot ?: false)
        ch_versions = ch_versions.mix(UNIPROT_DATA.out.versions.first())

        if ( !params.skip_rfam ) {
            RFAM_ACCESSIONS(joined_rfam, params.rfam_db)
            ch_versions = ch_versions.mix(RFAM_ACCESSIONS.out.versions.first())
        }

        // modify meta
        input.genome_file
            .map { meta, gf ->
                def new_meta = [ id: meta.genome_id, ena_tax: meta.ena_tax ]
                [ new_meta, gf ]
            }
            .set { genome_ch }

        // fetch genome fasta file
        GENOME_ASSEMBLY(genome_ch)
        ch_versions = ch_versions.mix(GENOME_ASSEMBLY.out.versions.first())

        // fetch ENA metadata
        ENA_RNA_CSV(taxa_ch, params.order_runs_by_smallest)
        ch_versions = ch_versions.mix(ENA_RNA_CSV.out.versions.first())

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
            if ( !params.download_rna_fastq ) {
                log.info 'Ignoring --download_rna_fastq because --sourmash is true.'
            }

            SOURMASH(
                ena_metadata_grouped,
                GENOME_ASSEMBLY.out.assembly_fa,
                1,
                params.max_runs
            )
            ch_versions = ch_versions.mix(SOURMASH.out.versions.first())
        }
        else if (params.download_rna_fastq) {
            DOWNLOAD_FASTQ_FILES(
                ena_metadata_grouped,
                1,
                params.max_runs
            )
            ch_versions = ch_versions.mix(DOWNLOAD_FASTQ_FILES.out.versions.first())

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
