/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { INPUT_CHECK         } from '../subworkflows/local/input_check'

include { STITCHING           } from './stitching'
include { REGISTRATION        } from './registration'
include { SEGMENTATION        } from './segmentation'
include { SPOT_EXTRACTION     } from './spot_extraction'
include { WARP_SPOTS          } from './warp_spots'
include { SPOT_COUNT_ASSIGN   } from './spot_assignment'
include { EXTRACT_SPOTS_PROPS } from './spots_features'


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow EASIFISH {

    take:
    ch_inputs // ch: [samplesheet_file, indir]
    ch_versions

    main:
    def session_work_dir = "${params.workdir}/${workflow.sessionId}"

    def outdir = file(params.outdir).toAbsolutePath().normalize() as String
    def imagesdir = params.stitching_dir ? file(params.stitching_dir) : "${outdir}/stitching"

    def checked_inputs = INPUT_CHECK (
        ch_inputs.map { it -> it[0] }, // samplesheet_file
        ch_inputs.map { it -> it[1] }, // inputdir
        imagesdir,
        params.skip_stitching && !params.download_all_first,
    )

    def ch_acquisitions_raw = checked_inputs.acquisitions

    ch_versions = ch_versions.mix(checked_inputs.versions)

    // If download_all_first is set, wait for all INPUT_CHECK processes to complete
    // before starting STITCHING by collecting all acquisitions then re-emitting them
    def ch_acquisitions = params.download_all_first
        ? ch_acquisitions_raw
            .collect(flat: false)
            .flatMap { aq ->
                log.info "Downloaded acquisition: $aq"
                aq
            }
        : ch_acquisitions_raw
            .map { aq ->
                log.info "Available acquisition: $aq"
                aq
            }

    def stitching_results = STITCHING(
        ch_acquisitions,
        outdir,
        session_work_dir
    )

    stitching_results.view { it -> log.debug "Stitching result: $it " }

    def registration_results = REGISTRATION(
        stitching_results,
        outdir,
    )

    registration_results.view { it -> log.debug "Registration result: $it " }

    def segmentation_results = SEGMENTATION(
        stitching_results,
        outdir,
    )

    segmentation_results.view { it -> log.debug "Segmentation result: $it " }

    def spot_extraction_results = SPOT_EXTRACTION(
        stitching_results,
        outdir,
        "${session_work_dir}/spot_extraction",
    )

    spot_extraction_results.view { it -> log.debug "Spot extraction result: $it " }

    def warped_spots_results = WARP_SPOTS(
        registration_results,
        spot_extraction_results,
        outdir,
    ) // final_spot_results includes spots for fixed and warped spots from the moving rounds

    warped_spots_results.view { it -> log.debug "Warped spots results: $it " }

    def spots_stats_results = SPOT_COUNT_ASSIGN(
        warped_spots_results,
        segmentation_results,
        outdir,
    )

    spots_stats_results.view { it -> log.debug "Spots stats: $it " }

    def spots_props = EXTRACT_SPOTS_PROPS(
        registration_results,
        segmentation_results,
        outdir,
    )

    spots_props.view { it -> log.debug "Spots props: $it " }
}
