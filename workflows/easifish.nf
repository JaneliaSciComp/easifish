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
include { SPOTS_STATS         } from './spots_features'
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

    def ch_acquisitions = INPUT_CHECK (
        ch_inputs.map { it[0] }, // samplesheet_file
        ch_inputs.map { it[1] }, // inputdir
        imagesdir,
        params.skip_stitching,
    )
    .acquisitions

    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)

    def stitching_results = STITCHING(
        ch_acquisitions,
        outdir,
        session_work_dir
    )

    stitching_results.subscribe { log.debug "Stitching result: $it " }

    def registration_results = REGISTRATION(
        stitching_results,
        outdir,
    )

    registration_results.subscribe { log.debug "Registration result: $it " }

    def segmentation_results = SEGMENTATION(
        stitching_results,
        outdir,
    )

    segmentation_results.subscribe { log.debug "Segmentation result: $it " }

    def spot_extraction_results = SPOT_EXTRACTION(
        stitching_results,
        outdir,
        "${session_work_dir}/spot_extraction",
    )

    spot_extraction_results.subscribe { log.debug "Spot extraction result: $it " }

    def warped_spots_results = WARP_SPOTS(
        registration_results,
        spot_extraction_results,
        outdir,
    ) // final_spot_results includes spots for fixed and warped spots from the moving rounds

    warped_spots_results.subscribe { log.debug "Warped spots results: $it " }

    def spots_stats_results = SPOTS_STATS(
        warped_spots_results,
        segmentation_results,
        outdir,
    )

    spots_stats_results.subscribe { log.debug "Spots stats: $it " }

    def spots_props = EXTRACT_SPOTS_PROPS(
        registration_results,
        segmentation_results,
        outdir,
    )

    spots_props.subscribe { log.debug "Spots props: $it " }

}
