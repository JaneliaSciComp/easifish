/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/



include {
    paramsSummaryLog;
    paramsSummaryMap;
} from 'plugin/nf-validation'


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


def validate_params() {

    // Check mandatory parameters
    def samplesheet_file
    if (params.input) {
        samplesheet_file = file(params.input)
    } else {
        exit 1, 'Input samplesheet not specified!'
    }

    // Validate input parameters
    if (params.spark_workers > 1 && !params.spark_cluster) {
        exit 1, 'You must enable --spark_cluster if --spark_workers is greater than 1.'
    }

    // Default indir if it was not specified
    def indir_d
    if (!params.indir) {
        indir_d = file(params.input).parent
        log.info "Setting default indir to: "+indir_d
    } else {
        indir_d = file(params.indir)
    }

    // Make indir absolute
    def indir = indir_d.toAbsolutePath().normalize().toString()
    log.info "Using absolute path for indir: ${indir}"

    def outdir_d = file(params.outdir)

    // Make outdir absolute
    def outdir = outdir_d.toAbsolutePath().normalize().toString()
    log.info "Using absolute path for outdir: "+outdir

    // Check input path parameters to see if they exist
    def checked_paths = [ params.input, indir ]

    for (param in checked_paths) {
        if (param) {
            file(param, checkIfExists: true)
        }
    }

    if (!params.skip_registration && !params.reg_ch ||
        !params.skip_global_align && !params.fix_global_channel ||
        !params.skip_local_align && !params.fix_local_channel) {
        exit 1, 'The registration channel is required - one of reg_ch, fix_global_channel or fix_local_channel must be defined'
    }

    def logo = NfcoreTemplate.logo(workflow, params.monochrome_logs)
    def citation = '\n' + WorkflowMain.citation(workflow) + '\n'
    paramsSummaryMap(workflow)

    // Print parameter summary log to screen
    log.info logo + paramsSummaryLog(workflow) + citation

    WorkflowEASIFISH.initialise(params, log)

    [ indir, samplesheet_file, outdir ]
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow EASIFISH {

    def (indir, samplesheet_file, outdir) = validate_params()


    def ch_versions = Channel.empty()

    def session_work_dir = "${params.workdir}/${workflow.sessionId}"
    def stitching_dir = params.stitching_dir ? file(params.stitching_dir) : "${outdir}/stitching"

    def ch_acquisitions = INPUT_CHECK (
        samplesheet_file,
        indir,
        stitching_dir,
        params.skip_stitching,
    )
    .acquisitions

    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)
    // TODO: OPTIONAL, you can use nf-validation plugin to create an input channel from the samplesheet with Channel.fromSamplesheet("input")
    // See the documentation https://nextflow-io.github.io/nf-validation/samplesheets/fromSamplesheet/
    // ! There is currently no tooling to help you write a sample sheet schema

    def stitching_results = STITCHING(
        ch_acquisitions,
        outdir,
        session_work_dir
    )

    stitching_results.subscribe { log.debug "Stitching result: $it " }

    def registration_results = REGISTRATION(
        stitching_results,
        outdir
    )

    registration_results.subscribe { log.debug "Registration result: $it " }

    def segmentation_results = SEGMENTATION(
        stitching_results,
        outdir
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


workflow.onComplete {
    if (params.email || params.email_on_fail) {
        NfcoreTemplate.email(workflow, params, summary_params, projectDir, log)
    }
    NfcoreTemplate.dump_parameters(workflow, params)
    NfcoreTemplate.summary(workflow, params, log)
    if (params.hook_url) {
        NfcoreTemplate.IM_notification(workflow, params, summary_params, projectDir, log)
    }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
