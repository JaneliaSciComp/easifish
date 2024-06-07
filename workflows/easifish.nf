/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Check mandatory parameters
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
log.info "Using absolute path for indir: "+indir

def outdir_d = file(params.outdir)

// Make outdir absolute
outdir = outdir_d.toAbsolutePath().normalize().toString()
log.info "Using absolute path for outdir: "+outdir

// Check input path parameters to see if they exist
def checkPathParamList = [ params.input, indir ]
for (param in checkPathParamList) {
    if (param) { file(param, checkIfExists: true) }
}

include { paramsSummaryLog; paramsSummaryMap } from 'plugin/nf-validation'

def logo = NfcoreTemplate.logo(workflow, params.monochrome_logs)
def citation = '\n' + WorkflowMain.citation(workflow) + '\n'
def summary_params = paramsSummaryMap(workflow)

// Print parameter summary log to screen
log.info logo + paramsSummaryLog(workflow) + citation

WorkflowEASIFISH.initialise(params, log)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { INPUT_CHECK            } from '../subworkflows/local/input_check'
include { SPARK_START            } from '../subworkflows/janelia/spark_start/main'
include { SPARK_STOP             } from '../subworkflows/janelia/spark_stop/main'
include { BIGSTREAM_REGISTRATION } from '../subworkflows/janelia/bigstream_registration/main'

include { STITCHING_PREPARE      } from '../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI     } from '../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5       } from '../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD    } from '../modules/local/stitching/flatfield/main'
include { STITCHING_STITCH       } from '../modules/local/stitching/stitch/main'
include { STITCHING_FUSE         } from '../modules/local/stitching/fuse/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT NF-CORE MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { CUSTOM_DUMPSOFTWAREVERSIONS } from '../modules/nf-core/custom/dumpsoftwareversions/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow EASIFISH {
    def ch_versions = Channel.empty()
    def data_dirs = [indir, outdir]

    def ch_acquisitions = INPUT_CHECK (
        samplesheet_file,
        indir,
        outdir,
    )
    .acquisitions
    .map {
        def (meta, files) = it
        // set output subdirectories for each acquisition
        meta.spark_work_dir = "${params.workdir}/${workflow.sessionId}/${meta.id}"
        meta.stitching_dir = "${outdir}/stitching/${meta.id}"
        meta.stitching_result = params.stitching_result_container
        // Add output dir here so that it will get mounted into the Spark processes
        def data_files = files + [outdir]
        def r = [ meta, data_files ]
        log.debug "Input acquisitions: $files -> $r"
        r
    }

    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)
    // TODO: OPTIONAL, you can use nf-validation plugin to create an input channel from the samplesheet with Channel.fromSamplesheet("input")
    // See the documentation https://nextflow-io.github.io/nf-validation/samplesheets/fromSamplesheet/
    // ! There is currently no tooling to help you write a sample sheet schema

    STITCHING_PREPARE(
        ch_acquisitions
    )

    def stitching_input = SPARK_START(
        STITCHING_PREPARE.out,
        params.workdir,
        [outdir],
        params.spark_cluster,
        params.spark_workers as int,
        params.spark_worker_cores as int,
        params.spark_gb_per_core as int,
        params.spark_driver_cores as int,
        params.spark_driver_memory
    )
    | join(ch_acquisitions, by:0)
    | map {
        def (meta, spark, files) = it
        def r = [
            meta, files, spark,
        ]
        log.debug "Stitching input: $it -> $r"
        r
    }

    STITCHING_PARSECZI(stitching_input)
    ch_versions = ch_versions.mix(STITCHING_PARSECZI.out.versions)

    STITCHING_CZI2N5(STITCHING_PARSECZI.out.acquisitions)
    ch_versions = ch_versions.mix(STITCHING_CZI2N5.out.versions)

    flatfield_done = STITCHING_CZI2N5.out
    if (params.flatfield_correction) {
        flatfield_done = STITCHING_FLATFIELD(
            STITCHING_CZI2N5.out.acquisitions
        )
        ch_versions = ch_versions.mix(flatfield_done.versions)
    }

    STITCHING_STITCH(flatfield_done.acquisitions)
    ch_versions = ch_versions.mix(STITCHING_STITCH.out.versions)

    STITCHING_FUSE(STITCHING_STITCH.out.acquisitions)
    ch_versions = ch_versions.mix(STITCHING_FUSE.out.versions)

    def spark_stop_input = STITCHING_FUSE.out.acquisitions
    | map {
        def (meta, files, spark) = it
        [ meta, spark ]
    }

    done = SPARK_STOP(spark_stop_input, params.spark_cluster)

}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    COMPLETION EMAIL AND SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

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
