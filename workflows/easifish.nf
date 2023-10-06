/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    PRINT PARAMS SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Validate input parameters
if (params.spark_workers > 1 && !params.spark_cluster) {
    exit 1, "You must enable --spark_cluster if --spark_workers is greater than 1."
}

// Default indir if it was not specified
def indir = params.indir
if (!indir) {
    indir = params.outdir + "/input"
    log.info "Setting default indir to: "+indir
}

// Create input data directory if we need to
def indir_d = new File(indir)
if (!indir_d.exists()) {
    indir_d.mkdirs()
}

// Make indir absolute
indir = indir_d.toPath().toAbsolutePath().normalize().toString()
log.info "Using absolute path for indir: "+indir

def outdir_d = new File(params.outdir)
if (!outdir_d.exists()) {
    exit 1, "The path specified by --outdir does not exist: "+params.outdir
}

// Make outdir absolute
outdir = outdir_d.toPath().toAbsolutePath().normalize().toString()
log.info "Using absolute path for outdir: "+outdir

// Check input path parameters to see if they exist
def checkPathParamList = [ params.input, indir, outdir ]
for (param in checkPathParamList) { if (param) { file(param, checkIfExists: true) } }


// Check mandatory parameters
if (params.input) { samplesheet_file = file(params.input) } else { exit 1, 'Input samplesheet not specified!' }

include { paramsSummaryLog; paramsSummaryMap } from 'plugin/nf-validation'

def logo = NfcoreTemplate.logo(workflow, params.monochrome_logs)
def citation = '\n' + WorkflowMain.citation(workflow) + '\n'
def summary_params = paramsSummaryMap(workflow)

// Print parameter summary log to screen
log.info logo + paramsSummaryLog(workflow) + citation

WorkflowEASIFISH.initialise(params, log)


/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    CONFIG FILES
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

ch_multiqc_config          = Channel.fromPath("$projectDir/assets/multiqc_config.yml", checkIfExists: true)
ch_multiqc_custom_config   = params.multiqc_config ? Channel.fromPath( params.multiqc_config, checkIfExists: true ) : Channel.empty()
ch_multiqc_logo            = params.multiqc_logo   ? Channel.fromPath( params.multiqc_logo, checkIfExists: true ) : Channel.empty()
ch_multiqc_custom_methods_description = params.multiqc_methods_description ? file(params.multiqc_methods_description, checkIfExists: true) : file("$projectDir/assets/methods_description_template.yml", checkIfExists: true)

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { INPUT_CHECK         } from '../subworkflows/local/input_check'
include { SPARK_START         } from '../subworkflows/janelia/spark_start/main'
include { SPARK_STOP          } from '../subworkflows/janelia/spark_stop/main'
include { STITCHING_PREPARE   } from '../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI  } from '../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5    } from '../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD } from '../modules/local/stitching/flatfield/main'
include { STITCHING_STITCH    } from '../modules/local/stitching/stitch/main'
include { STITCHING_FUSE      } from '../modules/local/stitching/fuse/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT NF-CORE MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { MULTIQC                     } from '../modules/nf-core/multiqc/main'
include { CUSTOM_DUMPSOFTWAREVERSIONS } from '../modules/nf-core/custom/dumpsoftwareversions/main'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow EASIFISH {
    ch_versions = Channel.empty()

    INPUT_CHECK (
        indir,
        samplesheet_file
    )
    .acquisitions
    .map {
        def (meta, files) = it
        // set output subdirectories for each acquisition
        meta.spark_work_dir = "${outdir}/spark/${workflow.sessionId}/${meta.id}"
        meta.stitching_dir = "${outdir}/stitching/${meta.id}"
        // Add top level dirs here so that they get mounted into the Spark processes
        dirs = [indir, outdir]
        [meta, dirs+files]
    }
    .set { ch_acquisitions }
    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)
    // TODO: OPTIONAL, you can use nf-validation plugin to create an input channel from the samplesheet with Channel.fromSamplesheet("input")
    // See the documentation https://nextflow-io.github.io/nf-validation/samplesheets/fromSamplesheet/
    // ! There is currently no tooling to help you write a sample sheet schema

    STITCHING_PREPARE(
        ch_acquisitions
    )

    SPARK_START(
        STITCHING_PREPARE.out,
        [indir, outdir],
        params.spark_cluster,
        params.spark_workers as int,
        params.spark_worker_cores as int,
        params.spark_gb_per_core as int,
        params.spark_driver_cores as int,
        params.spark_driver_memory
    )

    STITCHING_PARSECZI(SPARK_START.out)
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

    done = SPARK_STOP(STITCHING_FUSE.out.acquisitions)

    //
    // MODULE: Pipeline reporting
    //
    CUSTOM_DUMPSOFTWAREVERSIONS (
        ch_versions.unique().collectFile(name: 'collated_versions.yml')
    )

    //
    // MODULE: MultiQC
    //
    workflow_summary    = WorkflowEASIFISH.paramsSummaryMultiqc(workflow, summary_params)
    ch_workflow_summary = Channel.value(workflow_summary)

    methods_description    = WorkflowEASIFISH.methodsDescriptionText(workflow, ch_multiqc_custom_methods_description, params)
    ch_methods_description = Channel.value(methods_description)

    ch_multiqc_files = Channel.empty()
    ch_multiqc_files = ch_multiqc_files.mix(ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml'))
    ch_multiqc_files = ch_multiqc_files.mix(ch_methods_description.collectFile(name: 'methods_description_mqc.yaml'))
    ch_multiqc_files = ch_multiqc_files.mix(CUSTOM_DUMPSOFTWAREVERSIONS.out.mqc_yml.collect())

    MULTIQC (
        ch_multiqc_files.collect(),
        ch_multiqc_config.toList(),
        ch_multiqc_custom_config.toList(),
        ch_multiqc_logo.toList()
    )
    multiqc_report = MULTIQC.out.report.toList()
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
