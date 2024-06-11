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

    def stitching_outdir = "${outdir}/stitching"
    def ch_acquisitions = INPUT_CHECK (
        samplesheet_file,
        indir,
        stitching_outdir,
    )
    .acquisitions
    .map {
        def (meta, files) = it
        // set output subdirectories for each acquisition
        meta.session_work_dir = "${params.workdir}/${workflow.sessionId}"
        meta.stitching_dir = stitching_outdir
        meta.stitching_result_dir = outdir
        meta.stitching_dataset = meta.id
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
        [outdir, stitching_outdir],
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

    def fuse_result = STITCHING_FUSE.out.acquisitions
    | map {
        def (meta, files, spark) = it
        [ meta, spark ]
    }

    def completed_stitching_result = SPARK_STOP(fuse_result, params.spark_cluster)

    completed_stitching_result.subscribe {
        log.debug "Stitching result: $it"
    }

    def ref_volume = completed_stitching_result
    | filter { meta, spark -> meta.id == params.registration_fix_image }

    def mov_volumes = completed_stitching_result
    | filter { meta, spark -> meta.id != params.registration_fix_image }

    def fix_global_subpath = params.fix_global_subpath
        ? params.fix_global_subpath
        : "${params.reg_ch}/${params.global_scale}"
    def mov_global_subpath = params.mov_global_subpath
        ? params.mov_global_subpath
        : "${params.reg_ch}/${params.global_scale}"

    def fix_local_subpath = params.fix_local_subpath
        ? params.fix_local_subpath
        : "${params.reg_ch}/${params.local_scale}"
    def mov_local_subpath = params.mov_local_subpath
        ? params.mov_local_subpath
        : "${params.reg_ch}/${params.local_scale}"
    def deform_subpath = params.deform_subpath
        ? params.deform_subpath
        : params.local_scale
    def dask_config = params.dask_config
        ? file(params.dask_config)
        : ''

    def registration_inputs = ref_volume
    | combine(mov_volumes)
    | map {
        def (fix_meta, fix_spark, mov_meta, mov_spark) = it

        def reg_meta = [
            id: "${fix_meta.id}-${mov_meta.id}",
            fix: fix_meta,
            mov: mov_meta,
        ]
        def fix = "${fix_meta.stitching_dir}/${fix_meta.stitching_result}"
        def mov = "${mov_meta.stitching_dir}/${mov_meta.stitching_result}"

        def registration_working_dir = file("${outdir}/registration/${reg_meta.id}")
        def registration_output = outdir
        def registration_dataset = mov_meta.id
        def dask_work_dir = file("${fix_meta.session_work_dir}/dask/${reg_meta.id}")

        def deformations = get_warped_subpaths().collect { warped_subpath ->
            def deformation_input = [
                fix, "${fix_meta.stitching_dataset}/${warped_subpath}", '',
                mov, "${mov_meta.stitching_dataset}/${warped_subpath}", '',
                "${registration_output}/${params.registration_result_container}", '',
            ]
            log.debug "Deformation input: warped_subpath -> ${deformation_input}"
            deformation_input
        }

        def ri =  [
            reg_meta,

            fix, // global_fixed
            "${fix_meta.stitching_dataset}/${fix_global_subpath}", // global_fixed_subpath
            mov, // global_moving
            "${mov_meta.stitching_dataset}/${mov_global_subpath}", // global_moving_subpath
            '', '', // global_fixed_mask, global_fixed_mask_dataset
            '', '', // global_moving_mask, global_fixed_moving_dataset

            params.global_steps,
            registration_working_dir, // global_transform_output
            'aff/affine.mat', // global_transform_name
            registration_output, // global_align_output
            "aff/${params.registration_result_container}", // global_aligned_name
            '',    // global_alignment_subpath

            fix, // local_fixed
            "${fix_meta.stitching_dataset}/${fix_local_subpath}", // local_fixed_subpath
            mov, // local_moving
            "${mov_meta.stitching_dataset}/${mov_local_subpath}", // local_moving_subpath
            '', '', // local_fixed_mask, local_fixed_mask_dataset
            '', '', // local_moving_mask, local_fixed_moving_dataset

            params.local_steps,
            registration_working_dir, // local_transformation_output
            "transform", deform_subpath, // local_transform_name, local_transform_dataset
            "invtransform", deform_subpath, // local_inv_transform_name, local_inv_transform_dataset
            registration_output, // local_align_output
            params.registration_result_container, // local_aligned_name
            '', // local_aligned_subpath

            deformations,

            params.with_dask_cluster,
            dask_work_dir,
            dask_config,
            params.local_align_workers,
            params.local_align_min_workers,
            params.local_align_worker_cpus,
            params.local_align_worker_mem_gb,
        ]
        log.debug "Registration inputs: $it -> $ri"
        ri
    }

    def registration_results = BIGSTREAM_REGISTRATION(
        registration_inputs,
        params.bigstream_config,
        params.global_align_cpus,
        params.global_align_mem_gb,
        params.local_align_cpus,
        params.local_align_mem_gb,
    ).local

    registration_results | view

    emit:
    done = registration_results
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

def get_warped_subpaths() {
    def warped_channels_param = params.warped_channels ?: params.channels
    def warped_scales_param = params.warped_scales ?: params.local_scale

    if (params.warped_subpaths) {
        as_list(params.warped_subpaths)
    } else if (warped_channels_param && warped_scales_param) {
        warped_scales = as_list(warped_scales_param)
        warped_channels = as_list(warped_channels_param)
        [warped_channels, warped_scales]
            .combinations()
            .collect { warped_ch, warped_scale ->
                "${warped_ch}/${warped_scale}"
	    }
    } else {
        []
    }
}

def as_list(v) {
    def vlist
    if (v instanceof Collection) {
        vlist = deformation_entries
    } else if (v) {
        vlist = v.tokenize(', ')
    } else {
        vlist = []
    }
    vlist
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
