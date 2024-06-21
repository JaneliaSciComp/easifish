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
include { STITCHING              } from '../subworkflows/local/stitching'

include { BIGSTREAM_GLOBALALIGN } from '../modules/janelia/globalalign/main'

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

    def session_work_dir = "${params.workdir}/${workflow.sessionId}"
    def stitching_dir = params.stitching_dir ? file(params.stitching_dir) : "${outdir}/stitching"
    def stitching_result_dir = params.stitching_result_dir ? file(params.stitching_result_dir) : outdir

    def ch_acquisitions = INPUT_CHECK (
        samplesheet_file,
        indir,
        stitching_dir,
    )
    .acquisitions

    ch_versions = ch_versions.mix(INPUT_CHECK.out.versions)
    // TODO: OPTIONAL, you can use nf-validation plugin to create an input channel from the samplesheet with Channel.fromSamplesheet("input")
    // See the documentation https://nextflow-io.github.io/nf-validation/samplesheets/fromSamplesheet/
    // ! There is currently no tooling to help you write a sample sheet schema

    def stitching_result = STITCHING(
        ch_acquisitions,
        params.flatfield_correction,
        params.spark_cluster,
        stitching_dir,
        params.darkfieldfile,
        params.flatfieldfile,
        stitching_result_dir,
        params.stitching_result_container,
        true, // use ID for stitched dataset subpath
        session_work_dir,
        params.skip_stitching,
        params.spark_workers as int,
        params.min_spark_workers as int,
        params.spark_worker_cores as int,
        params.spark_gb_per_core as int,
        params.spark_driver_cores as int,
        params.spark_driver_mem_gb as int,
    )

    stitching_result.subscribe { log.debug "Stitched image ready for the next step: $it " }

    def ref_volume = stitching_result
    | filter { meta ->
        log.debug "Check ${meta} if it is fixed image"
        meta.id == params.registration_fix_id
    }

    ref_volume.subscribe { log.debug "Fix image: $it" }

    def mov_volumes = stitching_result
    | filter { meta ->
        log.debug "Check ${meta} if it is fixed image"
        meta.id != params.registration_fix_id
    }

    mov_volumes.subscribe { log.debug "Moving image: $it" }

    def fix_global_subpath = params.fix_global_subpath
        ? params.fix_global_subpath
        : "${params.reg_ch}/${params.global_scale}"
    def mov_global_subpath = params.mov_global_subpath
        ? params.mov_global_subpath
        : "${params.reg_ch}/${params.global_scale}"

    def global_fix_mask = params.global_fix_mask ? file(params.global_fix_mask) : []
    def global_mov_mask = params.global_mov_mask ? file(params.global_mov_mask) : []

    def bigstream_config = params.bigstream_config ? file(params.bigstream_config) : []

    def registration_inputs = ref_volume
    | combine(mov_volumes)
    | map {
        def (fix_meta, mov_meta) = it
        log.debug "Prepare registration inputs: $it -> ${fix_meta}, ${mov_meta}"
        def reg_meta = [
            id: "${fix_meta.id}-${mov_meta.id}",
            fix_id: fix_meta.id,
            mov_id: mov_meta.id,
        ]
        [ reg_meta, fix_meta, mov_meta ]
    }

    def global_registration_inputs = registration_inputs
    | map { reg_meta, fix_meta, mov_meta ->

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def registration_working_dir = file("${outdir}/affine-registration/${reg_meta.id}")
        def registration_output = file("${outdir}/aff")
        def registration_dataset = mov_meta.id

        def ri =  [
            reg_meta,

            fix, // global_fixed
            "${fix_meta.stitched_dataset}/${fix_global_subpath}", // global_fixed_subpath
            mov, // global_moving
            "${mov_meta.stitched_dataset}/${mov_global_subpath}", // global_moving_subpath
            global_fix_mask, params.global_fix_mask_subpath,
            global_mov_mask, params.global_mov_mask_subpath,

            params.global_steps,
            registration_working_dir, // global_transform_output
            'affine.mat', // global_transform_name
            registration_output, // global_align_output
            params.registration_result_container, // global_aligned_name
            '',    // global_alignment_subpath (defaults to mov_global_subpath)
        ]
        log.debug "Global registration inputs: $it -> $ri"
        ri
    }

    def global_registration_results = BIGSTREAM_GLOBALALIGN(
        global_registration_inputs,
        bigstream_config,
        params.global_align_cpus,
        params.global_align_mem_gb ?: params.default_mem_gb_per_cpu * params.global_align_cpus,
    )

    global_align_results.subscribe {
        log.debug "Completed global alignment -> $it"
    }

    def cluster_files = global_align_results.toList()
    | map { global_bigstream_results ->
        global_bigstream_results
        .collect { reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath ->
            [
                reg_meta.fix_id: [ fix, mov, transform_dir, align_dir]
            ]
        }
        .inject([:]) { current, result ->
            result + current
        }
    }

    emit:
    done = cluster_files
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
