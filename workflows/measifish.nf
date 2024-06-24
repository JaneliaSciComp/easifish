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

include { INPUT_CHECK           } from '../subworkflows/local/input_check'
include { STITCHING             } from '../subworkflows/local/stitching'

include { BIGSTREAM_GLOBALALIGN } from '../modules/janelia/bigstream/globalalign/main'

include { DASK_START            } from '../subworkflows/janelia/dask_start/main'

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

    def global_fix_mask_file = params.global_fix_mask ? file(params.global_fix_mask) : []
    def global_mov_mask_file = params.global_mov_mask ? file(params.global_mov_mask) : []

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
    | map {
        def (reg_meta, fix_meta, mov_meta) = it

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def global_registration_working_dir = file("${outdir}/affine-registration/${reg_meta.id}")
        def global_registration_output = file("${outdir}/aff")

        def ri =  [
            reg_meta,

            fix, // global_fixed
            "${fix_meta.stitched_dataset}/${fix_global_subpath}", // global_fixed_subpath
            mov, // global_moving
            "${mov_meta.stitched_dataset}/${mov_global_subpath}", // global_moving_subpath
            global_fix_mask_file, params.global_fix_mask_subpath,
            global_mov_mask_file, params.global_mov_mask_subpath,

            params.global_steps,
            global_registration_working_dir, // global_transform_output
            'affine.mat', // global_transform_name
            global_registration_output, // global_align_output
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
    | map {
        def (reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath) = it
        [
           reg_meta.fix_id, reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath,
        ]
    }

    global_registration_results.subscribe {
        log.debug "Completed global alignment -> $it"
    }

    def dask_work_dir = file("${session_work_dir}/dask/")
    def dask_config = params.dask_config_file ? file(params.dask_config) : ''

    def local_fix_mask_file = params.local_fix_mask ? file(params.local_fix_mask) : []
    def local_mov_mask_file = params.local_mov_mask ? file(params.local_mov_mask) : []

    def prepare_cluster_inputs = global_registration_results.toList()
    | flatMap { global_bigstream_results ->
        def r = global_bigstream_results
        .collect { fix_id, reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath ->
            [
                [id: fix_id]: [ fix, mov, transform_dir]
            ]
        }
        .inject([:]) { result, current ->
            current.each { k, v ->
                if (result[k] != null) {
                    result[k] = result[k] + v
                } else {
                    result[k] = v
                }
            }
    	    result
        }
        .collect { k, v ->
            [
                k,
		        v + // append dask_work_dir and the masks if they are set
		        [ dask_work_dir ] +
                ( dask_config_file ? [dask_config_file] : [] )
		        ( local_fix_mask_file ? [local_fix_mask_file] : [] ) +
		        ( local_mov_mask_file ? [local_mov_mask_file] : [] ),
            ]
        }
        log.info "Collected files for dask: $r"
        r
    }

    def cluster_info = DASK_START(
        prepare_cluster_inputs,
        params.with_dask_cluster,
        dask_work_dir,
        params.local_align_workers,
        params.local_align_min_workers,
        params.local_align_worker_cpus,
        params.local_align_worker_mem_gb ?: params.default_mem_gb_per_cpu * params.local_align_worker_cpus,
    )

    def fix_local_subpath = params.fix_local_subpath
        ? params.fix_local_subpath
        : "${params.reg_ch}/${params.local_scale}"
    def mov_local_subpath = params.mov_local_subpath
        ? params.mov_local_subpath
        : "${params.reg_ch}/${params.local_scale}"

    def global_transform_with_dask_cluster = cluster_info
    | map { dask_meta, dask_context ->
        log.info "Dask cluster -> ${dask_meta}, ${dask_context}"
        [
            dask_meta.id /* fix_id */, dask_meta, dask_context, 
        ]
    }
    | combine(global_registration_results, by:0)
    | map {
        def (fix_id,
             dask_meta, dask_context,
             reg_meta,
             global_fix, global_fix_subpath, 
             global_mov, global_mov_subpath,
             global_transform_dir,
             global_transform_name,
             global_align_dir,
             global_align_name, global_align_subpath) = it
        [
            reg_meta,
            global_transform_dir,
            global_transform_name,
            dask_meta, dask_context,
        ]
    }

    def local_registration_inputs = registration_inputs
    | map {
        def (reg_meta, fix_meta, mov_meta) = it

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def local_registration_working_dir = file("${outdir}/local-registration/${reg_meta.id}")
        def local_registration_output = file("${outdir}")

        def ri =  [
            reg_meta,

            fix, // local_fixed
            "${fix_meta.stitched_dataset}/${fix_local_subpath}", // local_fixed_subpath
            mov, // local_moving
            "${mov_meta.stitched_dataset}/${mov_local_subpath}", // local_moving_subpath
            local_fix_mask_file, params.local_fix_mask_subpath,
            local_mov_mask_file, params.local_mov_mask_subpath,

            params.local_steps,
            local_registration_working_dir, // local_transform_output
            'transform', // local_transform_name
            'invtransform', // local_inv_transform_name
            local_registration_output, // local_align_output
            params.registration_result_container, // local_aligned_name
            '',    // local_alignment_subpath (defaults to mov_global_subpath)
        ]
        log.debug "Prepare local registration inputs: $it -> $ri"
        ri
    }
    | join(global_transform_with_dask_cluster, by:0)
    | multiMap {
        def (
            reg_meta,

            local_fix, local_fix_subpath,
            local_mov, local_mov_subpath,

	        local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,

            local_steps,
            local_registration_working_dir, // local_transform_output
            local_transform_name, local_inv_transform_name,
            local_registration_output, // local_align_output
            local_align_name,
            local_align_subpath,

            global_transform_dir,
            global_transform_name,

            dask_meta, dask_context
        ) = it
        def data = [
            reg_meta,

            local_fix, local_fix_subpath,
            local_mov, local_mov_subpath,

	        local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,

            global_transform_name ? "${global_transform_dir}/${global_transform_name}" : [],

            local_steps,
            local_registration_working_dir, // local_transform_output
            local_transform_name, '' /* local_transform_subpath */,
            local_inv_transform_name, '' /* local_inv_transform_subpath */,

            local_registration_output, // local_align_output
            local_align_name, local_align_subpath,
        ]
        def cluster = [
            dask_context.scheduler_address,
            dask_config_file,
        ]
        log.debug "Local registration inputs: $it -> $data, $cluster"
        data: data
        cluster: cluster
    }

    def local_registration_results = BIGSTREAM_LOCALALIGN(
        local_registration_inputs.data,
        bigstream_config,
        local_registration_inputs.cluster,
        params.local_align_cpus,
        params.local_align_mem_gb ?: params.default_mem_gb_per_cpu * params.local_align_cpus,
    )

    local_registration_results.subscribe {
        // [
        //    meta, fix, fix_subpath, mov, mov_subpath,
        //    local_transform_output,
        //    local_deform, local_deform_subpath,
        //    local_inv_deform, local_inv_deform_subpath
        //    warped_output, warped_name_only, warped_subpath
        //  ]
        log.info "Completed local alignment -> $it"
    }

    local_registration_results | view

    emit:
    done = local_registration_results
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
