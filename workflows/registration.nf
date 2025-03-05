/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { MULTISCALE_PYRAMID       } from '../modules/local/multiscale/pyramid/main'

include { MULTISCALE               } from '../subworkflows/local/multiscale'

include { BIGSTREAM_GLOBALALIGN    } from '../modules/janelia/bigstream/globalalign/main'
include { BIGSTREAM_LOCALALIGN     } from '../modules/janelia/bigstream/localalign/main'
include { BIGSTREAM_COMPUTEINVERSE } from '../modules/janelia/bigstream/computeinverse/main'
include { BIGSTREAM_DEFORM         } from '../modules/janelia/bigstream/deform/main'

include { DASK_START               } from '../subworkflows/janelia/dask_start/main'
include { DASK_STOP                } from '../subworkflows/janelia/dask_stop/main'
include { SPARK_START              } from '../subworkflows/janelia/spark_start/main'
include { SPARK_STOP               } from '../subworkflows/janelia/spark_stop/main'

include { as_list                  } from './util_functions'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN REGISTRATION WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow REGISTRATION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outdir

    main:
    def session_work_dir = "${params.workdir}/${workflow.sessionId}"

    def ref_volume = ch_meta
    | filter { meta ->
        log.debug "Check ${meta} if it is fixed image"
        meta.id == params.registration_fix_id
    }

    ref_volume.subscribe { log.debug "Fix image: $it" }

    def mov_volumes = ch_meta
    | filter { meta ->
        log.debug "Check ${meta} if it is fixed image"
        meta.id != params.registration_fix_id
    }

    mov_volumes.subscribe { log.debug "Moving image: $it" }

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

    def reg_outdir = file("${outdir}/${params.registration_subdir}")
    def global_registration_results = RUN_GLOBAL_REGISTRATION(
        registration_inputs,
        bigstream_config,
        reg_outdir,
        params.skip_global_align || params.skip_registration,
    )

    def additional_cluster_files = get_params_as_list_of_files(
        [
            params.local_fix_mask,
            params.local_mov_mask,
        ]
    )
    def local_registrations_cluster = START_EASIFISH_DASK(
        global_registration_results.global_registration_results,
        additional_cluster_files,
        params.with_dask_cluster && !params.skip_registration,
        "${session_work_dir}/bigstream-dask/",
        params.dask_config,
    )

    def local_registration_results = RUN_LOCAL_REGISTRATION(
        registration_inputs,
        global_registration_results.global_transforms,
        local_registrations_cluster,
        bigstream_config,
        reg_outdir,
        params.skip_local_align || params.skip_registration,
    )

    def local_inverse_results = RUN_COMPUTE_INVERSE(
        registration_inputs,
        local_registration_results,
        local_registrations_cluster,
        params.skip_inverse || params.skip_registration,
    )

    def local_deformation_results = RUN_LOCAL_DEFORMS(
        registration_inputs,
        local_registration_results,
        local_registrations_cluster,
        params.skip_deformations || params.skip_registration,
    )

    def stopped_clusters = local_deformation_results
    | combine(local_registrations_cluster, by: 0)
    | combine(local_inverse_results, by: 0)
    | map {
        def (
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            dask_meta, dask_context
        ) = it
        def r = [ dask_meta, dask_context, reg_meta ]
        log.debug "The dask cluster $r ($it) can be stopped"
        log.debug "Finished warping ${warped}, ${warped_subpath} on dask cluster ${dask_meta}, ${dask_context}"
        r
    }
    | groupTuple(by: [0, 1])
    | map {
        def (dask_meta, dask_context, reg_metas) = it
        log.debug "Prepare to stop dask cluster used for $reg_metas -> [ ${dask_meta}, ${dask_context} ]"
        [ dask_meta, dask_context ]
    }
    | DASK_STOP

    stopped_clusters.subscribe { log.debug "Stopped dask cluster $it" }

    RUN_MULTISCALE_WITH_SINGLE_CLUSTER(
        local_deformation_results,
        "${session_work_dir}/multiscale",
    )

    emit:
    done = stopped_clusters
}

workflow RUN_GLOBAL_REGISTRATION {

    take:
    registration_inputs
    bigstream_config
    reg_outdir
    skip_global_align

    main:
    def fix_global_subpath = params.fix_global_subpath
        ? params.fix_global_subpath
        : "${params.reg_ch}/${params.global_scale}"
    def mov_global_subpath = params.mov_global_subpath
        ? params.mov_global_subpath
        : "${params.reg_ch}/${params.global_scale}"

    def global_fix_mask_file = params.global_fix_mask ? file(params.global_fix_mask) : []
    def global_mov_mask_file = params.global_mov_mask ? file(params.global_mov_mask) : []

    def global_registration_inputs = registration_inputs
    | map {
        def (reg_meta, fix_meta, mov_meta) = it

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def global_registration_working_dir = file("${reg_outdir}/global/${reg_meta.id}")
        def global_registration_output = file("${reg_outdir}")

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
            'global-affine.mat', // global_transform_name
            global_registration_output, // global_align_output
            params.global_registration_container, // global_aligned_name
            '',    // global_alignment_subpath (defaults to mov_global_subpath)
        ]
        log.debug "Global registration inputs: $it -> $ri"
        ri
    }
    if (!skip_global_align) {
        global_registration_results = BIGSTREAM_GLOBALALIGN(
            global_registration_inputs,
            bigstream_config,
            params.global_align_cpus,
            params.global_align_mem_gb ?: params.default_mem_gb_per_cpu * params.global_align_cpus,
        )
    } else {
        global_registration_results = global_registration_inputs
        | map {
            def ri =(
                reg_meta,
                fix, fix_subpath,
                mov, mov_subpath,
                fix_mask, fix_mask_subpath,
                mov_mask, mov_mask_subpath,
                steps,
                transform_dir,
                transform_name,
                align_dir, align_name, align_subpath
            ) = it
            log.debug "Skip global alignment $it"
            [
                reg_meta,
                fix, fix_subpath,
                mov, mov_subpath,
                transform_dir, transform_name,
                align_dir, align_name, align_subpath,
            ]
        }
    }
    // Prepare global transform output
    global_transforms = global_registration_results
    | map {
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            transform_dir, transform_name,
            align_dir, align_name, align_subpath) = it
        log.debug "Completed global alignment: $it"
        def r = [
        reg_meta, "${transform_dir}/${transform_name}",
        ]
        log.debug "Global transform $it -> $r"
        r
    }

    emit:
    global_transforms
    global_registration_results
}

workflow START_EASIFISH_DASK {
    take:
    global_registration_results // ch: [reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath]
    additional_files_list // list of additional files to be mapped in the dask cluster
    start_dask_cluster
    dask_work_dir // dask work dir
    dask_config   // dask config

    main:
    def dask_work_dir_file = dask_work_dir ? file(dask_work_dir) : []
    def dask_config_file = dask_config ? file(dask_config) : []

    def extended_global_registration_results = global_registration_results
    | map {
        def (reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath) = it
        [
           reg_meta.fix_id, reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath,
        ]
    }

    def cluster_files = extended_global_registration_results
    | toList() // wait for all global registrations to complete
    | flatMap { global_bigstream_results ->
        def r = global_bigstream_results
        .collect { fix_id, reg_meta, fix, fix_subpath, mov, mov_subpath, transform_dir, transform_name, align_dir, align_name, align_subpath ->
            // collect the data files into a map with
            // key = meta and value is a set of files
            def data_dir_set = [ fix, mov, transform_dir].toSet()
            [
                [id: fix_id]: data_dir_set,
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
                v +
                additional_files_list +
                (dask_work_dir_file ? [ dask_work_dir_file ] : []) +
                (dask_config_file ? [ dask_config_file ] : [] )
            ]
        }
        log.debug "Collected files for dask: $r"
        r
    }

    def cluster_info = DASK_START(
        cluster_files,
        start_dask_cluster,
        dask_config_file,
        dask_work_dir_file,
        params.local_align_workers,
        params.local_align_min_workers,
        params.local_align_worker_cpus,
        params.local_align_worker_mem_gb ?: params.default_mem_gb_per_cpu * params.local_align_worker_cpus,
    )

    def local_registrations_dask_cluster = cluster_info
    | map { dask_meta, dask_context ->
        log.debug "Dask cluster -> ${dask_meta}, ${dask_context}"
        [
            dask_meta.id /* fix_id */, dask_meta, dask_context,
        ]
    }
    | combine(extended_global_registration_results, by:0)
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
        def registration_cluster = [
            reg_meta,
            dask_meta,
            dask_context + [ config: dask_config_file ],
        ]
        log.debug "Use local registration cluster: ${registration_cluster}"
        registration_cluster
    }

    emit:
    cluster = local_registrations_dask_cluster
}

workflow RUN_LOCAL_REGISTRATION {
    take:
    registration_inputs // ch: [ reg_meta, fix_meta, mov_meta]
    global_transforms   // ch: [ reg_meta, global_transform ]
    local_registrations_dask_cluster // ch: [ reg_meta, dask_meta, dask_context ]
    bigstream_config    // string|file bigstream yaml config
    reg_outdir
    skip_local_registration

    main:
    def fix_local_subpath = params.fix_local_subpath
        ? params.fix_local_subpath
        : "${params.reg_ch}/${params.local_scale}"
    def mov_local_subpath = params.mov_local_subpath
        ? params.mov_local_subpath
        : "${params.reg_ch}/${params.local_scale}"

    def local_fix_mask_file = params.local_fix_mask ? file(params.local_fix_mask) : []
    def local_mov_mask_file = params.local_mov_mask ? file(params.local_mov_mask) : []

    def local_registration_inputs = registration_inputs
    | join(global_transforms, by: 0)
    | map {
        def (reg_meta, fix_meta, mov_meta, global_transform) = it

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def local_registration_working_dir = file("${reg_outdir}/local/${reg_meta.id}")
        def local_registration_output = file("${reg_outdir}")

        def ri =  [
            reg_meta,

            fix, // local_fixed
            "${fix_meta.stitched_dataset}/${fix_local_subpath}", // local_fixed_subpath
            mov, // local_moving
            "${mov_meta.stitched_dataset}/${mov_local_subpath}", // local_moving_subpath

            local_fix_mask_file, params.local_fix_mask_subpath,
            local_mov_mask_file, params.local_mov_mask_subpath,

            global_transform,

            params.local_steps,
            local_registration_working_dir,   // local_transform_output
            params.local_transform_name, '',  // local_deform_name
            '', '',                           // local_inv_deform_name - no inverse - we compute this separately
            local_registration_output,        // local_align_output
            '',                               // local_aligned_name - do not apply the deform transform
            '',                               // local_alignment_subpath (defaults to mov_global_subpath)
        ]
        log.debug "Prepare local registration inputs: $it -> $ri"
        ri
    }
    | join(local_registrations_dask_cluster, by:0)
    | multiMap {
        def (
            reg_meta,

            local_fix, local_fix_subpath,
            local_mov, local_mov_subpath,

	        local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,

            global_transform,

            local_steps,
            local_registration_working_dir, // local_transform_output
            local_transform_name, local_transform_subpath,
            local_inv_transform_name, local_inv_transform_subpath,
            local_registration_output, // local_align_output
            local_align_name, local_align_subpath,

            dask_meta, dask_context
        ) = it
        def data = [
            reg_meta,

            local_fix, local_fix_subpath,
            local_mov, local_mov_subpath,

	        local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,

            global_transform,

            local_steps,
            local_registration_working_dir, // local_transform_output
            local_transform_name, local_transform_subpath,
            local_inv_transform_name, local_inv_transform_subpath,

            local_registration_output, // local_align_output
            local_align_name, local_align_subpath,
        ]
        def cluster = [
            dask_context.scheduler_address,
            dask_context.config,
        ]
        log.debug "Local registration inputs: $it -> $data, $cluster"
        data: data
        cluster: cluster
    }

    if (!skip_local_registration) {
        local_registration_results = BIGSTREAM_LOCALALIGN(
            local_registration_inputs.data,
            bigstream_config,
            local_registration_inputs.cluster,
            params.local_align_cpus,
            params.local_align_mem_gb ?: params.default_mem_gb_per_cpu * params.local_align_cpus,
        )

        local_registration_results.subscribe {
            // [
            //    meta, fix, fix_subpath, mov, mov_subpath,
            //    affine_transform,
            //    local_deform_dir,
            //    local_deform, local_deform_subpath,
            //    local_inv_deform, local_inv_deform_subpath
            //    warped_output, warped_name_only, warped_subpath
            //  ]
            log.debug "Completed local alignment -> $it"
        }
    } else {
        local_registration_results = local_registration_inputs.data
        | map {
            def data = (
                reg_meta,
                local_fix, local_fix_subpath,
                local_mov, local_mov_subpath,
	            local_fix_mask, local_fix_mask_subpath,
                local_mov_mask, local_mov_mask_subpath,
                global_transform,
                local_steps,
                local_registration_working_dir, // local_transform_output
                local_transform_name, local_transform_subpath,
                local_inv_transform_name, local_inv_transform_subpath,
                local_registration_output, // local_align_output
                local_align_name, local_align_subpath
            ) = it
            log.debug "Skip local alignment $it"
            [
                reg_meta,
                local_fix, local_fix_subpath,
                local_mov, local_mov_subpath,
                global_transform,
                local_registration_working_dir,
                local_transform_name, local_transform_subpath ?: local_mov_subpath,
                local_inv_transform_name, local_inv_transform_subpath ?: local_mov_subpath,
                local_registration_output,
                local_align_name, local_align_subpath
            ]
        }
    }

    emit:
    local_registration_results
}

workflow RUN_COMPUTE_INVERSE {
    take:
    registration_inputs
    local_registration_results
    local_registrations_cluster
    skip_inverse

    main:
    def compute_inv_inputs = registration_inputs
    | join(local_registration_results, by: 0)
    | join(local_registrations_cluster, by: 0)
    | map {
        def (
            reg_meta, fix_meta, mov_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            affine_transform,
            local_transform_output,
            local_transform, local_transform_subpath,
            local_inv_transform, local_inv_transform_subpath,
            warped_output, local_warped_name, local_warped_subpath,
            dask_meta, dask_context
        ) = it
        log.debug "Prepare compute inverse inputs: $it"
        [
            [
                reg_meta,
                local_transform_output,
                local_transform, local_transform_subpath,
                local_transform_output,
                params.local_inv_transform_name ?: "inv-${local_transform}", local_transform_subpath,
            ],
            dask_context,
        ]
    }

    if (!skip_inverse) {
        inverse_results = BIGSTREAM_COMPUTEINVERSE(
            compute_inv_inputs.map { it[0] },
            compute_inv_inputs.map { [ it[1].scheduler_address, it[1].config ] },
            params.local_inverse_cpus,
            params.local_inverse_mem_gb ?: params.default_mem_gb_per_cpu * params.local_inverse_cpus,
        )
        inverse_results.subscribe {
            log.debug "Completed inverse -> $it"
        }
    } else {
        inverse_results = compute_inv_inputs.map { it[0] }
        inverse_results.subscribe {
            log.debug "Skipped inverse -> $it"
        }
    }

    emit:
    inverse_results
}

workflow RUN_LOCAL_DEFORMS {
    take:
    registration_inputs
    local_registration_results
    local_registrations_cluster
    skip_deformations

    main:
    def deformation_inputs = registration_inputs
    | join(local_registration_results, by: 0)
    | join(local_registrations_cluster, by: 0)
    | flatMap {
        def (
            reg_meta, fix_meta, mov_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            affine_transform,
            local_transform_output,
            local_deform, local_deform_subpath,
            local_inv_deform, local_inv_deform_subpath,
            warped_output, local_warped_name, local_warped_subpath,
            dask_meta, dask_context
        ) = it
        log.debug "Prepare deformation inputs: $it"
        def warped_name = local_warped_name ?: params.local_registration_container
        def r = get_warped_subpaths()
	            .findAll { fix_warped_subpath, warped_subpath -> warped_subpath != local_warped_subpath }
                .collect { fix_warped_subpath, warped_subpath ->
                    def deformation_input = [
                        reg_meta,
                        fix, "${fix_meta.stitched_dataset}/${fix_warped_subpath}", ''/* fix_spacing */,
                        mov, "${mov_meta.stitched_dataset}/${warped_subpath}", ''/* mov_spacing */,

                        affine_transform,

                        "${local_transform_output}/${local_deform}", local_deform_subpath,

                        "${warped_output}/${warped_name}", "${mov_meta.stitched_dataset}/${warped_subpath}",
                    ]
                    def r = [ deformation_input, dask_context ]
                    log.debug "Deformation input: ${warped_subpath} -> $r "
                    r
                }
        r
    }

    if (!skip_deformations) {
        deformation_results = BIGSTREAM_DEFORM(
            deformation_inputs.map { it[0] },
            deformation_inputs.map { [ it[1].scheduler_address, it[1].config ] },
            params.local_deform_cpus,
            params.local_deform_mem_gb ?: params.default_mem_gb_per_cpu * params.local_deform_cpus,
        )

        deformation_results.subscribe {
            log.debug "Completed deformation -> $it"
        }
    } else {
        deformation_results = deformation_inputs
        | map {
            def (
                reg_meta,
                fix, fix_subpath, fix_spacing,
                mov, mov_subpath, mov_spacing,
                affine_transform,
                deform_transform, deform_transform_subpath,
                warped, warped_subpath
            ) = it[0]
            def r = [
                reg_meta,
                fix, fix_subpath,
                mov, mov_subpath,
                warped, warped_subpath,
            ]
            log.debug "Skip deformation -> $r"
            r
        }
    }

    emit:
    deformation_results
}

workflow RUN_MULTISCALE_WITH_SINGLE_CLUSTER {
    take:
    deformation_results // ch: [ meta, fix, fix_subpath, mov, mov_subpath, warped, warped_subpath ]
    multiscale_work_dir // string|file

    main:
    def multiscale_inputs = deformation_results
    | map {
        def (
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath
        ) = it
        def r = [
            reg_meta.mov_id, warped, warped_subpath,
        ]
        log.debug "Multiscale input: $it -> $r"
        r
    }

    def multiscale_cluster_data = multiscale_inputs
    | toList() // wait for all deformations to complete
    | flatMap { all ->
        all
        .collect { id, data_dir, data_subpath ->
            // convert to a map in which the
            // key = meta, value = a list containing data_dir
            def data_dir_set = [ data_dir ].toSet()
            [
                [id: id]: data_dir_set,
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
            // convert the key value back to a tuple
            [ k, v ]
        }
    }
    | map { meta, data_dirs ->
        meta.session_work_dir = "${multiscale_work_dir}/${meta.id}"
        def r = [ meta, data_dirs ]
        log.debug "Multiscale cluster data: $r"
        r
    }

    if (!params.skip_multiscale) {
        def downsample_input = SPARK_START(
            multiscale_cluster_data,
            params.multiscale_with_spark_cluster,
            multiscale_work_dir,
            params.multiscale_spark_workers ?: params.spark_workers,
            params.multiscale_min_spark_workers,
            params.multiscale_spark_worker_cores ?: params.spark_worker_cores,
            params.multiscale_spark_gb_per_core ?: params.spark_gb_per_core,
            params.multiscale_spark_driver_cores,
            params.multiscale_spark_driver_mem_gb,
        ) // ch: [ meta, spark ]
        | map { meta, spark ->
            [ meta.id, meta, spark ]
        }
        | combine(multiscale_inputs, by: 0)
        | map {
            def (id, meta, spark, n5_container, fullscale_dataset) = it
            def r = [
                meta, n5_container, fullscale_dataset, spark,
            ]
            log.debug "Downsample input: $it -> $r"
            r
        }

        MULTISCALE_PYRAMID(downsample_input)

        def spark_cluster_to_stop = MULTISCALE_PYRAMID.out.data
        | map {
            def (meta, n5_container, fullscale_dataset, spark) = it
            log.debug "Completed downsampling  $it"
            // spark_stop only needs meta and spark
            log.debug "Prepare to stop [${meta}, ${spark}]"
            [ meta, spark ]
        }
        | groupTuple(by: [0, 1])

        completed_downsampling = SPARK_STOP(spark_cluster_to_stop, params.multiscale_with_spark_cluster)
        | map {
            def (meta, spark) = it
            log.debug "Stopped multiscale spark ${spark} - downsampled result: $meta"
            meta
        }
    } else {
        completed_downsampling = multiscale_cluster_data
        | map {
            def (meta) = it
            log.debug "Skipped multiscale - returned result: $meta"
            meta
        }
    }

    emit:
    completed_downsampling // ch: [ meta ]
}

workflow RUN_MULTISCALE_WITH_CLUSTER_PER_TASK {
    take:
    deformation_results // ch: [ meta, fix, fix_subpath, mov, mov_subpath, warped, warped_subpath ]
    multiscale_work_dir // string|file

    main:
    def multiscale_inputs = deformation_results
    | map {
        def (
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath
        ) = it
        def multiscale_meta = [
            id: reg_meta.mov_id,
        ]
        def r = [
            multiscale_meta, warped, warped_subpath,
        ]
        log.debug "Multiscale input: $it -> $r"
        r
    }

    completed_downsampling = MULTISCALE(
        multiscale_inputs,
        params.multiscale_with_spark_cluster,
        multiscale_work_dir,
        params.skip_multiscale,
        params.multiscale_spark_workers ?: params.spark_workers,
        params.multiscale_min_spark_workers,
        params.multiscale_spark_worker_cores ?: params.spark_worker_cores,
        params.multiscale_spark_gb_per_core ?: params.spark_gb_per_core,
        params.multiscale_spark_driver_cores,
        params.multiscale_spark_driver_mem_gb,
    )

    emit:
    completed_downsampling
}

def get_params_as_list_of_files(lparams) {
    lparams
        .findAll { it }
        .collect { file(it) }
}

def get_warped_subpaths() {
    def warped_channels_param = params.warped_channels ?: params.channels
    def warped_scales_param = params.warped_scales ?: params.local_scale

    if (params.warped_subpaths) {
        as_list(params.warped_subpaths)
            .collect { warped_subpath_param ->
                def (fix_subpath, warped_subpath) = warped_subpath_param.tokenize(':')
                [
                    fix_subpath,
                    warped_subpath ?: fix_subpath,
                ]
            }
    } else if (warped_channels_param && warped_scales_param) {
        warped_scales = as_list(warped_scales_param)
        warped_channels = as_list(warped_channels_param)
        [warped_channels, warped_scales]
            .combinations()
            .collect { warped_ch, warped_scale ->
                [
                    "${warped_ch}/${warped_scale}", // fixed subpath
                    "${warped_ch}/${warped_scale}", // warped subpath
                ]
	    }
    } else {
        []
    }
}
