/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { BIGSTREAM_GLOBALALIGN                                    } from '../modules/janelia/bigstream/globalalign/main'
include { BIGSTREAM_LOCALALIGN                                     } from '../modules/janelia/bigstream/localalign/main'
include { BIGSTREAM_COMPUTEINVERSE                                 } from '../modules/janelia/bigstream/computeinverse/main'
include { BIGSTREAM_DEFORM                                         } from '../modules/janelia/bigstream/deform/main'

include { BIGSTREAM_FOREGROUNDMASK as BIGSTREAM_FOREGROUNDMASK_FIX } from '../modules/janelia/bigstream/foregroundmask/main'
include { BIGSTREAM_FOREGROUNDMASK as BIGSTREAM_FOREGROUNDMASK_MOV } from '../modules/janelia/bigstream/foregroundmask/main'

include { BIGSTREAM_CORRELATIONMETRIC                              } from '../modules/janelia/bigstream/correlationmetric/main'

include { DASK_START                                               } from '../subworkflows/janelia/dask_start/main'
include { DASK_STOP                                                } from '../subworkflows/janelia/dask_stop/main'
include { SPARK_START                                              } from '../subworkflows/janelia/spark_start/main'
include { SPARK_STOP                                               } from '../subworkflows/janelia/spark_stop/main'

include { MULTISCALE                                               } from '../subworkflows/local/multiscale'

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

    ref_volume.view { it -> log.debug "Fix image: $it" }

    def mov_volumes = ch_meta
    | filter { meta ->
        log.debug "Check ${meta} if it is fixed image"
        meta.id != params.registration_fix_id
    }

    mov_volumes.view { it -> log.debug "Moving image: $it" }

    def bigstream_config = params.bigstream_config ? file(params.bigstream_config) : []

    def registration_inputs = ref_volume
    | combine(mov_volumes)
    | map { it ->
        def (fix_meta, mov_meta) = it
        def reg_meta = [
            // ids
            id: "${fix_meta.id}-${mov_meta.id}",
            fix_id: fix_meta.id,
            mov_id: mov_meta.id,
            // fix channels info
            fix_sample_channels: fix_meta.sample_channels,
            fix_spots_channels: fix_meta.spots_channels,
            fix_dapi_channel: fix_meta.dapi_channel,
            // mov channels info
            mov_sample_channels: mov_meta.sample_channels,
            mov_spots_channels: mov_meta.spots_channels,
            mov_dapi_channel: mov_meta.dapi_channel,
            // mov channels output mapping
            warped_channels_mapping: mov_meta.warped_channels_mapping ?: [:],
        ]
        log.debug "Prepare registration inputs: $it -> reg_meta:${reg_meta}, fix_meta:${fix_meta}, mov_meta:${mov_meta}"
        [ reg_meta, fix_meta, mov_meta ]
    }

    def reg_outdir = file("${outdir}/${params.registration_subdir}")

    // Resolve fix and mov masks once; same generated mask is used for both global and local.
    def resolved_masks = RESOLVE_MASKS(registration_inputs, outdir)
    resolved_masks.view { it -> log.debug "Resolved masks: $it" }

    def global_registration_results = RUN_GLOBAL_REGISTRATION(
        registration_inputs,
        resolved_masks,
        bigstream_config,
        reg_outdir,
        params.skip_global_align || params.skip_registration,
    )

    global_registration_results.global_transforms.view { it -> log.debug "Global affine transforms: $it" }
    global_registration_results.global_registration_results.view { it -> log.debug "Global transformed results: $it" }

    // Build the list of mask files the dask workers need access to.
    // For generated masks the output path is deterministic so we can list it statically;
    // all per-volume moving masks share the same masks/ subdirectory, so we mount the directory.
    def cluster_mask_files = []

    if (params.generate_fix_mask)
        cluster_mask_files << "${reg_outdir}/masks/${params.fix_mask}"
    else if (params.fix_mask?.startsWith('/'))
        cluster_mask_files << params.fix_mask

    if (params.generate_mov_mask)
        cluster_mask_files << "${reg_outdir}/masks"
    else if (params.mov_mask?.startsWith('/'))
        cluster_mask_files << params.mov_mask

    def additional_cluster_files = ParamUtils.get_params_as_list_of_files(cluster_mask_files)

    def local_registrations_cluster = START_EASIFISH_DASK(
        global_registration_results.global_registration_results,
        additional_cluster_files,
        params.distributed_bigstream &&
        !params.skip_registration &&
        (!params.skip_local_align || !params.skip_deformations || !params.skip_inverse || params.run_warped_multiscale),
        "${session_work_dir}/bigstream-dask/",
        params.dask_config,
    )

    def local_registration_results = RUN_LOCAL_REGISTRATION(
        registration_inputs,
        global_registration_results.global_transforms,
        local_registrations_cluster,
        resolved_masks,
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

    RUN_GLOBAL_ALIGNMENT_CORRELATION(
        registration_inputs,
        global_registration_results.global_registration_results,
        reg_outdir,
        params.run_global_metric,
    )

    RUN_LOCAL_ALIGNMENT_CORRELATION(
        registration_inputs,
        local_deformation_results,
        reg_outdir,
        params.run_local_metric,
    )

    def multiscale_warped_inputs = local_deformation_results
    | combine(local_registrations_cluster, by: 0)
    | map { it ->
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            _dask_meta, dask_context) = it
        log.debug "Prepare multiscale input for warped image $it"
        [
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            dask_context.scheduler_address ?: '',
            dask_context.config ?: [],
        ]
    }
    | groupTuple(by: [0, 1, 2, 3, 4, 5, 6])
    | map { it ->
        def (reg_meta,
             fix, fix_subpath,
             mov, mov_subpath,
             warped, warped_subpath,
             scheduler_addresses, dask_configs) = it
        log.debug "Grouped multiscale input by ${reg_meta.id}: warped=${warped}, warped_subpath=${warped_subpath} (source: $it)"
        def r = [
            [
                reg_meta,
                warped, warped_subpath,
            ],
            [
                scheduler_addresses[0],
                dask_configs[0],
            ],
        ]
        log.debug "Multiscale warped image input $r"
        r
    }

    def multiscale_results = MULTISCALE(
        multiscale_warped_inputs.map { it[0] },
        multiscale_warped_inputs.map { it[1] },
        params.run_warped_multiscale,
        params.multiscale_cpus,
        params.multiscale_mem_gb,
    )

    def all_registration_results = local_deformation_results
    | combine(local_registrations_cluster, by: 0)
    | combine(local_inverse_results, by: 0)
    | combine(multiscale_results, by: 0)
    | combine(global_registration_results.global_transforms, by: 0)
    | map { it ->
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            dask_meta, dask_context,
            transform_output, transform_name, transform_subpath,
            inv_transform_output, inv_transform_name, inv_transform_subpath,
            _ms_warped, _ms_warped_subpath,
            global_transform, global_inv_transform) = it
        log.debug "Prepare all registration results: $it"
        def r = [
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            global_transform, global_inv_transform,
            transform_output,
            transform_name, transform_subpath,
            inv_transform_output,
            inv_transform_name, inv_transform_subpath,
            dask_meta, dask_context,
        ]
        log.debug "Finished warping ${warped}, ${warped_subpath} on dask cluster ${dask_meta}, ${dask_context}"
        log.debug "All registration results: $r"
        r
    }

    def stopped_clusters = all_registration_results
    | map { it ->
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            _global_transform, _global_inv_transform,
            transform_output, transform_name, transform_subpath,
            inv_transform_output, inv_transform_name, inv_transform_subpath,
            dask_meta, dask_context) = it
        def r = [ dask_meta, dask_context, reg_meta ]
        log.debug "Prepare to stop dask cluster $reg_meta"
        r
    }
    | groupTuple(by: [0, 1])
    | map { it ->
        def (dask_meta, dask_context, reg_metas) = it
        log.debug "Prepare to stop dask cluster used for $reg_metas -> [ ${dask_meta}, ${dask_context} ]"
        [ dask_meta, dask_context ]
    }
    | DASK_STOP

    stopped_clusters.view { it -> log.debug "Stopped dask cluster $it" }

    def final_results = all_registration_results
    | map { it ->
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            global_transform, global_inv_transform,
            transform_output, transform_name, transform_subpath,
            inv_transform_output, inv_transform_name, inv_transform_subpath,
            dask_meta, dask_context) = it
        def r = [
            reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            warped, warped_subpath,
            global_transform, global_inv_transform,
            transform_output,
            transform_name, transform_subpath,
            inv_transform_output,
            inv_transform_name, inv_transform_subpath,
        ]
        log.debug "Final registration results: $it -> $r"
        r
    }

    emit:
    done = final_results
}

workflow RUN_GLOBAL_REGISTRATION {

    take:
    registration_inputs
    resolved_masks    // [reg_meta_id, fix_mask, fix_mask_subpath, mov_mask, mov_mask_subpath]
    bigstream_config
    reg_outdir
    skip_global_align

    main:
    def fix_global_subpath = params.fix_global_subpath
        ? params.fix_global_subpath
        : "${params.fix_global_channel ?: params.reg_ch}/${params.global_scale}"
    def mov_global_subpath = params.mov_global_subpath
        ? params.mov_global_subpath
        : "${params.mov_global_channel ?: params.reg_ch}/${params.global_scale}"
    def global_results_subdir = params.global_results_subdir ?: 'global'
    def global_registration_inputs = registration_inputs
    | map { reg_meta, fix_meta, mov_meta -> [reg_meta.id, reg_meta, fix_meta, mov_meta] }
    | join(resolved_masks, by: 0)
    | map { _reg_id, reg_meta, fix_meta, mov_meta,
            global_fix_mask_file, fix_mask_subpath,
            global_mov_mask_file, mov_mask_subpath ->

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def global_registration_working_dir = file("${reg_outdir}/${global_results_subdir}/${reg_meta.id}")
        def global_registration_output = file("${reg_outdir}")

        def global_fix_channel = params.fix_global_channel ?: params.reg_ch // default to reg_ch
        def global_mov_channel = params.mov_global_channel ?: global_fix_channel // default to fix_channel

        // get the corresponding output channel from the warped to output mapping if there is one
        def global_output_channel = reg_meta.warped_channels_mapping[global_mov_channel]

        log.debug "Prepare global registration inputs: global_fix_channel=${global_fix_channel}, global_mov_channel=${global_mov_channel}, global_output_channel=${global_output_channel}, fix_mask=${global_fix_mask_file}, mov_mask=${global_mov_mask_file}"

        def ri =  [
            reg_meta,

            fix, // global_fixed
            "${fix_meta.stitched_dataset}/${fix_global_subpath}", // global_fixed_subpath
            params.fix_global_timeindex, global_fix_channel,

            mov, // global_moving
            "${mov_meta.stitched_dataset}/${mov_global_subpath}", // global_moving_subpath
            params.mov_global_timeindex, global_mov_channel,

            global_fix_mask_file, fix_mask_subpath,
            global_mov_mask_file, mov_mask_subpath,

            params.global_steps,
            global_registration_working_dir, // global_transform_output
            params.global_transform_name,
            params.global_inv_transform_name,
            global_registration_output, // global_align_output
            params.global_registration_container, // global_aligned_name
            '',    // global_alignment_subpath (defaults to mov_global_subpath)
            params.global_registration_timeindex, global_output_channel,
        ]
        log.debug "Global registration inputs: $ri"
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
        | map { it ->
            def (reg_meta,
                fix, fix_subpath, fix_timeindex, fix_channel,
                mov, mov_subpath, mov_timeindex, mov_channel,
                fix_mask, fix_mask_subpath,
                mov_mask, mov_mask_subpath,
                steps,
                transform_dir, transform_name, inv_transform_name,
                align_dir, align_name, align_subpath) = it
            def r = [
                reg_meta,
                fix, fix_subpath,
                mov, mov_subpath,
                transform_dir, transform_name, inv_transform_name,
                align_dir, align_name, align_subpath,
            ]
            log.debug "Skip global alignment: $r"
            r
        }
    }
    // Prepare global transform output
    global_transforms = global_registration_results
    | map { it ->
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            transform_dir, transform_name, inv_transform_name,
            align_dir, align_name, align_subpath) = it
        log.debug "Completed global alignment: $it"
        def full_transform_path = transform_dir && transform_name
            ? "${transform_dir}/${transform_name}"
            : ''
        def full_inv_transform_path = transform_dir && inv_transform_name
            ? "${transform_dir}/${inv_transform_name}"
            : ''
        def r = [
            reg_meta, full_transform_path, full_inv_transform_path,
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
        def (reg_meta,
            fix, fix_subpath,
            mov, mov_subpath,
            transform_dir, transform_name,
            align_dir, align_name, align_subpath) = it
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
        params.bigstream_dask_workers,
        params.bigstream_dask_min_workers,
        params.bigstream_dask_worker_cpus,
        params.bigstream_dask_worker_mem_gb ?: params.default_mem_gb_per_cpu * params.bigstream_dask_worker_cpus,
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
        def (fix_id, dask_meta, dask_context, reg_meta,
            global_fix, global_fix_subpath,
            global_mov, global_mov_subpath,
            global_transform_dir, global_transform_name,
            global_align_dir, global_align_name, global_align_subpath) = it
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
    registration_inputs              // ch: [ reg_meta, fix_meta, mov_meta]
    global_transforms                // ch: [ reg_meta, global_transform, global_inv_transform ]
    local_registrations_dask_cluster // ch: [ reg_meta, dask_meta, dask_context ]
    resolved_masks                   // ch: [ reg_meta_id, fix_mask, fix_mask_subpath, mov_mask, mov_mask_subpath ]
    bigstream_config                 // string|file bigstream yaml config
    reg_outdir
    skip_local_registration

    main:
    def fix_local_subpath = params.fix_local_subpath
        ? params.fix_local_subpath
        : "${params.fix_local_channel ?: params.reg_ch}/${params.local_scale}"
    def mov_local_subpath = params.mov_local_subpath
        ? params.mov_local_subpath
        : "${params.mov_local_channel ?: params.reg_ch}/${params.local_scale}"
    def local_results_subdir = params.local_results_subdir ?: 'local'
    def local_registration_inputs = registration_inputs
    | join(global_transforms, by: 0)
    | map { reg_meta, fix_meta, mov_meta, global_transform, _global_inv_transform ->
        [reg_meta.id, reg_meta, fix_meta, mov_meta, global_transform]
    }
    | join(resolved_masks, by: 0)
    | map { _reg_id, reg_meta, fix_meta, mov_meta, global_transform,
            local_fix_mask_file, fix_mask_subpath,
            local_mov_mask_file, mov_mask_subpath ->

        def fix = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        def mov = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"

        def local_registration_working_dir = file("${reg_outdir}/${local_results_subdir}/${reg_meta.id}")
        def local_registration_output = file("${reg_outdir}")

        def ri =  [
            reg_meta,

            fix, // local_fixed
            "${fix_meta.stitched_dataset}/${fix_local_subpath}", // local_fixed_subpath
            mov, // local_moving
            "${mov_meta.stitched_dataset}/${mov_local_subpath}", // local_moving_subpath

            local_fix_mask_file, fix_mask_subpath,
            local_mov_mask_file, mov_mask_subpath,

            global_transform,

            params.local_steps,
            local_registration_working_dir,   // local_transform_output
            params.local_transform_name, '',  // local_deform_name
            '', '',                           // local_inv_deform_name - no inverse - we compute this separately
            local_registration_output,        // local_align_output
            '',                               // local_aligned_name - do not apply the deform transform
            '',                               // local_alignment_subpath (defaults to mov_global_subpath)
        ]
        log.debug "Prepare local registration inputs: $ri"
        ri
    }
    | join(local_registrations_dask_cluster, by:0)
    | multiMap { it ->
        def (reg_meta,
            local_fix, local_fix_subpath,
            local_mov, local_mov_subpath,
            local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,
            global_transform,
            local_steps, local_registration_working_dir,
            local_transform_name, local_transform_subpath,
            local_inv_transform_name, local_inv_transform_subpath,
            local_registration_output, local_align_name, local_align_subpath,
            dask_meta, dask_context) = it

        def local_fix_channel = params.fix_local_channel ?: params.reg_ch
        def local_mov_channel = params.mov_local_channel ?: local_fix_channel
        // get the corresponding output channel from the warped to output mapping if there is one
        def local_output_channel = reg_meta.warped_channels_mapping[local_mov_channel]

        def data = [
            reg_meta,

            local_fix, local_fix_subpath,
            params.fix_local_timeindex, local_fix_channel,

            local_mov, local_mov_subpath,
            params.mov_local_timeindex, local_mov_channel,

	        local_fix_mask, local_fix_mask_subpath,
            local_mov_mask, local_mov_mask_subpath,

            global_transform,

            local_steps,
            local_registration_working_dir, // local_transform_output
            local_transform_name, local_transform_subpath,
            local_inv_transform_name, local_inv_transform_subpath,

            local_registration_output, // local_align_output
            local_align_name, local_align_subpath,
            params.local_registration_timeindex, local_output_channel,
        ]
        def cluster = [
            dask_context.scheduler_address,
            dask_context.config,
        ]
        log.debug "Local registration inputs: $it -> $data, $cluster"
        log.debug "Local registration cluster: $cluster"
        log.debug "Local registration data: $data"

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

        local_registration_results.view { it ->
            // [
            //    meta, fix, fix_subpath,
            //    mov, mov_subpath,
            //    affine_transform,
            //    local_deform_dir,
            //    local_deform, local_deform_subpath,
            //    local_inv_deform, local_inv_deform_subpath
            //    warped_output, warped_name_only, warped_subpath
            //  ]
            log.debug "Completed local alignment: $it"
        }
    } else {
        local_registration_results = local_registration_inputs.data
        | map { it ->
            def (reg_meta,
                local_fix, local_fix_subpath, _local_fix_timeindex, _local_fix_channel,
                local_mov, local_mov_subpath, _local_mov_timeindex, _local_mov_channel,
                _local_fix_mask, _local_fix_mask_subpath,
                _local_mov_mask, _local_mov_mask_subpath,
                global_transform,
                _local_steps, local_registration_working_dir,
                local_transform_name, local_transform_subpath,
                local_inv_transform_name, local_inv_transform_subpath,
                local_registration_output, local_align_name, local_align_subpath) = it
            def r = [
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
            log.debug "Skip local alignment: $r"
            r
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
    | map { it ->
        def (reg_meta,
            _fix_meta, _mov_meta,
            _fix, _fix_subpath,
            _mov, _mov_subpath,
            _affine_transform,
            local_transform_output,
            local_transform, local_transform_subpath,
            _local_inv_transform, _local_inv_transform_subpath,
            _warped_output,
            _local_warped_name, _local_warped_subpath,
            _dask_meta, dask_context) = it
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
        inverse_results.view { it ->
            log.debug "Completed inverse -> $it"
        }
    } else {
        inverse_results = compute_inv_inputs.map { it[0] }
        inverse_results.view { it ->
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
    | flatMap { it ->
        def (reg_meta, fix_meta, mov_meta,
            fix, _fix_subpath,
            mov, _mov_subpath,
            affine_transform,
            local_transform_output,
            local_deform, local_deform_subpath,
            _local_inv_deform, _local_inv_deform_subpath,
            warped_output, local_warped_name, local_warped_subpath,
            _dask_meta, dask_context) = it
        log.debug "Prepare deformation inputs: $it"
        def warped_name = local_warped_name ?: params.local_registration_container
        def warped_subpaths = ParamUtils.get_warped_subpaths(params)
        def warped_and_output_channels = ParamUtils.get_warped_and_output_channels(params, reg_meta.warped_channels_mapping)

        def r = [ warped_subpaths, warped_and_output_channels ]
                .combinations()
                .collect {
                    def (subpaths, channel_mapping) = it
                    def (fixed_subpath, warped_subpath) = subpaths
                    [ fixed_subpath, warped_subpath, channel_mapping ]
                }
	            .findAll { _fix_warped_subpath, warped_subpath, _channel_mapping -> warped_subpath != local_warped_subpath }
                .collect { fix_warped_subpath, warped_subpath, channel_mapping ->
                    def (warped_channel, output_channel) = channel_mapping
                    def deformation_input = [
                        reg_meta,
                        fix, "${fix_meta.stitched_dataset}/${fix_warped_subpath}",
                        params.fix_local_timeindex, params.fix_local_channel,
                        /* fix_spacing */'',

                        mov, "${mov_meta.stitched_dataset}/${warped_subpath}",
                        params.mov_local_timeindex, warped_channel,
                        /* mov_spacing */'',

                        affine_transform,

                        "${local_transform_output}/${local_deform}", local_deform_subpath,

                        "${warped_output}/${warped_name}", "${mov_meta.stitched_dataset}/${warped_subpath}",
                        params.local_registration_timeindex, output_channel
                    ]
                    def r = [
                        deformation_input,
                        dask_context,
                    ]
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

        deformation_results.view { it ->
            log.debug "Completed deformation: $it"
        }
    } else {
        deformation_results = deformation_inputs
        | map { it ->
            def (reg_meta,
                fix, fix_subpath, _fix_timeindex, _fix_channel, _fix_spacing,
                mov, mov_subpath, _mov_timeindex, _mov_channel, _mov_spacing,
                _affine_transform,
                _deform_transform, _deform_transform_subpath,
                warped, warped_subpath) = it[0]
            def r = [
                reg_meta,
                fix, fix_subpath,
                mov, mov_subpath,
                warped, warped_subpath,
            ]
            log.debug "Skip deformation: $r"
            r
        }
    }

    emit:
    deformation_results
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FOREGROUND MASK RESOLUTION
    Generates one fix mask and one mov mask (each used for both global and local alignment).
    Logic per slot:
      - generate_fix/mov_mask == true  → run BIGSTREAM_FOREGROUNDMASK, ignore *_mask params
      - generate_fix/mov_mask == false → use file from global_<any>/local_<any> mask params as-is
    Emits two named channels (global_masks_per_pair / local_masks_per_pair) carrying
    [reg_meta_id, fix_mask, fix_mask_subpath, mov_mask, mov_mask_subpath] so that existing
    global_*_mask_subpath / local_*_mask_subpath params continue to be respected when
    masks are supplied manually.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow RESOLVE_MASKS {

    take:
    registration_inputs  // [reg_meta, fix_meta, mov_meta]
    outdir

    main:
    // Derive image subpath for mask generation from the global alignment params
    // (global scale is typically a lower-res scale and faster to process)

    def fix_global_ch     = params.fix_global_channel ?: params.reg_ch
    def mov_global_ch     = params.mov_global_channel ?: fix_global_ch
    def fix_global_sp_val = params.fix_mask_subpath ?: params.fix_global_subpath ?: "${fix_global_ch}/${params.global_scale}"
    def mov_global_sp_val = params.mov_mask_subpath ?: params.mov_global_subpath ?: "${mov_global_ch}/${params.global_scale}"

    // When we generate the mask we generate one mask for the fixed image and one mask for the moving image
    // at the same scale we use for global registration
    // and these masks will be used both for local and global registration

    // ── Fix mask ──────────────────────────────────────────────────────────────
    // [fix_id, reg_id, image, image_subpath, timeindex, channel]
    def fix_image_info = registration_inputs
    | map { reg_meta, fix_meta, _mov_meta ->
        def image = "${fix_meta.stitching_result_dir}/${fix_meta.stitching_container}"
        [
            fix_meta.id,
            reg_meta.id,
            image,
            "${fix_meta.stitched_dataset}/${fix_global_sp_val}",
            params.fix_global_timeindex, fix_global_ch,
        ]
    }
    | unique { it[0] }

    // fix_mask_result: [fix_id, fix_mask, fix_sp]
    def fix_mask_result
    if (params.generate_fix_mask) {
        fix_image_info.view { it -> log.debug "Fix image for generating mask: $it" }
        def mask_out = file("${outdir}/${params.fix_mask}")
        fix_mask_result = BIGSTREAM_FOREGROUNDMASK_FIX(
            fix_image_info.map { fix_id, _reg_id, img, sp, ti, ch ->
                def mask_sp = sp.startsWith(fix_id) ? sp : fix_id + '/' + sp.trim('/')
                [[id: fix_id], file(img), sp, ti, ch, mask_out, mask_sp]
            }
        )
        | map { meta, _img, _sp, full_mask, mask_sp -> [meta.id, file(full_mask), mask_sp] }
    } else {
        def fix_mask_file
        if (!params.fix_mask) {
            fix_mask_file = []
        } else if (params.fix_mask.startsWith('/')) {
            // this is an absolute path
            fix_mask_file = file(params.fix_mask)
        } else {
            // use a path relative to outdir
            fix_mask_file = file("${outdir}/${params.fix_mask}")
        }

        fix_mask_result = fix_image_info
        | map { fix_id, _reg_id, _img, _sp, _ti, _ch ->
            def mask_sp = params.fix_mask_subpath
                            ? (params.fix_mask_subpath.startsWith(fix_id)
                                ? params.fix_mask_subpath
                                : "${fix_id}/${params.fix_mask_subpath}")
                            : ''
            [
                fix_id,
                fix_mask_file, mask_sp,
            ]
        }
    }

    // ── Mov mask ──────────────────────────────────────────────────────────────
    // [mov_id, reg_id, image, image_subpath, timeindex, channel, mask_output_path]
    def mov_image_info = registration_inputs
    | map { reg_meta, _fix_meta, mov_meta ->
        def image = "${mov_meta.stitching_result_dir}/${mov_meta.stitching_container}"
        [
            mov_meta.id,
            reg_meta.id,
            image,
            "${mov_meta.stitched_dataset}/${mov_global_sp_val}",
            params.mov_global_timeindex, mov_global_ch
        ]
    }
    | unique { it[0] }

    // mov_mask_result: [mov_id, mov_mask, mov_sp]
    def mov_mask_result
    if (params.generate_mov_mask) {
        mov_image_info.view { it -> log.debug "Mov image for generating mask: $it" }
        def mask_out = file("${outdir}/${params.mov_mask}")
        mov_mask_result = BIGSTREAM_FOREGROUNDMASK_MOV(
            mov_image_info.map { mov_id, _reg_id, img, sp, ti, ch ->
                def mask_sp = sp.startsWith(mov_id) ? sp : mov_id + '/' + sp.trim('/')
                [[id: mov_id], file(img), sp, ti, ch, mask_out, mask_sp]
            }
        )
        | map { meta, _img, _sp, full_mask, mask_sp -> [meta.id, file(full_mask), mask_sp] }
    } else {
        def mov_mask_file
        if (!params.mov_mask) {
            mov_mask_file = []
        } else if (params.mov_mask.startsWith('/')) {
            // this is an absolute path
            mov_mask_file = file(params.mov_mask)
        } else {
            // use a path relative to outdir
            mov_mask_file = file("${outdir}/${params.mov_mask}")
        }
        mov_mask_result = mov_image_info
        | map { mov_id, _reg_id, _img, _sp, _ti, _ch ->
            def mask_sp = params.mov_mask_subpath
                            ? (params.mov_mask_subpath.startsWith(mov_id)
                                ? params.mov_mask_subpath
                                : "${mov_id}/${params.mov_mask_subpath}")
                            : ''
            [
                mov_id,
                mov_mask_file, mask_sp,
            ]
        }
    }

    // ── Join all masks back to registration pairs ────────────────────────────
    // combined: [reg_id,
    //            fix_mask, fix_sp,
    //            mov_mask, mov_sp]
    def pair_keys = registration_inputs
    | map { reg_meta, fix_meta, mov_meta -> [fix_meta.id, mov_meta.id, reg_meta.id] }

    def with_fix = pair_keys
    | join(fix_mask_result, by: 0)
    | map { _fix_id, mov_id, reg_id, fix_m, fix_m_sp ->
        [mov_id, reg_id, fix_m, fix_m_sp]
    }

    def combined = with_fix
    | join(mov_mask_result, by: 0)
    | map { _mov_id, reg_id,
            fix_m, fix_m_sp,
            mov_m, mov_m_sp ->
        [reg_id, fix_m, fix_m_sp, mov_m, mov_m_sp]
    }

    emit:
    masks = combined  // [reg_meta_id, fix_mask, fix_mask_subpath, mov_mask, mov_mask_subpath]
}

workflow RUN_GLOBAL_ALIGNMENT_CORRELATION {
    take:
    registration_inputs         // ch: [ reg_meta, fix_meta, mov_meta ]
    global_registration_results // ch: [ reg_meta, fix, fix_sp, mov, mov_sp, transform_dir, transform_name, inv_transform_name, align_dir, align_name, align_subpath ]
    reg_outdir                  // file: registration output directory
    run_global_metric           // boolean: if true compute the global correlation metric

    main:
    if (run_global_metric) {
        def global_metric_ch = registration_inputs
        | join(global_registration_results, by: 0)
        | map { reg_meta, fix_meta, mov_meta,
                fix, fix_sp, _mov, mov_sp,
                _transform_dir, _transform_name, _inv_transform_name,
                align_dir, align_name, align_subpath ->
            def global_results_subdir = params.global_results_subdir ?: 'global'
            def metrics_container = params.global_correlation_container
            def mov_container = "${align_dir}/${align_name ?: params.global_registration_container}"
            def global_fix_channel = params.fix_global_channel ?: params.reg_ch
            def global_mov_channel = params.mov_global_channel ?: global_fix_channel
            def global_align_subpath = align_subpath ?: mov_sp
            // get the corresponding output channel from the warped to output mapping if there is one
            def global_aligned_channel = reg_meta.warped_channels_mapping[global_mov_channel] ?: global_mov_channel
            def r = [
                reg_meta,
                file(fix), fix_sp,
                params.fix_global_timeindex, global_fix_channel,
                '', // fix_spacing
                file(mov_container), global_align_subpath,
                params.global_registration_timeindex, global_aligned_channel,
                '', // alignment_spacing
                file("${reg_outdir}/${global_results_subdir}/${metrics_container}"), "${reg_meta.id}/${params.global_correlation_output_subpath}",
            ]
            log.debug "Global metric inputs: $r"
            r
        }
        BIGSTREAM_CORRELATIONMETRIC(global_metric_ch)
    }
}

workflow RUN_LOCAL_ALIGNMENT_CORRELATION {
    take:
    registration_inputs       // ch: [ reg_meta, fix_meta, mov_meta ]
    local_deformation_results // ch: [ reg_meta, fix, fix_subpath, mov, mov_subpath, warped, warped_subpath ]
    reg_outdir                // file: registration output directory
    run_local_metric          // boolean: if true compute the local correlation metric

    main:
    if (run_local_metric) {
        def fix_local_subpath = params.fix_local_subpath
            ?: "${params.fix_local_channel ?: params.reg_ch}/${params.local_scale}"
        def mov_local_subpath = params.mov_local_subpath
            ?: "${params.mov_local_channel ?: params.reg_ch}/${params.local_scale}"
        def local_metric_ch = registration_inputs
        | join(local_deformation_results, by: 0)
        | map { reg_meta, fix_meta, mov_meta,
                 fix, fix_sp, mov, mov_sp,
                 warped, warped_sp ->
            def local_results_subdir = params.local_results_subdir ?: 'local'
            def metrics_container = params.local_correlation_container
            def local_fix_channel = params.fix_local_channel ?: params.reg_ch
            def local_mov_channel = params.mov_local_channel ?: local_fix_channel
            def local_align_subpath = params.local_correlation_input_subpath ?: "${mov_meta.stitched_dataset}/${mov_local_subpath}"
            def local_aligned_channel = reg_meta.warped_channels_mapping[local_mov_channel] ?: local_mov_channel
            def r = [
                reg_meta,
                file(fix), "${fix_meta.stitched_dataset}/${fix_local_subpath}",
                params.fix_local_timeindex, local_fix_channel,
                '', // fix_spacing
                file(warped), local_align_subpath,
                params.local_registration_timeindex, local_aligned_channel,
                '', // alignment_spacing
                file("${reg_outdir}/${local_results_subdir}/${metrics_container}"), "${reg_meta.id}/${params.local_correlation_output_subpath}",
            ]
            log.debug "Local metric inputs: $r"
            r
        }
        BIGSTREAM_CORRELATIONMETRIC(local_metric_ch)
    }
}
