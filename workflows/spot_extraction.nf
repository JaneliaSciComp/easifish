/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT EXTRACTION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { POST_RS_FISH                   } from '../modules/local/post_rs_fish'
include { POST_RS_FISH as VERIFY_RS_FISH } from '../modules/local/post_rs_fish'

include { FISHSPOT_EXTRACTION            } from '../subworkflows/local/fishspot_extraction'
include { RSFISH_SPOT_EXTRACTION         } from '../subworkflows/local/rsfish_spot_extraction'

workflow SPOT_EXTRACTION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outputdir       // file|string - output directory
    workdir         // file|string - work directory

    main:
    def spot_volume_ids = ParamUtils.as_list(params.spot_extraction_ids)

    def spots_inputs = ch_meta
    | filter { meta ->
        spot_volume_ids.empty || meta.id in spot_volume_ids
    }
    | flatMap { meta ->
        // Spot extraction is typically done for all cell (no DAPI) channels from all rounds
        def (input_img_dir, spots_output_dir) = get_spot_extraction_input_output(meta, outputdir)
        get_spots_subpaths(meta).collect { input_spot_subpath, spots_result_name, spots_image_subpath_ref, spots_channels ->
            def r = [
                meta,
                input_img_dir,
                input_spot_subpath,
                spots_output_dir,
                spots_result_name,
                spots_image_subpath_ref,
                spots_channels,
            ]
            log.debug "Spot extraction input: $r"
            r
        }
    }

    def skipped_spot_extraction_rounds_list = ParamUtils.as_list(params.skipped_spot_extraction_rounds)

    def spots_to_skip_ch
    def spots_to_process_ch
    if (params.skip_spot_extraction) {
        log.debug "Skipping spot extraction for all rounds"
        spots_to_skip_ch = spots_inputs
        spots_to_process_ch = channel.empty()
    } else if (!skipped_spot_extraction_rounds_list.empty) {
        log.debug "Skipping spot extraction for rounds: ${skipped_spot_extraction_rounds_list}"
        def branched = spots_inputs.branch { it ->
            skipped: it[0].id in skipped_spot_extraction_rounds_list
            active: true
        }
        spots_to_skip_ch = branched.skipped
        spots_to_process_ch = branched.active
    } else {
        spots_to_skip_ch = channel.empty()
        spots_to_process_ch = spots_inputs
    }

    def spots_verify_inputs = spots_to_skip_ch
    | map { it ->
        def (meta, input_img_dir, input_spot_subpath, spots_output_dir, spots_result_name, spots_image_subpath_ref, _spots_channels) = it
        def r = [
            meta,
            input_img_dir,
            input_spot_subpath,
            "${spots_output_dir}/${spots_result_name}",
            spots_image_subpath_ref,
        ]
        log.debug "Skipped spot extraction for round ${meta.id}, but verify any spot files at: $r"
        r
    }

    def spots_results
    if (params.spots_extraction_method == 'FISHSPOT') {
        log.debug "Extract spots using Fishspot"
        spots_results = FISHSPOT_EXTRACTION(
            spots_to_process_ch,
            params.distributed_spot_extraction,
            params.fishspots_config,
            params.fishspots_dask_config,
            workdir,
            params.fishspots_dask_workers,
            params.fishspots_dask_min_workers,
            params.fishspots_dask_worker_cpus,
            params.fishspots_dask_worker_mem_gb,
            params.fishspots_cpus ,
            params.fishspots_mem_gb,
        )
    } else {
        log.debug "Extract spots using RS_FISH"
        spots_results = RSFISH_SPOT_EXTRACTION(
            spots_to_process_ch,
            params.distributed_spot_extraction,
            workdir,
            params.rsfish_spark_local_dir,
            params.rsfish_spark_workers,
            params.rsfish_min_spark_workers,
            params.rsfish_spark_worker_cores,
            params.rsfish_spark_worker_mem_gb,
            params.rsfish_spark_executor_cores,
            params.rsfish_spark_executor_mem_gb,
            params.rsfish_spark_executor_overhead_mem_gb,
            params.rsfish_spark_task_cores,
            params.rsfish_spark_driver_cores,
            params.rsfish_spark_driver_mem_gb,
            params.rsfish_spark_gb_per_core,
            create_rsfish_spark_config(), // spark config
        )
    }

    def post_spots_processing_input = spots_results
    | join(spots_to_process_ch, by: 0)
    | map { it ->
        def (meta,
             input_img_dir, input_spot_subpath,
             full_spots_result_file,
             _img_dir, _img_subpath,
             _spots_output_dir, _spots_result_name,
             spots_image_subpath_ref) = it
        def r = [
            meta,
            input_img_dir, input_spot_subpath,
            full_spots_result_file,
            spots_image_subpath_ref,
        ]
        log.debug "Prepare post spot processing $it -> $r"
        r
    }

    spots_verify_inputs.view { it -> log.debug "Verify spots input: $it" }
    VERIFY_RS_FISH(spots_verify_inputs)
    def verify_results = VERIFY_RS_FISH.out.results
    verify_results.view { it -> log.debug "Verify spots result: $it" }

    post_spots_processing_input.view { it -> log.debug "Spot post-processing input: $it" }
    POST_RS_FISH(post_spots_processing_input)
    def processed_results = POST_RS_FISH.out.results
    processed_results.view { it -> log.debug "Spot post-processing result: $it" }

    def post_results = verify_results.mix(processed_results)

    def final_spot_results = expand_spot_results(post_results)

    emit:
    done = final_spot_results
}

def get_spot_extraction_input_output(meta, outputdir) {
    if (!params.extract_spots_from_warped || meta.id == params.registration_fix_id) {
        // extract the spots from the stitched image
        return [
            "${meta.stitching_result_dir}/${meta.stitching_container}",
            file("${outputdir}/${params.spots_extraction_subdir}/${meta.id}"),
        ]
    } else {
        // extract the spots from the aligned (moving) image
        return [
            file("${outputdir}/${params.registration_subdir}/${params.local_registration_container}"),
            file("${outputdir}/${params.warped_spots_subdir}/${meta.id}"),
        ]
    }
}

def create_rsfish_spark_config() {
    def spark_config = [:]
    if (params.rsfish_spark_max_partition_bytes) {
        spark_config['spark.sql.files.maxPartitionBytes'] = params.rsfish_spark_max_partition_bytes
    }
    return spark_config
}

def get_spots_subpaths(meta) {
    def spots_channels = SpotsUtils.get_spots_channels(meta, params)
    def spots_subpath = params.spots_subpath ?: ''
    def spots_scale = params.spots_scale ?: ''
    if (spots_subpath) {
        def spots_result_name = "spots-rsfish-${spots_subpath.replace('/', '-')}.csv"
        return [
            [
                "${meta.id}/${spots_subpath}",
                spots_result_name,
                params.spots_image_subpath_ref ? "${meta.id}/${params.spots_image_subpath_ref}" : '',
                spots_channels.join(','),
            ]
        ]
    }
    if (!spots_channels.empty) {
        return spots_channels.collect { ch ->
            def dataset = spots_scale ? "${ch}/${spots_scale}" : "${ch}"
            def r = [
                "${meta.id}/${dataset}",
                "spots-rsfish-${ch}.csv",
                params.spots_image_subpath_ref ? "${meta.id}/${params.spots_image_subpath_ref}" : '',
                "${ch}"
            ]
            log.debug "Spot dataset: $r"
            r
        }
    } else {
        return [
            // empty result
        ]
    }
}

def expand_spot_results(results) {
    results.flatMap { it ->
        def (meta, input_img_dir, input_spot_subpath, spots_results) = it
        log.debug "Expand spot results ${spots_results} (coming from: $it)"
        spots_results.split(/[ \n]+/)
        .collect { spots_file ->
            [
                meta, input_img_dir, input_spot_subpath, spots_file,
            ]
        }
    }
}
