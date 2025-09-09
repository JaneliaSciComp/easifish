/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT EXTRACTION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { POST_RS_FISH           } from '../modules/local/post_rs_fish'

include { FISHSPOT_EXTRACTION    } from '../subworkflows/local/fishspot_extraction'
include { RSFISH_SPOT_EXTRACTION } from '../subworkflows/local/rsfish_spot_extraction'

include { as_list                } from './util_functions'

workflow SPOT_EXTRACTION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outputdir       // file|string - output directory
    workdir         // file|string - work directory

    main:
    def spot_volume_ids = as_list(params.spot_extraction_ids)

    def spots_inputs = ch_meta
    | filter { meta ->
        spot_volume_ids.empty || meta.id in spot_volume_ids
    }
    | flatMap { meta ->
        // Spot extraction is typically done for all cell (no DAPI) channels from all rounds
        def input_img_dir = get_spot_extraction_input_volume(meta)
        def spots_output_dir = file("${outputdir}/${params.spot_extraction_subdir}/${meta.id}")

        get_spot_subpaths(meta).collect { input_spot_subpath, spots_result_name ->
            def r = [
                meta,
                input_img_dir,
                input_spot_subpath,
                spots_output_dir,
                spots_result_name,
            ]
            log.debug "Spot extraction input: $r"
            r
        }
    }

    def spots_results
    if (params.skip_spot_extraction) {
        log.debug "Skipping spot extraction"
        spots_results = spots_inputs
        | map {
            def (meta, input_img_dir, input_spot_subpath, spots_output_dir, spots_result_name) = it
            def r = [
                meta,
                input_img_dir,
                input_spot_subpath,
                "${spots_output_dir}/${spots_result_name}",
            ]
            log.debug "Skipped spot extraction, but verify any spot files at: $r"
            r
        }
    } else if (params.use_fishspots) {
        log.debug "Extract spots using Fishspot"
        spots_results = FISHSPOT_EXTRACTION(
            spots_inputs,
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
            spots_inputs,
            params.distributed_spot_extraction,
            workdir,
            params.rsfish_spark_workers,
            params.rsfish_min_spark_workers,
            params.rsfish_spark_worker_cores,
            params.rsfish_spark_gb_per_core,
            params.rsfish_spark_driver_cores,
            params.rsfish_spark_driver_mem_gb,
        )
    }

    POST_RS_FISH(spots_results)

    def final_spot_results = expand_spot_results(POST_RS_FISH.out.results)

    emit:
    done = final_spot_results
}

def get_spot_extraction_input_volume(meta) {
    return "${meta.stitching_result_dir}/${meta.stitching_container}"
}

def get_spot_subpaths(meta) {
    if (!params.spot_subpaths && !params.spot_channels && !params.spot_scales) {
        return [
            ['', ''],  // empty subpath, empty resultnane - the input image container contains the array dataset
        ]
    } else if (params.spot_subpaths) {
        // in this case the subpaths parameters must match exactly the container datasets
        return as_list(params.spot_subpaths)
            .collect { subpath ->
                def spots_result_name = "spots-rsfish-${subpath.replace('/', '-')}.csv"
                [ "${meta.stitched_dataset}/${subpath}", spots_result_name ]
            }
    } else {
        def spot_channels;
        if (params.spot_channels) {
            spot_channels = as_list(params.spot_channels)
            log.debug "Use specified spot channels: $spot_channels"
        } else {
            // all but the last channel which typically is DAPI
            def all_channels = as_list(params.channels)
            if (params.dapi_channel) {
                spot_channels = all_channels.findAll { it != params.dapi_channel }
            } else {
                // automatically consider DAPI the last channel
                // this may throw an exception if the channel list is empty or a singleton
                spot_channels = all_channels[0..-2] // all but the last channel
            }
            log.debug "Spot channels: $spot_channels (all from ${params.channels} except the last one)"
        }
        def spot_scales = as_list(params.spot_scales)

        return [spot_channels, spot_scales].combinations()
            .collect { ch, scale ->
                // when channel and scale is used we also prepend the stitched dataset
                def dataset = "${ch}/${scale}"
                def r = [
                    "${meta.stitched_dataset}/${dataset}",
                    "spots-rsfish-${ch}.csv"
                ]
                log.debug "Spot dataset: $r"
                r
        }
    }
}

def expand_spot_results(results) {
    results.flatMap {
        def (meta, input_img_dir, input_spot_subpath, spots_results) = it
        spots_results.split(/[ \n]+/)
        .collect { spots_file ->
            [
                meta, input_img_dir, input_spot_subpath, spots_file,
            ]
        }
    }
}
