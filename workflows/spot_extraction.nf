/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN SPOT EXTRACTION
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { RS_FISH     } from '../modules/janelia/rs_fish/main'
include { SPARK_START } from '../subworkflows/janelia/spark_start/main'
include { SPARK_STOP  } from '../subworkflows/janelia/spark_stop/main'
include { as_list     } from './util_functions'

workflow SPOT_EXTRACTION {
    take:
    ch_meta         // channel: [ meta ] - metadata containing stitching results
    outputdir       // file|string - output directory
    workdir         // file|string - work directory

    main:
    def spot_volume_ids = as_list(params.spot_extraction_ids)

    def spots_spark_input = ch_meta
    | filter { meta ->
        spot_volume_ids.empty || meta.id in spot_volume_ids
    }
    | map { meta ->
        def input_img_dir = get_spot_extraction_input_volume(meta)
        def spots_output_dir = file("${outputdir}/${params.spot_extraction_subdir}/${meta.id}")
        [
            meta,
            [ input_img_dir, spots_output_dir ],
        ]
    }

    def final_rsfish_results
    if (params.skip_spot_extraction) {
        log.info "Skipping spot extraction"

        final_rsfish_results = spots_spark_input
        | map {
            def (meta, spots_inout_dirs) = it
            def (input_img_dir, spots_output_dir) = spots_inout_dirs
            get_spot_subpaths(meta).collect { input_spot_subpath ->
                [
                    meta,
                    input_img_dir,
                    input_spot_subpath,
                    "${spots_output_dir}/${meta.id}-points.csv",
                ]
            }
        }
    } else {
        spots_spark_input.subscribe { log.debug "Spot extraction spark input: $it" }

        def rsfish_input = SPARK_START(
            spots_spark_input,
            params.distributed_spot_extraction,
            workdir,
            params.rsfish_spark_workers,
            params.rsfish_min_spark_workers,
            params.rsfish_spark_worker_cores,
            params.rsfish_spark_gb_per_core,
            params.rsfish_spark_driver_cores,
            params.rsfish_spark_driver_mem_gb,
        ) // ch: [ meta, spark ]
        | join(ch_meta, by: 0) // join to add the files
        | flatMap {
            def (meta, rsfish_spark) = it
            def input_img_dir = get_spot_extraction_input_volume(meta)
            def spots_output_dir = file("${outputdir}/${params.spot_extraction_subdir}/${meta.id}")

            get_spot_subpaths(meta).collect { input_spot_subpath ->
                [
                    meta,
                    input_img_dir,
                    input_spot_subpath,
                    spots_output_dir,
                    rsfish_spark,
                ]
            }
        }

        rsfish_input.subscribe { log.debug "RS_FISH input: $it" }

        RS_FISH(rsfish_input)

        def rsfish_results = RS_FISH.out.params
        | combine(RS_FISH.out.csv)
        | map {
            def (meta, input_image, input_dataset, spots_output_dir, spark, full_output_filename) = it
            [
                meta,
                input_image,
                input_dataset,
                full_output_filename,
                spark,
            ]
        }
        rsfish_results.subscribe { log.debug "RS_FISH results: $it" }

        final_rsfish_results = rsfish_results.map { it[0 .. -1] }

        def prepare_spark_stop = rsfish_results
        | groupTuple(by: [0, 4]) // group by meta and spark
        | map {
            def (meta, input_image, input_dataset, output_filename, spark) = it
            [
                meta, spark,
            ]
        }

        SPARK_STOP(
            prepare_spark_stop,
            params.distributed_spot_extraction,
        )

    }

    emit:
    done = final_rsfish_results
}

def get_spot_extraction_input_volume(meta) {
    return "${meta.stitching_result_dir}/${meta.stitching_container}"
}

def get_spot_subpaths(meta) {
    def input_img_dir = get_spot_extraction_input_volume(meta)

    if (!params.spot_subpaths && !params.spot_channels && !params.spot_scales) {
        return [ '' ] // empty subpath - the input image container contains the array data
    } else if (params.spot_subpaths) {
        // in this case the subpaths parameters must match exactly the container datasets
        return as_list(params.spot_subpaths)
    } else {
        def spot_channels = as_list(params.spot_channels)
        def spot_scales = as_list(params.spot_scales)

        return [spot_channels, spot_scales].combinations()
            .collect {
                // when channel and scale is used we also prepend the stitched dataset
                def dataset = it.join('/')
                "${meta.stitched_dataset}/${dataset}"
        }
    }
}