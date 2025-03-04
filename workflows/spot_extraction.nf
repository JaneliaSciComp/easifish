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
    workdir         // file|string - work directory

    main:
    def spot_volume_ids = as_list(params.spot_extraction_ids)

    def spots_spark_input = ch_meta
    | filter { meta ->
        spot_volume_ids.empty || meta.id in spot_volume_ids
    }
    | map { meta ->
        def input_img_dir = get_spot_extraction_input_volume(meta)
        [
            meta,
            [ input_img_dir ],
        ]
    }

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
        def spot_subpaths

        def input_img_dir = get_spot_extraction_input_volume(meta)

        if (!params.spot_subpaths && !params.spot_channels && !params.spot_scales) {
            spot_subpaths = [ '' ] // empty subpath - the input image container contains the array data
        } else if (params.spot_subpaths) {
            // in this case the subpaths parameters must match exactly the container datasets
            spot_subpaths = as_list(params.spot_subpaths)
        } else {
            def spot_channels = as_list(params.spot_channels)
            def spot_scales = as_list(params.spot_scales)

            spot_subpaths = [spot_channels, spot_scales].combinations()
                .collect {
                    // when channel and scale is used we also prepend the stitched dataset
                    def dataset = it.join('/')
                    "${meta.stitched_dataset}/${dataset}"
            }
        }

        spot_subpaths.collect { input_spot_subpath ->
            [
                meta,
                input_img_dir,
                input_spot_subpath,
                rsfish_spark,
            ]
        }
    }

    rsfish_input.subscribe { log.debug "RS_FISH input: $it" }

    RS_FISH(rsfish_input)

    def rsfish_results = RS_FISH.out.params
    | combine(RS_FISH.out.csv)
    | map {
        def (meta, input_image, input_dataset, spark, output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            spark,
            output_filename,
        ]
    }
    rsfish_results.subscribe { log.info "!!!!!!!!!!!!! RS_FISH results: $it" }

    def prepare_spark_stop = rsfish_results
    | groupTuple(by: [0, 3]) // group by meta and spark
    | map {
        def (meta, input_image, input_dataset, spark, output_filename) = it
        [
            meta, spark,
        ]
    }

    SPARK_STOP(
        prepare_spark_stop,
        params.distributed_spot_extraction,
    )

    emit:
    done = rsfish_results
}

def get_spot_extraction_input_volume(meta) {
    return "${meta.stitching_result_dir}/${meta.stitching_container}"
}
