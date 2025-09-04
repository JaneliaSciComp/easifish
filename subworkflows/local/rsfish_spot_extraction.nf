include { POST_RS_FISH } from '../../modules/local/post_rs_fish'
include { RS_FISH      } from '../../modules/janelia/rs_fish'

include { SPARK_START  } from '../janelia/spark_start'
include { SPARK_STOP   } from '../janelia/spark_stop'

workflow RSFISH_SPOT_EXTRACTION {
    take:
    ch_spots_input             // ch: [ meta, input_img, input_subpath, spots_output_dir, spots_output_name ]
    distributed                // boolean
    workdir
    spark_workers
    min_spark_workers          // int: min required spark workers
    spark_worker_cores         // int: number of cores per worker
    spark_gb_per_core          // int: number of GB of memory per worker core
    spark_driver_cores         // int: number of cores for the driver
    spark_driver_mem_gb        // int: number of GB of memory for the driver

    main:
    def spots_spark_input = ch_spots_input
    | map {
        def (meta, input_img, input_subpath, spots_output_dir, spots_output_name) = it
        [
            meta,
            input_img,
            input_subpath,
            spots_output_dir,
            spots_output_name,
            rsfish_spark,
        ]
    }

    def rsfish_input = SPARK_START(
        spots_spark_input,
        [:],
        distributed,
        workdir,
        spark_workers,
        min_spark_workers,
        spark_worker_cores,
        spark_gb_per_core,
        spark_driver_cores,
        spark_driver_mem_gb,
    ) // ch: [ meta, spark ]
    | join(ch_spots_input, by:0)
    | map {
        def (meta, rsfish_spark,
             input_img, input_subpath,
             spots_output_dir, spots_output_name) = it
        def r = [
            meta,
            input_img,
            input_subpath,
            spots_output_dir,
            spots_output_name,
            rsfish_spark,
        ]
        log.debug "RS_FISH input: $r"
        r
    }

    RS_FISH(rsfish_input)

    def rsfish_results = RS_FISH.out.params
    | join(RS_FISH.out.csv, by: 0)
    | map {
        def (meta, input_image, input_dataset, spots_output_dir, spots_result_name, spark, full_output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            full_output_filename,
            spark,
        ]
    }
    rsfish_results.subscribe { log.debug "RS_FISH results: $it" }

    rsfish_results
    | map {
        def (meta, input_image, input_dataset, output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            output_filename,
        ]
    }
    | POST_RS_FISH

    final_rsfish_results = expand_spot_results(POST_RS_FISH.out.results)

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

    emit:
    done = final_rsfish_results

}
