include { SPOTS_RSFISH } from '../../modules/janelia/spots/rsfish'

include { SPARK_START  } from '../janelia/spark_start'
include { SPARK_STOP   } from '../janelia/spark_stop'

workflow RSFISH_SPOT_EXTRACTION {
    take:
    ch_spots_input                 // ch: [ meta, input_img, input_subpath, spots_output_dir, spots_output_name, spots_image_subpath_ref, spots_channels ]
    distributed                    // boolean
    workdir
    localdir
    spark_workers
    min_spark_workers              // int: min required spark workers
    spark_worker_cpus              // int: number of cpus per worker
    spark_worker_mem_gb            // int: number of GB of memory per worker
    spark_executor_cpus            // int: number of cpus per executor
    spark_executor_mem_gb          // number: number of GB of memory per executor
    spark_executor_overhead_mem_gb // number: executor memory overhead in GB
    spark_task_cpus                // int: number of cpus required for a spark executor task
    spark_driver_cpus              // int: number of cpus for the driver
    spark_driver_mem_gb            // int: number of GB of memory for the driver
    spark_gb_per_core              // int: number of GB of memory per worker core
    spark_config

    main:
    def spots_spark_input = ch_spots_input
    | map { it ->
        def (meta,
             input_img, _input_subpath,
             spots_output_dir, _spots_output_name,
             _spots_image_subpath_ref, _spots_channels) = it
        [
            meta,
            [ input_img, spots_output_dir ],
        ]
    }

    def rsfish_input = SPARK_START(
        spots_spark_input,
        spark_config,
        distributed,
        workdir,
        localdir,
        spark_workers,
        min_spark_workers,
        spark_worker_cpus,
        spark_worker_mem_gb,
        spark_executor_cpus,
        spark_executor_mem_gb,
        spark_executor_overhead_mem_gb,
        spark_task_cpus,
        spark_driver_cpus,
        spark_driver_mem_gb,
        spark_gb_per_core,
    ) // ch: [ meta, spark ]
    | join(ch_spots_input, by:0)
    | map { it ->
        def (meta, rsfish_spark, input_img, input_subpath, spots_output_dir, spots_output_name, _spots_image_subpath_ref, spots_channels) = it
        def r = [
            meta,
            input_img,
            input_subpath,
            spots_output_dir,
            spots_output_name,
            spots_channels,
            rsfish_spark,
        ]
        log.debug "RS_FISH input: $r"
        r
    }

    SPOTS_RSFISH(rsfish_input)

    def rsfish_results = SPOTS_RSFISH.out.params
    | join(SPOTS_RSFISH.out.csv, by: 0)
    | map { it ->
        def (meta, input_image, input_dataset, _spots_output_dir, _spots_result_name, spark, full_output_filename) = it
        [
            meta,
            input_image,
            input_dataset,
            full_output_filename,
            spark,
        ]
    }

    def final_rsfish_results = rsfish_results
    | map { it ->
        log.debug "RS_FISH results: $it"

        def (meta, input_image, input_dataset, full_output_filename) = it
        def r = [
            meta,
            input_image,
            input_dataset,
            full_output_filename,
        ]
        log.debug "Final RS_FISH results: $r"
        r
    }

    def prepare_spark_stop = rsfish_results
    | groupTuple(by: [0, 4]) // group by meta and spark
    | map { it ->
        def (meta, _input_image, _input_dataset, _output_filename, spark) = it
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
