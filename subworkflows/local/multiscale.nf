include { SPARK_START        } from '../janelia/spark_start/main'
include { SPARK_STOP         } from '../janelia/spark_stop/main'

include { MULTISCALE_PYRAMID } from '../../modules/local/multiscale/pyramid/main'

workflow MULTISCALE {
    take:
    ch_meta                 // channel: [ meta, n5_container_dir, fullscale_dataset ]
    with_spark_cluster      // boolean: use a distributed spark cluster
    workdir                 // string|file: spark work dir
    skip                    // boolean: if true skip multiscale completely and just return the meta as if it ran
    spark_workers           // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_spark_workers       // int: min required spark workers
    spark_worker_cores      // int: number of cores per worker
    spark_gb_per_core       // int: number of GB of memory per worker core
    spark_driver_cores      // int: number of cores for the driver
    spark_driver_mem_gb     // int: number of GB of memory for the driver

    main:
    def prepared_data = ch_meta
    | map {
        def (meta, n5_container_dir, fullscale_dataset) = it
        meta.session_work_dir = "${workdir}/${meta.id}"
        meta.downsampled_container = n5_container_dir
        meta.downsampled_dataset = fullscale_dataset
        def r = [
            meta,
            [ n5_container_dir ],
        ]
        log.debug "Input to downsample: $it -> $r"
        r
    }

    if (!skip) {
        def downsample_input = SPARK_START(
            prepared_data, // [meta, data_paths]
            with_spark_cluster,
            workdir,
            spark_workers,
            min_spark_workers,
            spark_worker_cores,
            spark_gb_per_core,
            spark_driver_cores,
            spark_driver_mem_gb
        ) // ch: [ meta, spark ]
        | join(ch_meta, by: 0) // join to add the files
        | map {
            def (meta, spark, n5_container, fullscale_dataset) = it
            def r = [
                meta, n5_container, fullscale_dataset, spark,
            ]
            log.debug "Downsample input: $it -> $r"
            r
        }

        MULTISCALE_PYRAMID(downsample_input)

        def spark_stop_input = MULTISCALE_PYRAMID.out.data
        | map {
            def (meta, n5_container, fullscale_dataset, spark) = it
            log.debug "Completed downsampling  $it"
            // spark_stop only needs meta and spark
            log.debug "Prepare to stop [${meta}, ${spark}]"
            [ meta, spark ]
        }

        completed_downsampled_result = SPARK_STOP(spark_stop_input, with_spark_cluster)
        | map {
            // Only meta contains data relevant for the next steps
            def (meta, spark) = it
            log.debug "Stopped multiscale spark ${spark} - downsampled result: $meta"
            meta
        }
    } else {
        completed_downsampled_result = prepared_data
        | map {
            def (meta, data_files) = it
            log.debug "Downsampled result (skipped): $meta"
            meta
        }
    }

    emit:
    completed_downsampled_result // channel: meta

}