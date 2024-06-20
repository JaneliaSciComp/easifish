
include { SPARK_START            } from '../janelia/spark_start/main'
include { SPARK_STOP             } from '../janelia/spark_stop/main'

include { STITCHING_PREPARE      } from '../../modules/local/stitching/prepare/main'
include { STITCHING_PARSECZI     } from '../../modules/local/stitching/parseczi/main'
include { STITCHING_CZI2N5       } from '../../modules/local/stitching/czi2n5/main'
include { STITCHING_FLATFIELD    } from '../../modules/local/stitching/flatfield/main'
include { STITCHING_STITCH       } from '../../modules/local/stitching/stitch/main'
include { STITCHING_FUSE         } from '../../modules/local/stitching/fuse/main'

workflow STITCHING {
    take:
    acquisition_data        // channel: [ meta, files ]
    flatfield_correction    // boolean: run flatfield correction
    with_spark_cluster      // boolean: use a distributed spark cluster
    stitching_dir           // string|file: directory holding intermediate stitching data
    darkfield               // string|file: file containing the darkfield for flatfield correction
    flatfield               // string|file: file containing the flatfield for flatfield correction
    stitching_result_dir    // string|file: directory where the final stitched results will be stored
    stitched_container_name // final stitched container name - defaults to export.n5
    id_for_stiched_dataset  // boolean: if true use id for stitched dataset otherwise no dataset is used 
    workdir                 // string|file: spark work dir
    skip                    // boolean: if true skip stitching completely and just return the meta as if it ran
    spark_workers           // int: number of workers in the cluster (ignored if spark_cluster is false)
    spark_worker_cores      // int: number of cores per worker
    spark_gb_per_core       // int: number of GB of memory per worker core
    spark_driver_cores      // int: number of cores for the driver
    spark_driver_mem_gb     // int: number of GB of memory for the driver

    main:
    def darkfield_file = darkfield ? file(darkfield) : []
    def flatfield_file = flatfield ? file(flatfield) : []

    def prepared_data = acquisition_data
    | map {
        def (meta, files) = it
        // set output subdirectories for each acquisition
        meta.session_work_dir = "${workdir}/${meta.id}"
        meta.stitching_dir = "${stitching_dir}/${meta.id}"
        meta.stitching_result_dir = stitching_result_dir
        meta.stitched_dataset = id_for_stiched_dataset ? meta.id : ''
        meta.stitching_container = stitched_container_name ?: "export.n5"
        // Add output dir here so that it will get mounted into the Spark processes
        def data_files = files +
            [stitching_dir, stitching_result_dir] +
            (darkfield_file ? [darkfield_file] : []) +
            (flatfield_file ? [flatfield_file] : [])
        def r = [ meta, data_files ]
        log.debug "Input acquisitions to stitch: ${data_files} -> $r"
        r
    }
    | STITCHING_PREPARE
    | map {
        def (meta, sfiles) = it
        def data_files = sfiles.tokenize().collect { file(it) }
        [ meta, data_files ]
    }

    prepared_data.subscribe { log.debug "Prepared stitching input: $it" }

    if (!skip) {
        def stitching_input = SPARK_START(
            prepared_data, // [meta, data_paths]
            with_spark_cluster,
            workdir,
            spark_workers,
            spark_worker_cores,
            spark_gb_per_core,
            spark_driver_cores,
            spark_driver_mem_gb
        ) // ch: [ meta, spark ]
        | join(prepared_data, by: 0) // join to add the files
        | map {
            def (meta, spark, files) = it
            // rearrange input args
            def r = [
                meta, files, spark,
            ]
            log.debug "Stitching input: $it -> $r"
            r
        }

        STITCHING_PARSECZI(stitching_input)

        STITCHING_CZI2N5(STITCHING_PARSECZI.out.acquisitions)

        def flatfield_results
        if (flatfield_correction) {
            flatfield_results = STITCHING_FLATFIELD(
                STITCHING_CZI2N5.out.acquisitions,
                darkfield_file,
                flatfield_file,
            )
            .acquisitions
        } else {
            flatfield_results = STITCHING_CZI2N5.out.acquisitions
        }

        STITCHING_STITCH(
            flatfield_results,
            darkfield_file,
            flatfield_file,
        )

        STITCHING_FUSE(
            STITCHING_STITCH.out.acquisitions,
            darkfield_file,
            flatfield_file,
        )

        def spark_stop_input = STITCHING_FUSE.out.acquisitions
        | map {
            def (meta, files, spark) = it
            log.debug "Finished stitching for $meta; prepare to stop $spark"
            // spark_stop only needs meta and spark
            [ meta, spark ]
        }

        completed_stitching_result = SPARK_STOP(spark_stop_input, with_spark_cluster)
        | map {
            // Only meta contains data relevant for the next steps
            def (meta, spark) = it
            log.debug "Stitching result: $meta"
            meta
        }
    } else {
        completed_stitching_result = prepared_data
        | map {
            def (meta, data_files) = it
            log.debug "Stitching result: $meta"
            meta
        }
    }

    emit:
    completed_stitching_result // channel: meta
}
