include { SPARK_START                            } from '../janelia/spark_start/main'
include { SPARK_STOP                             } from '../janelia/spark_stop/main'

include { BIGSTITCHER_MODULE as STITCH           } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as CREATE_CONTAINER } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as FUSE             } from '../../modules/janelia/bigstitcher/module'

workflow BIGSTITCHER {

    take:
    ch_acquisition_data        // channel: [ meta, files ]
    with_spark_cluster         // boolean: use a distributed spark cluster
    stitching_result_dir
    stitched_image_name        // stitched container name
    skip
    spark_workdir              // string|file: spark work dir
    spark_workers              // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_spark_workers          // int: min required spark workers
    spark_worker_cores         // int: number of cores per worker
    spark_gb_per_core          // int: number of GB of memory per worker core
    spark_driver_cores         // int: number of cores for the driver
    spark_driver_mem_gb        // int: number of GB of memory for the driver

    main:
    def prepared_data = ch_acquisition_data
    | map {
        def (meta, files) = it
        def stitching_meta = meta.clone()

        stitching_meta.session_work_dir = "${spark_workdir}/${meta.id}"
        stitching_meta.stitching_result_dir = stitching_result_dir
        stitching_meta.stitched_dataset = meta.id
        stitching_meta.stitching_container = stitched_image_name ?: "fused.ome.zarr"

        def to_lowercase_image_name = stitching_meta.stitching_container.toLowerCase()
        if (to_lowercase_image_name.endsWith('.n5')) {
            stitching_meta.stitching_container_storage = 'N5'
        } else if (to_lowercase_image_name.endsWith('.h5') ||
                   to_lowercase_image_name.endsWith('.hdf5')) {
            stitching_meta.stitching_container_storage = 'HDF5'
        } else {
            // default to OME-ZARR
            stitching_meta.stitching_container_storage = 'ZARR'
        }

        stitching_meta.stitching_xml = files.find { it.extension == 'xml' }
        if (stitching_meta.stitching_xml == null) {
            log.error 'No XML project found for BigStitcher. This may cause an error later'
        } else {
            log.debug "Stitching project file: ${stitching_meta.stitching_xml}"
        }
        def data_files = files + [
            file(stitching_meta.stitching_xml).parent,
        ]
        def r = [ stitching_meta, data_files ]
        log.debug "Prepared stitching input: $r"
        r
    }

    def stitching_results
    if (skip) {
        stitching_results = prepared_data.map {
            def (meta) = it
            meta
        }
    } else {
        def stitching_input = SPARK_START(
            prepared_data,
            [:], // spark config
            with_spark_cluster,
            spark_workdir,
            spark_workers,
            min_spark_workers,
            spark_worker_cores,
            spark_gb_per_core,
            spark_driver_cores,
            spark_driver_mem_gb
        )
        | join(prepared_data, by: 0)

        stitching_input.subscribe { log.debug "Stitching input: $it" }

        def pairwise_stitching_step_inputs = stitching_input
        | multiMap {
            def (meta, spark, files) = it
            def module_args = [
                meta,
                spark,
                'net.preibisch.bigstitcher.spark.SparkPairwiseStitching',
                [
                    '-x', meta.stitching_xml,
                ],
            ]
            log.debug "Stitching module args: $module_args"
            module_args: module_args
            data_files: files
        }

        STITCH(
            pairwise_stitching_step_inputs.module_args,
            pairwise_stitching_step_inputs.data_files,
        )

        def create_fused_container_inputs = stitching_input
        | join(STITCH.out, by: 0)
        | multiMap {
            def (meta, spark, files) = it
            def module_args = [
                meta,
                spark,
                'net.preibisch.bigstitcher.spark.CreateFusionContainer',
                [
                    '-x', meta.stitching_xml,
                    '-o', "${meta.stitching_result_dir}/${meta.stitching_container}/${meta.id}",
                    '-s', meta.stitching_container_storage,
                    '--multiRes',
                ]
            ]
            log.info "Create-container module args: $module_args"

            module_args: module_args
            data_files: files
        }

        CREATE_CONTAINER(
            create_fused_container_inputs.module_args,
            create_fused_container_inputs.data_files,
        )

        def fuse_inputs = stitching_input
        | join(CREATE_CONTAINER.out, by: 0)
        | multiMap {
            def (meta, spark, files) = it
            def module_args = [
                meta,
                spark,
                'net.preibisch.bigstitcher.spark.SparkAffineFusion',
                [
                    '-o', "${meta.stitching_result_dir}/${meta.stitching_container}/${meta.id}",
                ]
            ]
            log.info "Affine fuse module args: $module_args"

            module_args: module_args
            data_files: files
        }

        FUSE(
            fuse_inputs.module_args,
            fuse_inputs.data_files,
        )

        stitching_results = SPARK_STOP(
            FUSE.out,
            with_spark_cluster,
        )
        | map {
            // Only meta contains data relevant for the next steps
            def (meta, spark) = it
            log.debug "Stopped spark ${spark} - stitching result: $meta"
            meta
        }
    }

    emit:
    done = stitching_results
}
