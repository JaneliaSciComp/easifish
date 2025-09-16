include { SPARK_START                            } from '../janelia/spark_start/main'
include { SPARK_STOP                             } from '../janelia/spark_stop/main'

include { BIGSTITCHER_MODULE as STITCH           } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as CREATE_CONTAINER } from '../../modules/janelia/bigstitcher/module'
include { BIGSTITCHER_MODULE as FUSE             } from '../../modules/janelia/bigstitcher/module'

workflow BIGSTITCHER {

    take:
    ch_acquisition_data           // channel: [ meta, files ]
    with_spark_cluster            // boolean: use a distributed spark cluster
    stitching_result_dir
    stitched_image_name           // stitched container name
    preserve_anisotropy
    skip_all_steps
    skip_pairwise_stitch
    skip_create_container
    skip_affine_fusion
    spark_workdir                 // string|file: spark work dir
    spark_workers                 // int: number of workers in the cluster (ignored if spark_cluster is false)
    min_spark_workers             // int: min required spark workers
    spark_worker_cores            // int: number of cores per worker
    spark_gb_per_core             // int: number of GB of memory per worker core
    spark_driver_cores            // int: number of cores for the driver
    spark_driver_mem_gb           // int: number of GB of memory for the driver

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
        def data_files = files
        if (stitching_meta.stitching_xml == null) {
            log.debug 'No XML project found for BigStitcher.'
        } else {
            log.debug "Stitching project file was set: ${stitching_meta.stitching_xml}"
            data_files << file(stitching_meta.stitching_xml).parent
        }
        def r = [ stitching_meta, data_files ]
        log.debug "Prepared stitching input: $r"
        r
    }

    def stitching_results
    if (skip_all_steps || skip_pairwise_stitch && skip_create_container && skip_affine_fusion) {
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

        def pairwise_stitch_output
        if (skip_pairwise_stitch) {
            pairwise_stitch_output = stitching_input.map {
                def (meta, spark) = it
                [ meta, spark ]
            }
        } else {
            def pairwise_stitching_step_inputs = stitching_input
            | multiMap {
                def (meta, spark, files) = it
                def bigstitcher_class
                def bigstitcher_params
                // if stitching xml BDV is not set we get a default value and use that
                // but we don't set it back in the meta because
                // that alters the hash and downstream joins will not work correctly
                def stitching_xml = get_stitching_xml_or_default(meta)
                if (meta.stitching_xml) {
                    // BDV XML project is present in meta
                    bigstitcher_class = 'net.preibisch.bigstitcher.spark.SparkPairwiseStitching'
                    bigstitcher_params = [
                        '-x', stitching_xml,
                    ]
                } else {
                    // BDV XML project is not present in meta
                    bigstitcher_class = 'net.preibisch.bigstitcher.spark.ChainCommands'
                    def pattern = meta.pattern ?: "*.czi"
                    bigstitcher_params = [
                        '--command=create-dataset',
                        '--input-pattern', meta.pattern,
                        '--input-path', meta.image_dir,
                        '-x', stitching_xml,
                        '+',
                        '--command=resave',
                        '-x', stitching_xml,
                        '-o', "${meta.image_dir}/dataset.zarr",
                        '+',
                        '--command=stitching',
                        '-x', stitching_xml,
                    ]
                }
                log.debug "Bigstitcher parameters: ${bigstitcher_class}: ${bigstitcher_params}"
                def module_args = [
                    meta,
                    spark,
                    bigstitcher_class,
                    bigstitcher_params,
                ]
                log.debug "Stitching module args: $module_args"
                module_args: module_args
                data_files: files
            }

            pairwise_stitch_output = STITCH(
                pairwise_stitching_step_inputs.module_args,
                pairwise_stitching_step_inputs.data_files,
            )

            pairwise_stitch_output.subscribe { log.debug "Stitching output: $it" }

        }

        def create_fused_container_output
        if (skip_create_container) {
            create_fused_container_output = pairwise_stitch_output
        } else {
            def create_fused_container_inputs = stitching_input
            | join(pairwise_stitch_output, by: 0)
            | multiMap {
                def (meta, spark, files) = it
                def stitching_xml = get_stitching_xml_or_default(meta)
                def preserve_anisotropy_arg = preserve_anisotropy ? '--preserveAnisotropy' : ''
                def module_args = [
                    meta,
                    spark,
                    'net.preibisch.bigstitcher.spark.CreateFusionContainer',
                    [
                        '-x', stitching_xml,
                        '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
                        "--group", meta.id,
                        '-s', meta.stitching_container_storage,
                        '--multiRes',
                        preserve_anisotropy_arg,
                    ]
                ]
                log.debug "Create-container module args: $module_args"

                module_args: module_args
                data_files: files
            }

            create_fused_container_output = CREATE_CONTAINER(
                create_fused_container_inputs.module_args,
                create_fused_container_inputs.data_files,
            )

        }

        def fuse_output
        if (skip_affine_fusion) {
            fuse_output = create_fused_container_output
        } else {
            def fuse_inputs = stitching_input
            | join(create_fused_container_output, by: 0)
            | multiMap {
                def (meta, spark, files) = it
                def module_args = [
                    meta,
                    spark,
                    'net.preibisch.bigstitcher.spark.SparkAffineFusion',
                    [
                        '-o', "${meta.stitching_result_dir}/${meta.stitching_container}",
                        "--group", meta.id,
                        '-s', meta.stitching_container_storage,
                    ]
                ]
                log.debug "Affine fuse module args: $module_args"

                module_args: module_args
                data_files: files
            }

            fuse_output = FUSE(
                fuse_inputs.module_args,
                fuse_inputs.data_files,
            )

        }

        stitching_results = SPARK_STOP(
            fuse_output,
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

def get_stitching_xml_or_default(meta) {
    return meta.stitching_xml ?: "${meta.image_dir}/dataset.xml"
}
