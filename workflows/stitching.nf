include { BIGSTITCHER        } from '../subworkflows/local/bigstitcher'
include { SAALFELD_STITCHING } from '../subworkflows/local/saalfeld_stitching'


workflow STITCHING {
    take:
    ch_acquisition_data // channel: [meta, files]
    outdir              // string|file: output directory
    workdir             // string|file: session work directory

    main:
    def stitching_work_dir = params.stitching_dir ? file(params.stitching_dir) : "${outdir}/stitching" // stitcher's work directory
    def stitching_result_dir = params.stitching_result_dir ? file(params.stitching_result_dir) : outdir

    def stitching_results
    if (params.stitching_method == 'BigStitcher') {
        stitching_results = BIGSTITCHER(
            ch_acquisition_data,
            params.spark_cluster,
            stitching_result_dir,
            params.stitching_result_container,
            params.advanced_stitching_params,
            params.preserve_anisotropy,
            params.skip_stitching,
            params.skip_bigstitcher_create_dataset,
            params.skip_bigstitcher_resave,
            params.skip_bigstitcher_pairwise_stitch,
            params.skip_bigstitcher_create_container,
            params.skip_bigstitcher_affine_fusion,
            "${workdir}/stitching",
            params.spark_workers as int,
            params.min_spark_workers as int,
            params.spark_worker_cores as int,
            params.spark_gb_per_core as int,
            params.spark_driver_cores as int,
            params.spark_driver_mem_gb as int,
            create_stitching_spark_config(),
        )
    } else {
        stitching_results = SAALFELD_STITCHING(
            ch_acquisition_data,
            params.flatfield_correction,
            params.spark_cluster,
            stitching_work_dir,
            params.darkfieldfile,
            params.flatfieldfile,
            stitching_result_dir,
            params.stitching_result_container,
            params.skip_stitching,
            "${workdir}/stitching",
            params.spark_workers as int,
            params.min_spark_workers as int,
            params.spark_worker_cores as int,
            params.spark_gb_per_core as int,
            params.spark_driver_cores as int,
            params.spark_driver_mem_gb as int,
            create_stitching_spark_config(),
        )
    }

    emit:
    done = stitching_results

}

def create_stitching_spark_config() {
    def spark_config = [:]
    if (params.spark_executor_cores) {
        spark_config['spark.executor.cores'] = params.spark_executor_cores
    }
    if (params.spark_executor_memoryOverhead) {
        spark_config['spark.executor.memoryOverhead'] = params.spark_executor_memoryOverhead
    }
    if (params.spark_max_partition_bytes) {
        spark_config['spark.sql.files.maxPartitionBytes'] = params.spark_max_partition_bytes
    }
    return spark_config
}
