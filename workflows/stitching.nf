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
        def bigstitcher_config = params.bigstitcher_config
            ? new org.yaml.snakeyaml.Yaml().load(new java.io.FileInputStream(params.bigstitcher_config))
            : [:]
        stitching_results = BIGSTITCHER(
            ch_acquisition_data,
            params.spark_cluster,
            stitching_result_dir,
            params.stitching_result_container,
            params.skip_stitching,
            bigstitcher_config,
            ParamUtils.as_list(params.bigstitcher_steps),
            "${workdir}/stitching",
            params.spark_local_dir,
            params.spark_workers as int,
            params.min_spark_workers as int,
            params.spark_worker_cores as int,
            params.spark_worker_mem_gb as int,
            params.spark_executor_cores as int,
            params.spark_executor_mem_gb as float,
            params.spark_executor_overhead_mem_gb as float,
            params.spark_task_cores,
            params.spark_driver_cores as int,
            params.spark_driver_mem_gb as int,
            params.spark_gb_per_core as int,
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
            params.spark_local_dir,
            params.spark_workers as int,
            params.min_spark_workers as int,
            params.spark_worker_cores as int,
            params.spark_worker_mem_gb as int,
            params.spark_executor_cores as int,
            params.spark_executor_mem_gb as float,
            params.spark_executor_overhead_mem_gb as float,
            params.spark_task_cores,
            params.spark_driver_cores as int,
            params.spark_driver_mem_gb as int,
            params.spark_gb_per_core as int,
            create_stitching_spark_config(),
        )
    }

    emit:
    done = stitching_results

}

def create_stitching_spark_config() {
    def spark_config = [:]
    if (params.spark_max_partition_bytes) {
        spark_config['spark.sql.files.maxPartitionBytes'] = params.spark_max_partition_bytes
    }
    return spark_config
}
