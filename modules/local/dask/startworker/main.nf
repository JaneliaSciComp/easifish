process DASK_STARTWORKER {
    container { task.ext.container ?: 'docker.io/multifish/biocontainers-dask:2023.8.1' }
    cpus { worker_cores }
    memory "${worker_mem_in_gb} GB"
    clusterOptions { task.ext.cluster_opts }

    input:
    tuple val(meta), path(cluster_work_dir), val(scheduler_address), val(worker_id)
    path(data)
    val(worker_cores)
    val(worker_mem_in_gb)

    output:
    tuple val(meta), val(cluster_work_fullpath), val(scheduler_address), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: [:]
    def container_engine = workflow.containerEngine

    def dask_worker_name = "worker-${worker_id}"
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_worker_start_timeout_secs = args.dask_worker_start_timeout_secs ?: '120'
    def dask_worker_poll_interval_secs = args.dask_worker_poll_interval_secs ?: '5'
    def dask_worker_work_dir = "${cluster_work_dir}/${dask_worker_name}"
    def dask_worker_pid_file = "${dask_worker_work_dir}/${dask_worker_name}.pid"
    def dask_worker_threads_arg = args.dask_worker_threads > 0 
                                    ? "--nthreads ${args.dask_worker_threads}"
                                    : ""
    def dask_worker_base_port_arg = args.dask_worker_base_port > 0
                                    ? "--worker-port ${args.dask_worker_base_port+worker_id-1}"
                                    : ""

    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    worker_name = dask_worker_name
    worker_dir = dask_worker_work_dir

    """
    /opt/scripts/daskscripts/startworker.sh \
        --container-engine ${container_engine} \
        --name ${dask_worker_name} \
        --worker-dir ${dask_worker_work_dir} \
        --scheduler-address ${scheduler_address} \
        --pid-file ${dask_worker_pid_file} \
        --memory-limit "${worker_mem_in_gb}G" \
        --worker-start-timeout ${dask_worker_start_timeout_secs} \
        --worker-poll-interval ${dask_worker_poll_interval_secs} \
        --terminate-file ${terminate_file_name} \
        ${dask_worker_base_port_arg} \
        ${dask_worker_threads_arg}

    cat <<-END_VERSIONS > versions.yml
    "dask":
        : \$(echo \$(dask --version 2>&1)))
    END_VERSIONS
    """
}
