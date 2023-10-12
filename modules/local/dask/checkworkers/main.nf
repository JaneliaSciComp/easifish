process DASK_CHECKWORKERS {
    label 'process_low'
    container { task.ext.container ?: 'docker.io/multifish/biocontainers-dask:2023.8.1' }

    input:
    tuple val(meta), path(cluster_work_dir), val(scheduler_address)
    val(total_workers)
    val(required_workers)

    output:
    tuple val(meta), val(cluster_work_fullpath), val(scheduler_address), env(available_workers), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: [:]
    def container_engine = workflow.containerEngine
    
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_worker_start_timeout_secs = args.dask_worker_start_timeout_secs ?: '120'
    def dask_worker_poll_interval_secs = args.dask_worker_poll_interval_secs ?: '5'

    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()

    """
    # checkworkers.sh sets available_workers variable
    . /opt/scripts/daskscripts/checkworkers.sh \
        --cluster-work-dir ${cluster_work_dir} \
        --scheduler-address ${scheduler_address} \
        --worker-start-timeout ${dask_worker_start_timeout_secs} \
        --worker-poll-interval ${dask_worker_poll_interval_secs} \
        --total-workers ${total_workers} \
        --required-workers ${required_workers} \
        --terminate-file ${terminate_file_name}

    cat <<-END_VERSIONS > versions.yml
    "dask":
        : \$(echo \$(dask --version 2>&1)))
    END_VERSIONS
    """
}
