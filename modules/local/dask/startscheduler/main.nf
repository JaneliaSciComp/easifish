process DASK_STARTSCHEDULER {
    label 'process_single'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    tuple path(cluster_work_dir)

    output:
    tuple val(cluster_work_fullpath), emit: clusterpath
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def container_engine = workflow.containerEngine
    
    def dask_scheduler_pid_file ="${cluster_work_dir}/dask-scheduler.pid"
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_scheduler_start_timeout_secs = args.dask_scheduler_start_timeout_secs ?: '120'
    def dask_scheduler_poll_interval_secs = args.dask_scheduler_poll_interval_secs ?: '5'
    def dask_scheduler_port_arg = args.dask_scheduler_port ? "--port ${args.dask_scheduler_port}" : ''

    def with_dashboard_arg = params.with_dask_dashboard 
                                ? "--dashboard"
                                : ""


    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    """
    /opt/scripts/startscheduler.sh \
        --container-engine ${container_engine} \
        --pid-file ${dask_scheduler_pid_file} \
        --scheduler-file ${dask_scheduler_info_file} \
        --scheduler-start-timeout ${dask_scheduler_start_timeout_secs} \
        --scheduler-poll-interval ${dask_scheduler_poll_interval_secs} \
        --terminate-file ${terminate_file_name} \
        ${dask_scheduler_port_arg} \
        ${with_dashboard_arg}

    cat <<-END_VERSIONS > versions.yml
    "dask":
        : \$(echo \$(dask --version 2>&1)))
    END_VERSIONS
    """
}
