process DASK_STARTSCHEDULER {
    label 'process_single'
    container { task.ext.container ?: 'docker.io/multifish/biocontainers-dask:2023.8.1' }
    containerOptions { get_container_opts(task.ext.args ?: [:]) }

    input:
    tuple val(meta), path(cluster_work_dir)

    output:
    tuple val(meta), val(cluster_work_fullpath), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: [:]
    def container_engine = workflow.containerEngine
    
    def dask_scheduler_pid_file ="${cluster_work_dir}/dask-scheduler.pid"
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_scheduler_start_timeout_secs = args.dask_scheduler_start_timeout_secs ?: '120'
    def dask_scheduler_poll_interval_secs = args.dask_scheduler_poll_interval_secs ?: '5'
    def dask_scheduler_port_arg = args.dask_scheduler_port ? "--port ${args.dask_scheduler_port}" : ''

    def with_dashboard_arg = args.with_dask_dashboard 
                                ? "--dashboard"
                                : ""


    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    """
    /opt/scripts/daskscripts/startscheduler.sh \
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

def get_container_opts(args) {
    def containerOptions = args.containerOptions ?: ''
    if (workflow.containerEngine == 'docker') {
        def dask_scheduler_port = args.dask_scheduler_port ?: 8786

        def port_bindings = "-p ${dask_scheduler_port}:${dask_scheduler_port}"
        if (args.with_dask_dashboard) {
            def dask_dashboard_port = args.dask_dashboard_port ?: 8787
            port_bindings = "${port_bindings} -p ${dask_dashboard_port}:${dask_dashboard_port}"
        }
        return containerOptions + ' ' + port_bindings
    }
    return containerOptions
}
