process DASK_CLUSTERINFO {
    label 'process_low'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    path(cluster_work_dir)

    output:
    val(cluster_work_fullpath), emit: clusterpath
    tuple env(scheduler_address), val(cluster_work_fullpath), val(available_workers), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: [:]
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    def dask_scheduler_start_timeout_secs = args.dask_scheduler_start_timeout_secs ?: '120'
    def dask_scheduler_poll_interval_secs = args.dask_scheduler_poll_interval_secs ?: '5'

    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    available_workers = 0

    """
    /opt/scripts/waitforanyfile.sh "0" \
        "${dask_scheduler_info_file},${terminate_file_name}" \
        ${dask_scheduler_start_timeout_secs} \
        ${dask_scheduler_poll_interval_secs}

    if [[ -e "${dask_scheduler_info_file}" ]] ; then
        echo "\$(date): Get cluster info from ${dask_scheduler_info_file}"
        scheduler_address=\$(jq ".address" ${dask_scheduler_info_file})
        cluster_workdir="${cluster_work_fullpath}"
    else
        echo "\$(date): Cluster info file ${dask_scheduler_info_file} not found"
        scheduler_address=
    fi

    cat <<-END_VERSIONS > versions.yml
    "dask":
        : \$(echo \$(dask --version 2>&1)))
    END_VERSIONS
    """

}