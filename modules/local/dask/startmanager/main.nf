process DASK_STARTMANAGER {
    label 'process_single'
    container { params.dask_container ?: 'docker.io/multifish/biocontainers-dask:2023.8.1' }

    input:
    tuple val(meta), path(cluster_work_dir)

    output:
    tuple val(meta), env(cluster_work_fullpath), emit: cluster_info
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def container_engine = workflow.containerEngine
    
    def dask_scheduler_pid_file ="${cluster_work_dir}/dask-scheduler.pid"
    def dask_scheduler_info_file = "${cluster_work_dir}/dask-scheduler-info.json"
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"

    """
    cluster_work_fullpath=\$(realpath ${cluster_work_dir})

    /opt/scripts/daskscripts/startmanager.sh \
        --container-engine ${container_engine} \
        --pid-file ${dask_scheduler_pid_file} \
        --scheduler-file ${dask_scheduler_info_file} \
        --terminate-file ${terminate_file_name} \
        ${args}

    cat <<-END_VERSIONS > versions.yml
    "dask":
        : \$(echo \$(dask --version 2>&1)))
    END_VERSIONS
    """
}
