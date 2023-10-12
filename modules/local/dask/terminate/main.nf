process DASK_TERMINATE {
    label 'process_low'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    tuple val(meta), path(cluster_work_dir)

    output:
    tuple val(meta), val(cluster_work_fullpath)

    when:
    task.ext.when == null || task.ext.when

    script:
    def cluster_work_path = cluster_work_dir
    cluster_work_fullpath = cluster_work_path.resolveSymLink().toString()
    def terminate_file_name = "${cluster_work_path}/terminate-dask"
    """
    echo "\$(date): Terminate DASK Scheduler: ${cluster_work_path}"
    echo $PWD
    cat > ${terminate_file_name} <<EOF
    \$(date)
    DONE
    EOF
    cat ${terminate_file_name}
    """
}
