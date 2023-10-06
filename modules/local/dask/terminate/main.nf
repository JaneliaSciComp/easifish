process DASK_TERMINATE {
    label 'process_low'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    path(cluster_work_dir)

    output:
    val(cluster_work_fullpath)

    script:
    cluster_work_fullpath = cluster_work_dir.resolveSymLink().toString()
    def terminate_file_name = "${cluster_work_dir}/terminate-dask"
    """
    echo "\$(date): Terminate DASK Scheduler: ${cluster_work_dir}"
    cat > ${terminate_file_name} <<EOF
    DONE
    EOF
    cat ${terminate_file_name}
    """
}
