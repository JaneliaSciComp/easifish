process DASK_PREPARE {
    label 'process_low'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    // The parent dask dir and the dir name are passed separately so that parent
    // gets mounted and work dir can be created within it
    tuple path(dask_work_dir), val(dask_work_dirname)

    output:
    val(cluster_work_fullpath)

    when:
    task.ext.when == null || task.ext.when

    script:
    cluster_work_fullpath = dask_work_dir.resolveSymLink().resolve(dask_work_dirname).toString()
    """
    /opt/scripts/prepare.sh "${dask_work_dir}/${dask_work_dirname}"
    """
}
