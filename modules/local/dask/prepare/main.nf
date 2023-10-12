process DASK_PREPARE {
    label 'process_low'
    container 'docker.io/multifish/biocontainers-dask:2023.8.1'

    input:
    // The parent dask dir and the dir name are passed separately so that parent
    // gets mounted and work dir can be created within it
    tuple val(meta), path(dask_work_parent), val(dask_work_fullpath)

    output:
    tuple val(meta), val(cluster_work_fullpath)

    when:
    task.ext.when == null || task.ext.when

    script:
    cluster_work_fullpath = file(dask_work_fullpath).resolveSymLink().toString()
    """
    /opt/scripts/daskscripts/prepare.sh "${dask_work_fullpath}"
    """
}
