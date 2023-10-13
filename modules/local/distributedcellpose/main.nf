process DISTRIBUTEDCELLPOSE {
    container 'docker.io/multifish/distributed-cellpose:2.1.1'
    cpus { cellpose_cpus }
    memory "${cellpose_mem_in_gb} GB"
    clusterOptions { task.ext.cluster_opts }

    input:
    tuple val(meta), path(image)
    val(dask_scheduler)
    cellpose_cpus
    cellpose_mem_in_gb

    output:
    val(meta)

    script:
    def args = task.ext.args ?: ''
    def image_dataset_arg = meta.image_dataset ? "--input-subpath ${meta.image_dataset}": ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''

    """
    python /opt/scripts/cellposescripts/distributed_cellpose.py \
        -i ${image} ${image_dataset_arg} \
        ${dask_scheduler_arg} \
        ${args}
    """

}