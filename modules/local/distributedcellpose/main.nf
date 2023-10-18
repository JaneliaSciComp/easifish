process DISTRIBUTEDCELLPOSE {
    container 'docker.io/multifish/distributed-cellpose:2.2.3'
    cpus { cellpose_cpus }
    memory "${cellpose_mem_in_gb} GB"
    clusterOptions { task.ext.cluster_opts }

    input:
    tuple val(meta), path(image), path(outputdir)
    val(dask_scheduler)
    val(cellpose_cpus)
    val(cellpose_mem_in_gb)

    output:
    tuple val(meta), path(image)

    script:
    def args = task.ext.args ?: ''
    def image_dataset_arg = meta.image_dataset ? "--input-subpath ${meta.image_dataset}": ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''

    """
    python /opt/scripts/cellpose/distributed_cellpose.py \
        -i ${image} ${image_dataset_arg} \
        -o ${outputdir} \
        ${dask_scheduler_arg} \
        ${args}
    """

}