process BIGSTREAM_COMPUTEINVERSE {
    container { task.ext.container ?: 'ghcr.io/janeliascicomp/bigstream:1.3.2-dask2024.4.1-py11' }
    cpus { bigstream_cpus }
    memory "${bigstream_mem_in_gb} GB"

    input:
    tuple val(meta),
          path(deform_dir, stageAs: 'deform/*'), // location of the displacement vector
          val(deform_name),
          val(deform_subpath), // displacement vector subpath
          path(inv_deform_dir, stageAs: 'inv_deform/*'),
          val(inv_deform_name),
          val(inv_deform_subpath) // inverse displacement vector subpath
    tuple val(dask_scheduler),
          path(dask_config) // this is optional - if undefined pass in as empty list ([])
    val(bigstream_cpus)
    val(bigstream_mem_in_gb)

    output:
    tuple val(meta),
          env(full_deform_dir), val(deform_name), val(deform_subpath),
          env(full_inv_deform_dir), val(inv_deform_name), val(inv_deform_subpath), emit: results

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def deform_dir_arg = "--transform-dir ${deform_dir}"
    def deform_name_arg = deform_name ? "--transform-name ${deform_name}" : ''
    def deform_subpath_arg = deform_subpath ? "--transform-subpath ${deform_subpath}" : ''

    def inv_deform_dir_arg = inv_deform_dir ? "--inv-transform-dir ${inv_deform_dir}" : ''
    def inv_deform_name_arg = inv_deform_name ? "--inv-transform-name ${inv_deform_name}" : ''
    def inv_deform_subpath_arg = inv_deform_subpath ? "--inv-transform-subpath ${inv_deform_subpath}" : ''

    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_scheduler && dask_config ? "--dask-config ${dask_config}" : ''

    """
    full_deform_dir=\$(readlink ${deform_dir})
    if [[ "${inv_deform_dir_arg}" == "" ]]; then
        full_inv_deform_dir=\${full_deform_dir}
    else
        full_inv_deform_dir=\$(readlink -m ${inv_deform_dir})
        mkdir -p \${full_inv_deform_dir}
    fi

    python /app/bigstream/scripts/main_compute_local_inverse.py \
        ${deform_dir_arg} \
        ${deform_name_arg} \
        ${deform_subpath_arg} \
        ${inv_deform_dir_arg} \
        ${inv_deform_name_arg} \
        ${inv_deform_subpath_arg} \
        ${dask_scheduler_arg} \
        ${dask_config_arg} \
        ${args}
    """
}
