process OMEZARRTOOLS_MULTISCALE {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/zarr-tools:0.3.1-dask2025.11.0-py12-ol9'
    cpus { cpus }
    memory "${mem_gb} GB"

    input:
    tuple val(meta), path(data_container), val(dataset_subpath)
    tuple val(dask_scheduler), path(dask_config) // this is optional - if undefined pass in as empty list ([])
    val(cpus)
    val(mem_gb)

    output:
    tuple val(meta), env('full_data_container_path'), val(dataset_subpath), emit: data
    path "versions.yml"                                                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def dataset_subpath_arg = dataset_subpath ? "--input-subpath ${dataset_subpath}" : ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_config ? "--dask-config ${dask_config}" : ''
    """
    # Create command line parameters
    full_data_container_path=\$(readlink -e ${data_container})
    echo "Generate pyramid for \${full_data_container_path}:${dataset_subpath}"
    CMD=(
        python -m zarr_tools.cli.main_multiscale
        -i \${full_data_container_path}
        ${dataset_subpath_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
        ${args}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        zarr-multiscale: 0.1.0
    END_VERSIONS
    """
}
