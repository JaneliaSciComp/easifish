process FISHSPOTS {
    tag "${meta.id}"
    container { task && task.ext.container ?: 'ghcr.io/janeliascicomp/fishspot' }
    cpus { fishspots_cpus }
    memory { fishspots_mem_gb }

    input:
    tuple val(meta),
          path(input_path),
          val(input_dataset),
          path(spots_output_dir, stageAs: 'spots/*'),
          val(spots_result_name)
    tuple val(dask_scheduler), path(dask_config) // this is optional - if undefined pass in as empty list ([])
    val(fishspots_cpus)
    val(fishspots_mem_gb)

    output:
    tuple val(meta),
          env(INPUT_IMG),
          val(input_dataset),
          path(spots_output_dir),
          val(spots_result_name),               emit: params
    tuple val(meta), env(full_output_filename), emit: csv
    path "versions.yml",                        emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def output_filename = spots_result_name ?: "${meta.id}-points.csv"

    """
    INPUT_IMG=\$(realpath ${input_path})
    full_spots_dir=\$(readlink -m ${spots_output_dir})
    mkdir -p \${full_spots_dir}
    full_output_filename=\${full_spots_dir}/${output_filename}

    CMD=(
        python -m distributed_tools.main_spot_extraction
        --input \${INPUT_IMG}
        --input_subpath ${input_dataset}
        ${extra_args}
    )
    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        fishspots: 0.5.0
    END_VERSIONS

    """
}
