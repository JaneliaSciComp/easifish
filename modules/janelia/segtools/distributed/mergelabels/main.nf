process SEGTOOLS_DISTRIBUTED_MERGELABELS {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/cellpose:4.0.8-dask2025.11.0-py12'
    cpus { cpus }
    memory "${mem_in_gb} GB"
    conda "${moduleDir}/conda-env.yml"

    input:
    tuple val(meta),
          path(input_labels, stageAs: 'input-labels/*'),
          val(input_subpath),
          path(output_labels, stageAs: 'output-labels/*'),
          val(output_subpath),
          path(working_dir, stageAs: 'cellpose-work/*') // this is optional
    tuple val(dask_scheduler),
          path(dask_config) // this is optional - if undefined pass in as empty list ([])
    path(logging_config) // this is optional - if undefined pass in as empty list ([])
    val(cpus)
    val(mem_in_gb)

    output:
    tuple val(meta),
          env('input_labels_fullpath'),
          val(input_subpath),
          env('output_labels_fullpath'),
          val("${output_subpath ?: input_subpath}"), emit: results
    path('versions.yml')                           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def input_subpath_arg = input_subpath
                               ? "--input-subpath ${input_subpath}"
                               : ''
    def set_output_labels = output_labels ? "output_labels_fullpath=\$(\${READLINK_TOOL} -m ${output_labels})" : 'output_labels_fullpath='
    def output_labels_arg = output_labels ? "-o \${output_labels_fullpath}" : ''
    def output_subpath_arg = output_subpath
                               ? "--output-subpath ${output_subpath}"
                               : ''
    def logging_config_arg = logging_config ? "--logging-config ${logging_config}" : ''
    def working_dir_arg = working_dir ? "--working-dir \$(\${READLINK_TOOL} -m ${working_dir})" : ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_config ? "--dask-config ${dask_config}" : ''
    """
    case \$(uname) in
        Darwin)
            detected_os=OSX
            READLINK_TOOL="greadlink"
            ;;
        *)
            detected_os=Linux
            READLINK_TOOL="readlink"
            ;;
    esac
    ${set_output_labels}
    input_labels_fullpath=\$(\${READLINK_TOOL} -m ${input_labels})
    echo "Input labels: \${input_labels_fullpath}"

    if [[ -z "\${output_labels_fullpath}" ]]; then
        echo "set output_labels_fullpath=\${input_labels_fullpath}"
        output_labels_fullpath="\${input_labels_fullpath}"
    else
        mkdir -p \$(dirname \${output_labels_fullpath})
    fi

    echo "Output labels: \${output_labels_fullpath}"

    CMD=(
        python -m tools.main_merge_labels
        -i \${input_labels_fullpath} ${input_subpath_arg}
        ${output_labels_arg} ${output_subpath_arg}
        ${working_dir_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
        ${logging_config_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    cellpose_version=\$(python -m tools.main_distributed_cellpose \
                        --version | \
                        grep "cellpose version" | \
                        sed "s/cellpose version:\\s*//")
    echo "Cellpose version: \${cellpose_version}"
    cat <<-END_VERSIONS > versions.yml
    cellpose: \${cellpose_version}
    END_VERSIONS
    """
}
