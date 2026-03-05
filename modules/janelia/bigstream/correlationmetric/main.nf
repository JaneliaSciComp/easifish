process BIGSTREAM_CORRELATIONMETRIC {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/bigstream:5.1.2-omezarr-dask2025.11.0-py12-ol9'
    conda "${moduleDir}/conda-env.yml"

    input:
    tuple val(meta),
          path(fix_image, stageAs: 'fix/*'),val(fix_image_subpath),
          val(fix_timeindex), val(fix_channel), val(fix_spacing),
          path(mov_image, stageAs: 'mov/*'),val(mov_image_subpath),
          val(mov_timeindex), val(mov_channel), val(mov_spacing),
          path(output_dir, stageAs: 'metric/*'), val(output_subpath)

    output:
    tuple val(meta),
          env('fix_fullpath'), val(fix_image_subpath),
          env('mov_fullpath'), val(mov_image_subpath),
          env('output_fullpath'), val(output_subpath), emit: results

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def fix_image_subpath_arg = fix_image_subpath ? "--fix-subpath ${fix_image_subpath}" : ''
    def fix_timeindex_arg = fix_timeindex ? "--fix-timeindex ${fix_timeindex}" : ''
    def fix_channel_arg = fix_channel ? "--fix-channel ${fix_channel}" : ''
    def fix_spacing_arg = fix_spacing ? "--fix-spacing ${fix_spacing}" : ''
    def mov_image_subpath_arg = mov_image_subpath ? "--mov-subpath ${mov_image_subpath}" : ''
    def mov_timeindex_arg = mov_timeindex ? "--mov-timeindex ${mov_timeindex}" : ''
    def mov_channel_arg = mov_channel ? "--mov-channel ${mov_channel}" : ''
    def mov_spacing_arg = mov_spacing ? "--mov-spacing ${mov_spacing}" : ''
    def output_subpath_arg = output_subpath ? "--output-subpath ${output_subpath}" : ''

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
    echo "Detected OS: \${detected_os}"
    fix_fullpath=\$(\${READLINK_TOOL} ${fix_image})
    mov_fullpath=\$(\${READLINK_TOOL} ${mov_image})
    output_fullpath=\$(\${READLINK_TOOL} -m ${output_dir})
    mkdir -p \${output_fullpath}

    CMD=(
        python -m bigstream.tools.main_registration_metric
        --fix \${fix_fullpath} ${fix_image_subpath_arg}
        ${fix_timeindex_arg} ${fix_channel_arg} ${fix_spacing_arg}
        --mov \${mov_fullpath} ${mov_image_subpath_arg}
        ${mov_timeindex_arg} ${mov_channel_arg} ${mov_spacing_arg}
        --output \${output_fullpath} ${output_subpath_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}
