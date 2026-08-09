process BIGSTREAM_DEFORM {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/bigstream:5.1.2-omezarr-dask2025.11.0-py12-ol9'
    cpus { cpus }
    memory "${mem_gb} GB"
    conda "${moduleDir}/conda-env.yml"

    input:
    tuple val(meta),
          path(fix_image, stageAs: 'fix/*'),val(fix_image_subpath),
          val(fix_timeindex), val(fix_channel), val(fix_spacing),
          path(mov_image, stageAs: 'mov/*'),val(mov_image_subpath),
          val(mov_timeindex), val(mov_channel), val(mov_spacing),
          path(global_transform, stageAs: 'global-transform/*'), // one or more global transformations
          val(global_transform_subpath),
          path(local_transform, stageAs: 'local-transform/*'), // location of the displacement vector
          val(local_transform_subpath), // local transform subpath
          path(output_dir, stageAs: 'warped/*'),
          val(output_subpath), val(output_timeindex), val(output_channel)
    tuple val(dask_scheduler),
          path(dask_config) // this is optional - if undefined pass in as empty list ([])
    val(cpus)
    val(mem_gb)

    output:
    tuple val(meta),
          env('fix_fullpath'), val(fix_image_subpath),
          env('mov_fullpath'), val(mov_image_subpath),
          env('output_fullpath'), val(output_subpath)  , emit: results

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
    def transforms_paths = []
    def transforms_subpaths = []
    if (global_transform) {
        transforms_paths << global_transform
        if (global_transform_subpath) {
            transforms_subpaths << global_transform_subpath
        } else {
            transforms_subpaths << ''
        }
    }
    if (local_transform) {
        transforms_paths << local_transform
        if (local_transform_subpath) {
            transforms_subpaths << local_transform_subpath
        } else {
            transforms_subpaths << ''
        }
    }
    def transforms_arg
    if (transforms_paths) {
        def transforms_arg_value = [transforms_paths, transforms_subpaths].transpose().collect { p, sp -> "$p~$sp" }.join(',')
        transforms_arg = "--transforms ${transforms_arg_value}"
    } else {
        transforms_arg = ''
    }
    def output_subpath_arg = output_subpath ? "--output-subpath ${output_subpath}" : ''
    def output_timeindex_arg = output_timeindex ? "--output-timeindex ${output_timeindex}" : ''
    def output_channel_arg = output_channel ? "--output-channel ${output_channel}" : ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_scheduler && dask_config ? "--dask-config ${dask_config}" : ''

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
    fix_fullpath=\$(\${READLINK_TOOL} ${fix_image})
    mov_fullpath=\$(\${READLINK_TOOL} ${mov_image})
    output_fullpath=\$(\${READLINK_TOOL} ${output_dir})
    mkdir -p \${output_fullpath}

    CMD=(
        python -m bigstream.tools.main_apply_local_transform
        --fix \${fix_fullpath} ${fix_image_subpath_arg}
        ${fix_timeindex_arg} ${fix_channel_arg} ${fix_spacing_arg}
        --moving \${mov_fullpath} ${mov_image_subpath_arg}
        ${mov_timeindex_arg} ${mov_channel_arg} ${mov_spacing_arg}
        ${transforms_arg}
        --output \${output_fullpath} ${output_subpath_arg}
        ${output_timeindex_arg} ${output_channel_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}
