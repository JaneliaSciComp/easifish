process SPOTS_FISHSPOTS {
    tag "${meta.id}"
    container 'ghcr.io/janeliascicomp/easifish-spots-utils:v1.2-ome-dask2025.11.0'
    cpus { cpus }
    memory { "${mem_gb} GB" }

    input:
    tuple val(meta),
          path(input_path),
          val(input_dataset),
          path(mask_path),
          val(mask_subpath),
          path(spots_output_dir, stageAs: 'spots/*'),
          val(spots_result_name),
          val(spots_image_subpath_ref),
          val(spots_channels)
    tuple val(dask_scheduler), path(dask_config) // this is optional - if undefined pass in as empty list ([])
    path(fishspots_config)
    val(cpus)
    val(mem_gb)

    output:
    tuple val(meta),
          env('INPUT_IMG'),
          val(input_dataset),
          path(spots_output_dir),
          val(spots_result_name),
          val(dask_scheduler),                    emit: params
    tuple val(meta), env('full_output_filename'), emit: csv
    path "versions.yml",                          emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def extra_args = task.ext.args ?: ''
    def mask_arg = mask_path ? "--mask \$(\${READLINK_TOOL} ${mask_path})" : ''
    def mask_subpath_arg = mask_path && mask_subpath ? "--mask-subpath ${mask_subpath}" : ''
    def output_filename = spots_result_name ?: "${meta.id}-points.csv"
    def fishspots_config_arg = fishspots_config ? "--fishspots-config ${fishspots_config}" : ''
    def spots_image_subpath_ref_arg = spots_image_subpath_ref ? "--spots-image-subpath-reference ${spots_image_subpath_ref}" : ''
    def spots_channels_arg = spots_channels ? "--included-channels ${spots_channels}" : ''
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
    INPUT_IMG=\$(\${READLINK_TOOL} ${input_path})
    full_spots_dir=\$(\${READLINK_TOOL} -m ${spots_output_dir})
    mkdir -p \${full_spots_dir}
    full_output_filename=\${full_spots_dir}/${output_filename}

    CMD=(
        python -m easifish_spots_tools.main_spot_extraction
        --input \${INPUT_IMG}
        --input_subpath ${input_dataset}
        ${mask_arg}
        ${mask_subpath_arg}
        --output \${full_output_filename}
        ${fishspots_config_arg}
        ${spots_channels_arg}
        ${spots_image_subpath_ref_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
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
