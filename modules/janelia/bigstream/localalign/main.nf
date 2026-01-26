process BIGSTREAM_LOCALALIGN {
    tag "${meta.id}"
    container { task && task.ext.container ? task.ext.container : 'ghcr.io/janeliascicomp/bigstream:5.0.2-omezarr-dask2025.11.0-py12-ol9' }
    cpus { bigstream_cpus }
    memory "${bigstream_mem_in_gb} GB"
    conda 'modules/janelia/bigstream/conda-env.yml'

    input:
    tuple val(meta),
          path(fix_image, stageAs: 'fix/*'), val(fix_image_subpath),
          val(fix_timeindex), val(fix_channel),
          path(mov_image, stageAs: 'mov/*'), val(mov_image_subpath),
          val(mov_timeindex), val(mov_channel),
          path(fix_mask, stageAs: 'fixmask/*'), val(fix_mask_subpath),
          path(mov_mask, stageAs: 'movmask/*'), val(mov_mask_subpath),
          path(affine_transform), // global affine file name
          val(steps),
          path(transform_dir, stageAs: 'transform/*'),
          val(transform_name), val(transform_subpath),
          val(inv_transform_name), val(inv_transform_subpath),
          path(align_dir, stageAs: 'align/*'), val(align_name), val(align_subpath),
          val(align_timeindex), val(align_channel)


    path(bigstream_config)

    tuple val(dask_scheduler),
          path(dask_config) // this is optional - if undefined pass in as empty list ([])

    val(bigstream_cpus)

    val(bigstream_mem_in_gb)

    output:
    tuple val(meta),
          env(full_fix_image), val(fix_image_subpath),
          env(full_mov_image), val(mov_image_subpath),
          env(full_affine_transform),
          env(full_transform_dir),
          val(transform_name), val(transform_subpath_output),
          val(inv_transform_name), val(inv_transform_subpath_output),
          env(full_align_dir), val(align_name), val(align_subpath)  , emit: results

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def fix_image_arg = fix_image ? "--local-fix \${full_fix_image}" : ''
    def fix_image_subpath_arg = fix_image_subpath ? "--local-fix-subpath ${fix_image_subpath}" : ''
    def fix_timeindex_arg = fix_timeindex ? "--local-fix-timeindex ${fix_timeindex}" : ''
    def fix_channel_arg = fix_channel ? "--local-fix-channel ${fix_channel}" : ''

    def mov_image_arg = mov_image ? "--local-mov \${full_mov_image}" : ''
    def mov_image_subpath_arg = mov_image_subpath ? "--local-mov-subpath ${mov_image_subpath}" : ''
    def mov_timeindex_arg = mov_timeindex ? "--local-mov-timeindex ${mov_timeindex}" : ''
    def mov_channel_arg = mov_channel ? "--local-mov-channel ${mov_channel}" : ''

    def fix_mask_arg = fix_mask ? "--local-fix-mask \$(readlink ${fix_mask})" : ''
    def fix_mask_subpath_arg = fix_mask && fix_mask_subpath ? "--local-fix-mask-subpath ${fix_mask_subpath}" : ''
    def mov_mask_arg = mov_mask ? "--local-mov-mask \$(readlink ${mov_mask})" : ''
    def mov_mask_subpath_arg = mov_mask && mov_mask_subpath ? "--local-mov-mask-subpath ${mov_mask_subpath}" : ''
    def affine_transform_arg = affine_transform ? "--global-affine-transform ${affine_transform}" : ''
    def steps_arg = steps ? "--local-registration-steps ${steps}" : ''
    def transform_dir_arg = transform_dir ? "--local-transform-dir ${transform_dir}" : ''
    def transform_name_arg = transform_name ? "--local-transform-name ${transform_name}" : ''
    def transform_subpath_arg = transform_subpath ? "--local-transform-subpath ${transform_subpath}" : ''
    def inv_transform_name_arg = inv_transform_name ? "--local-inv-transform-name ${inv_transform_name}" : ''
    def inv_transform_subpath_arg = inv_transform_subpath ? "--local-inv-transform-subpath ${inv_transform_subpath}" : ''

    def align_dir_arg = align_dir ? "--local-align-dir \${full_align_dir}" : ''
    def aligned_name_arg = align_name ? "--local-align-name ${align_name}" : ''
    def aligned_subpath_arg = align_subpath ? "--local-align-subpath ${align_subpath}" : ''
    def align_timeindex_arg = align_timeindex ? "--local-align-timeindex ${align_timeindex}" : ''
    def align_channel_arg = align_channel ? "--local-align-channel ${align_channel}" : ''

    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_scheduler && dask_config ? "--dask-config ${dask_config}" : ''
    def bigstream_config_arg = bigstream_config ? "--align-config ${bigstream_config}" : ''

    transform_subpath_output = transform_subpath ?: mov_image_subpath
    inv_transform_subpath_output = inv_transform_subpath ?: transform_subpath_output

    """
    case \$(uname) in
        Darwin)
            detected_os=OSX
            READLINK_MISSING_OPT="readlink"
            ;;
        *)
            detected_os=Linux
            READLINK_MISSING_OPT="readlink -m"
            ;;
    esac
    echo "Detected OS: \${detected_os}"

    if [[ "${fix_image}" != "" ]];  then
        full_fix_image=\$(readlink ${fix_image})
        echo "Fix volume full path: \${full_fix_image}"
    else
        full_fix_image=
        echo "No fix volume provided"
    fi
    if [[ "${mov_image}" != "" ]];  then
        full_mov_image=\$(readlink ${mov_image})
        echo "Moving volume full path: \${full_mov_image}"
    else
        full_mov_image=
        echo "No moving volume provided"
    fi

    if [[ "${affine_transform}" != "" ]] ; then
        full_affine_transform=\$(readlink ${affine_transform})
    else
        full_affine_transform=
    fi

    if [[ "${transform_dir}" != "" ]] ; then
        full_transform_dir=\$(\${READLINK_MISSING_OPT} ${transform_dir})
        if [[ ! -e \${full_transform_dir} ]] ; then
            echo "Create transform directory: \${full_transform_dir}"
            mkdir -p \${full_transform_dir}
        else
            echo "Transform directory: \${full_transform_dir} - already exists"
        fi
        if [[ "${transform_name}" != "" ]] ; then
            # this is to cover the case when the transform name contains a relative path
            # e.g. direct/deform.n5
            data_dir=\$(dirname "\${full_transform_dir}/${transform_name}")
            if [[ "\${data_dir}" !=  "\${full_transform_dir}" ]] ; then
                echo "Create directory for affine transformation: \${data_dir}"
                mkdir -p \${data_dir}
            fi
        fi
        if [[ "${inv_transform_name}" != "" ]] ; then
            # this is to cover the case when the inverse transform name contains a relative path
            # e.g. inverse/deform.n5
            data_dir=\$(dirname "\${full_transform_dir}/${inv_transform_name}")
            if [[ "\${data_dir}" !=  "\${full_transform_dir}" ]] ; then
                echo "Create directory for affine transformation: \${data_dir}"
                mkdir -p \${data_dir}
            fi
        fi
    else
        full_transform_dir=
    fi

    if [[ "${align_dir}" != "" ]] ; then
        full_align_dir=\$(\${READLINK_MISSING_OPT} ${align_dir})
        if [[ ! -e \${full_align_dir} ]] ; then
            echo "Create align directory: \${full_align_dir}"
            mkdir -p \${full_align_dir}
        else
            echo "Align directory: \${full_align_dir} - already exists"
        fi
        if [[ "${align_name}" != "" ]] ; then
            # this is to cover the case when the transform name contains a relative path
            # e.g. deform/warped.n5
            data_dir=\$(dirname "\${full_align_dir}/${align_name}")
            if [[ "\${data_dir}" !=  "\${full_align_dir}" ]] ; then
                echo "Create directory for deformed result: \${data_dir}"
                mkdir -p \${data_dir}
            fi
        fi
    else
        full_align_dir=
    fi

    CMD=(
        python -m launchers.main_local_align_pipeline
        ${fix_image_arg} ${fix_image_subpath_arg}
        ${fix_timeindex_arg} ${fix_channel_arg}
        ${mov_image_arg} ${mov_image_subpath_arg}
        ${mov_timeindex_arg} ${mov_channel_arg}
        ${fix_mask_arg} ${fix_mask_subpath_arg}
        ${mov_mask_arg} ${mov_mask_subpath_arg}
        ${affine_transform_arg}
        ${steps_arg}
        ${bigstream_config_arg}
        ${transform_dir_arg}
        ${transform_name_arg} ${transform_subpath_arg}
        ${inv_transform_name_arg} ${inv_transform_subpath_arg}
        ${align_dir_arg} ${aligned_name_arg} ${aligned_subpath_arg}
        ${align_timeindex_arg} ${align_channel_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    """

}
