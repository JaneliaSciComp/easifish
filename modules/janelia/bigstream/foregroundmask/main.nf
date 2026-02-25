process BIGSTREAM_FOREGROUNDMASK {
    tag "${meta.id}"
    label 'process_single'
    container 'ghcr.io/janeliascicomp/bigstream:5.1.2-omezarr-dask2025.11.0-py12-ol9'
    conda 'modules/janelia/bigstream/conda-env.yml'

    input:
    tuple val(meta),
          path(image, stageAs: 'image-input/*'), val(image_subpath),
          val(timeindex), val(ch),
          path(mask, stageAs: 'mask-output/*'), val(mask_subpath)

    output:
    tuple val(meta),
          env('full_image'), val(image_subpath),
          env('full_mask'), val(mask_subpath)

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def image_arg = image ? "--image \${full_image}" : ''
    def image_subpath_arg = image_subpath ? "--image-subpath ${image_subpath}" : ''
    def timeindex_arg = timeindex ? "--timeindex ${timeindex}" : ''
    def channel_arg = ch ? "--channel ${ch}" : ''
    def mask_arg = mask ? "--output \${full_mask}" : ''
    def mask_subpath_arg = mask_subpath ? "--output-subpath ${mask_subpath}" : ''
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

    full_image=\$(\${READLINK_TOOL} -m ${image})
    echo "Input image full path: \${full_image}"

    full_mask=\$(\${READLINK_TOOL} -m ${mask})
    full_mask_outputdir=\$(dirname \${full_mask})
    if [[ ! -e \${full_mask_outputdir} ]] ; then
        echo "Create output mask directory: \${full_mask_outputdir}"
        mkdir -p \${full_mask_outputdir}
    else
        echo "Mask output directory: \${full_mask_outputdir} - already exists"
    fi

    CMD=(
        python -m bigstream.tools.main_prepare_foreground_mask
        ${image_arg} ${image_subpath_arg}
        ${timeindex_arg} ${channel_arg}
        ${mask_arg} ${mask_subpath_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")
    """
}
