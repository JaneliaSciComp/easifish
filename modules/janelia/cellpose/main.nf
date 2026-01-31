process CELLPOSE {
    container { task && task.ext.container ? task.ext.container : 'ghcr.io/janeliascicomp/cellpose:4.0.8-dask2025.11.0-py12' }
    cpus { cellpose_cpus }
    memory "${cellpose_mem_in_gb} GB"
    conda 'modules/janelia/cellpose/conda-env.yml'

    input:
    tuple val(meta),
          path(image, stageAs: 'cellpose-input/*'),
          val(image_subpath),
          path(models_path, stageAs: 'cellpose-models/*'), // this is optional - if undefined pass in as empty list ([])
          val(model_name), // model name
          path(output_dir),
          val(labels),
          val(labels_subpath),
          path(working_dir, stageAs: 'cellpose-work/*') // this is optional
    tuple val(dask_scheduler),
          path(dask_config) // this is optional - if undefined pass in as empty list ([])
    path(preprocessing_config) // preprocessing config file
    path(logging_config) // this is optional - if undefined pass in as empty list ([])
    val(cellpose_cpus)
    val(cellpose_mem_in_gb)

    output:
    tuple val(meta),
          env('input_image_fullpath'),
          val(image_subpath),
          eval('printf "%s\n" "\${output_label_images[@]}"'),
          val(output_labels_subpath)                        , emit: results
    path('versions.yml')                                    , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def image_name = file(image).name.replace('.', '_')
    def input_image_subpath_arg = image_subpath
                                    ? "--input-subpath ${image_subpath}"
                                    : ''
    output_labels_subpath = labels_subpath ?: image_subpath
    def output_labels_subpath_arg  = labels_subpath
                                    ? "--output-subpath ${labels_subpath}"
                                    : ''
    def set_models_path = models_path
        ? "models_fullpath=\$(readlink ${models_path}) && \
           echo \"Set models path: \${models_fullpath}\" && \
           mkdir -p \${models_fullpath} && \
           export CELLPOSE_LOCAL_MODELS_PATH=\${models_fullpath}"
        : ''
    def preprocessing_config_arg = preprocessing_config ? "--preprocessing-config ${preprocessing_config}" : ''
    def logging_config_arg = logging_config ? "--logging-config ${logging_config}" : ''
    def models_path_arg = models_path ? "--models-dir \${models_fullpath}" : ''
    def model_name_arg = model_name ? "--model ${model_name}": ''
    def subpath_name = image_subpath ? "/${image_subpath.split('/')[-1]}" : ''
    def working_dirname = working_dir ? working_dir : output_dir
    def labels_image = labels ?: ''
    def dask_scheduler_arg = dask_scheduler ? "--dask-scheduler ${dask_scheduler}" : ''
    def dask_config_arg = dask_config ? "--dask-config ${dask_config}" : ''
    def (labels_noext, labels_ext) = labels_image.lastIndexOf('.').with { pos ->
        pos == -1
            ? [labels_image, '']
            : [labels_image[0..<pos], labels_image[(pos+1)..-1]]
    }
    log.debug "Labels output name:ext => ${labels_noext}:${labels_ext}"

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
    input_image_fullpath=\$(readlink ${image})
    echo "Input image: \${input_image_fullpath}"
    # create the output directory using the canonical name
    output_fullpath=\$(\${READLINK_MISSING_OPT} ${output_dir})
    echo "Output dir: \${output_fullpath}"
    mkdir -p \${output_fullpath}
    echo "Created output dir: \${output_fullpath}"
    # create working directory
    working_fullpath=\$(\${READLINK_MISSING_OPT} ${working_dirname})
    echo "Working dir: \${working_fullpath}"
    full_workingname="\${working_fullpath}/${image_name}${subpath_name}"
    mkdir -p "\${full_workingname}"
    if [[ "${labels_image}" == "" ]]; then
        full_outputname=\${output_fullpath}
    else
        full_outputname="\${output_fullpath}/${labels_image}"
    fi
    ${set_models_path}

    CMD=(
        python -m tools.main_distributed_cellpose
        -i \${input_image_fullpath} ${input_image_subpath_arg}
        -o \${full_outputname} ${output_labels_subpath_arg}
        --working-dir \${full_workingname}
        ${models_path_arg}
        ${model_name_arg}
        ${dask_scheduler_arg}
        ${dask_config_arg}
        ${preprocessing_config_arg}
        ${logging_config_arg}
        ${args}
    )

    echo "CMD: \${CMD[@]}"
    (exec "\${CMD[@]}")

    output_label_images=()
    for sr in \$(ls \${output_fullpath} | grep -e "${labels_noext}.*${labels_ext}") ; do
        output_label_images+=("\${output_fullpath}/\${sr}")
    done

    echo "Output label images: \${output_label_images[@]}"

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
