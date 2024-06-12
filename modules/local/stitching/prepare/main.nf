process STITCHING_PREPARE {
    tag "${meta.id}"
    label 'process_single'

    input:
     // files input here is only used for
     // properly mounting the dir when running in a container
    tuple val(meta), path(files, stageAs: 'data/?/*')

    output:
    tuple val(meta), path(files)

    script:
    """
    file_list=("${files}.join(' ')")
    for f in \${file_list}; do
        echo \$(readlink -m \$f)
    done
    umask 0002
    echo "Create session working directory: ${meta.session_work_dir}"
    mkdir -p ${meta.session_work_dir}
    echo "Create stitching working directory: ${meta.stitching_dir}"
    mkdir -p ${meta.stitching_dir}
    echo "Create stitching final results directory: ${meta.stitching_result_dir}"
    mkdir -p ${meta.stitching_result_dir}
    """
}
